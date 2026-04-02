require "async"
require "async/condition"
require "async/semaphore"
require "socket"
require "logger"
require_relative "errors"
require_relative "frame_io"
require_relative "channel"
require_relative "sasl"

module AsyncRabbitMQ
  # Represents one AMQP connection to a RabbitMQ broker.
  #
  # Recovery: exponential backoff, initial 1s, max 30s, ±25% jitter, unlimited retries.
  # Re-registers consumers and re-publishes pending confirms after reconnect.
  #
  # Pool interface: implements reusable?, viable?, concurrency, close for Async::Pool.
  class Session
    CONNECT_TIMEOUT     = 30     # seconds for AMQP handshake
    RECOVERY_INITIAL    = 1.0    # seconds
    RECOVERY_MAX        = 30.0   # seconds
    RECOVERY_JITTER     = 0.25   # ±25%
    PROTOCOL_HEADER     = "AMQP\x00\x00\x09\x01".b.freeze

    attr_reader :host, :port, :vhost, :username

    def initialize(
      host: "localhost",
      port: 5672,
      vhost: "/",
      username: "guest",
      password: "guest",
      tls: false,
      tls_context: nil,
      heartbeat: 60,
      frame_max: 131_072,
      auth_mechanism: nil,
      logger: Logger.new($stdout)
    )
      @host           = host
      @port           = port
      @vhost          = vhost
      @username       = username
      @password       = password
      @tls            = tls
      @tls_context    = tls_context
      @heartbeat      = heartbeat
      @frame_max      = frame_max
      @auth_mechanism = auth_mechanism
      @logger         = logger

      @state             = :closed
      @frame_io          = nil
      @channels          = {}       # channel_id => Channel
      @next_channel_id   = 1
      @channel_mutex     = Async::Semaphore.new(1)
      @negotiated_hb     = nil
      @negotiated_fm     = nil
      @negotiated_cmax   = nil
      @recovery_in_progress = false
      @closed_by_user    = false
      @connecting        = false
      @open_condition    = nil
      @last_frame_at     = nil
      @heartbeat_task    = nil
      @channel0_task     = nil    # drains channel-0 queue; handles connection.blocked/unblocked
      @recovery_task     = nil
      @recovery_wakeup   = nil    # Async::Condition to interrupt the retry sleep
      @session_root_task = nil    # top-level task; recovery is spawned here so it
                                  # survives reader-task Cancel propagation
    end

    # Connect and complete AMQP handshake. Raises ConnectionTimeoutError if
    # the handshake does not complete within CONNECT_TIMEOUT seconds.
    # Raises NotOpenError if already connected.
    def connect
      raise NotOpenError, "Session is already connected" if open?
      @connecting = true
      @session_root_task = Async::Task.current
      Async::Task.current.with_timeout(CONNECT_TIMEOUT) do
        raw_socket = open_socket
        @frame_io  = build_frame_io(raw_socket)
        @frame_io.start
        handshake
        start_heartbeat_task
        start_channel0_monitor_task
        @state = :open
      end
      @connecting = false
    rescue Async::TimeoutError
      cleanup_after_failed_connect
      raise ConnectionTimeoutError, "AMQP handshake did not complete within #{CONNECT_TIMEOUT}s"
    rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, Errno::EHOSTUNREACH, SocketError => e
      cleanup_after_failed_connect
      raise ConnectionTimeoutError, "Could not connect to #{@host}:#{@port} — #{e.message}"
    rescue OpenSSL::SSL::SSLError => e
      cleanup_after_failed_connect
      raise ConnectionTimeoutError, "TLS handshake failed connecting to #{@host}:#{@port} — #{e.message}"
    rescue AuthenticationError
      cleanup_after_failed_connect
      raise
    end

    def open?
      @state == :open
    end

    def closed?
      @state == :closed
    end

    # Close the session gracefully. Also works if recovery is in progress.
    def close
      return if closed?
      @closed_by_user = true
      @recovery_wakeup&.signal rescue nil
      # Unblock any fibers waiting on channel replies (e.g. wait_for inside
      # reopen_after_recovery) so that @recovery_task.cancel below can actually
      # terminate the task rather than leaving it stuck on an Async::Condition.
      conn_error = ConnectionError.new(code: 0, text: "Session closed by user")
      @channels.each_value { |ch| ch.interrupt_wait!(conn_error) rescue nil }
      if open?
        @state = :closing
        @channel0_task&.cancel rescue nil
        send_connection_close rescue nil
      end
      @heartbeat_task&.cancel rescue nil
      @channel0_task&.cancel rescue nil
      @recovery_task&.cancel rescue nil
      @frame_io&.stop rescue nil
      @state = :closed
    end

    # Open a new channel. Returns an AsyncRabbitMQ::Channel.
    def open_channel
      raise NotOpenError, "Session is not open" unless open?
      channel_id = next_channel_id
      channel    = Channel.new(channel_id, self, @frame_io, frame_max: @negotiated_fm || @frame_max, logger: @logger)
      @channel_mutex.acquire { @channels[channel_id] = channel }
      channel.open
      channel
    end

    # Called by Channel when it closes itself.
    def channel_closed(channel_id)
      @channel_mutex.acquire { @channels.delete(channel_id) }
      @frame_io&.unregister_channel(channel_id)
    end

    # --- Async::Pool resource interface ---

    # Can this session be returned to the pool?
    def reusable?
      open? && !@recovery_in_progress
    end

    # Is the underlying socket alive?
    def viable?
      open?
    end

    # Max concurrent channels (negotiated with broker, or AMQP max 2047).
    def concurrency
      @negotiated_cmax || 2047
    end

    # --- Internal ---

    # Called by FrameIO when a connection-level error triggers recovery.
    def trigger_recovery(error)
      return if @closed_by_user
      return if @connecting

      @logger.warn("Connection lost (#{error.class}: #{error.message}). Starting recovery...")

      if @recovery_in_progress
        # Connection died again while a recovery attempt is in progress.
        # Unblock any fiber stuck in wait_channel0_method or channel wait_for
        # so that the current recover_loop iteration fails fast and retries.
        recovery_error = ConnectionError.new(code: 0, text: "Connection lost during recovery")
        q0 = @frame_io&.channel_queue(0)
        q0&.push([:method, recovery_error]) rescue nil
        @channels.each_value { |ch| ch.interrupt_wait!(recovery_error) rescue nil }
        return
      end

      @recovery_in_progress = true
      @state = :recovering

      # Stop the channel-0 monitor so it doesn't race on the stale queue.
      # Also unblock any fibers stuck in write_frame waiting on connection.blocked.
      @channel0_task&.cancel rescue nil
      @channel0_task = nil
      @frame_io&.set_unblocked rescue nil

      # Interrupt all channel fibers so they raise ConnectionError instead of
      # hanging forever waiting for a reply that will never come.
      conn_error = ConnectionError.new(code: 0, text: "Connection lost: #{error.message}")
      @channels.each_value { |ch| ch.interrupt_wait!(conn_error) rescue nil }

      # Schedule recover_loop BEFORE stopping old frame_io. old_io.stop
      # cancels reader/writer tasks; if we ARE the reader task, cancel raises
      # Async::Cancel (< Exception) which bypasses `rescue nil` and propagates,
      # so anything after stop might not run.
      # Spawn recovery as a child of @session_root_task (the connect-call task),
      # NOT of Async::Task.current (the reader task). If we're in the reader task,
      # its Cancel propagation would also cancel a child recovery task.
      parent_task = @session_root_task || Async::Task.current
      @recovery_task = parent_task.async { recover_loop }

      # Stop the old frame_io: closes the dead socket, pushes nil to channel
      # queues (unblocking wait_channel0_method), and cancels writer task.
      # Reader task cancel may raise Async::Cancel — recovery is already scheduled.
      old_io    = @frame_io
      @frame_io = nil
      old_io&.stop rescue nil
    end

    def frame_max
      @negotiated_fm || @frame_max
    end

    private

    # Stop any in-flight frame_io / recovery tasks spawned during a failed connect.
    def cleanup_after_failed_connect
      @closed_by_user = true
      @recovery_task&.cancel rescue nil
      @recovery_task = nil
      @heartbeat_task&.cancel rescue nil
      @channel0_task&.cancel rescue nil
      @frame_io&.stop rescue nil
      @frame_io = nil
      @state = :closed
    end

    def open_socket
      if @tls
        require "openssl"
        ctx        = @tls_context || build_tls_context
        raw        = TCPSocket.new(@host, @port)
        ssl        = OpenSSL::SSL::SSLSocket.new(raw, ctx)
        ssl.hostname = @host
        ssl.connect
        ssl
      else
        TCPSocket.new(@host, @port)
      end
    end

    def build_tls_context
      require "openssl"
      ctx                     = OpenSSL::SSL::SSLContext.new
      ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
      ctx.min_version         = OpenSSL::SSL::TLS1_2_VERSION
      ctx
    end

    def build_frame_io(raw_socket)
      io = FrameIO.new(raw_socket, logger: @logger)
      # Register channel 0 for connection-level frames
      io.register_channel(0)

      # Override trigger_recovery to delegate to Session
      session = self
      io.define_singleton_method(:trigger_recovery) { |error| session.trigger_recovery(error) }

      # Update heartbeat timestamp on every received frame
      io.on_frame = -> { @last_frame_at = Process.clock_gettime(Process::CLOCK_MONOTONIC) }
      io
    end

    def handshake
      # Send AMQP protocol header directly on the socket
      @frame_io.instance_variable_get(:@socket).write(PROTOCOL_HEADER)

      # connection.start — negotiate SASL mechanism
      start = wait_channel0_method(AMQ::Protocol::Connection::Start)
      sasl = SASL.negotiate(
        start.mechanisms,
        preferred: @auth_mechanism,
        username: @username,
        password: @password
      )
      send_connection_start_ok(sasl)

      # connection.tune (broker may send connection.secure challenges first)
      msg = wait_channel0_method(AMQ::Protocol::Connection::Tune, AMQ::Protocol::Connection::Secure)
      while msg.is_a?(AMQ::Protocol::Connection::Secure)
        @logger.debug("Received connection.secure SASL challenge for #{sasl.mechanism_name}")
        @frame_io.write_frame(AMQ::Protocol::Connection::SecureOk.encode(sasl.challenge_response(msg.challenge)).encode)
        msg = wait_channel0_method(AMQ::Protocol::Connection::Tune, AMQ::Protocol::Connection::Secure)
      end
      @negotiated_hb   = negotiate_heartbeat(msg.heartbeat)
      @negotiated_fm   = negotiate_frame_max(msg.frame_max)
      @negotiated_cmax = msg.channel_max == 0 ? 2047 : msg.channel_max
      send_connection_tune_ok

      # connection.open
      send_connection_open
      wait_channel0_method(AMQ::Protocol::Connection::OpenOk)
    end

    def wait_channel0_method(*expected_classes)
      queue = @frame_io.channel_queue(0)
      loop do
        msg = queue.pop
        # nil sentinel — pushed by frame_io.stop or interrupt during recovery
        raise ConnectionError, "Connection closed while waiting for #{expected_classes.join(', ')}" if msg.nil?
        next unless msg[0] == :method
        method = msg[1]
        # ConnectionError pushed directly by trigger_recovery to unblock this wait
        raise method if method.is_a?(ConnectionError) || method.is_a?(ChannelError)
        if expected_classes.any? { |c| method.is_a?(c) }
          return method
        elsif method.is_a?(AMQ::Protocol::Connection::Close)
          code = method.reply_code
          text = method.reply_text
          if FrameIO::SOFT_ERROR_CODES.include?(code)
            raise ChannelError.new(code: code, text: text)
          else
            raise ConnectionError.new(code: code, text: text)
          end
        end
      end
    end

    def send_connection_start_ok(sasl)
      @frame_io.write_frame(
        AMQ::Protocol::Connection::StartOk.encode(
          {},
          sasl.mechanism_name,
          sasl.initial_response,
          "en_US"
        ).encode
      )
    end

    def send_connection_tune_ok
      @frame_io.write_frame(
        AMQ::Protocol::Connection::TuneOk.encode(
          @negotiated_cmax,
          @negotiated_fm,
          @negotiated_hb
        ).encode
      )
    end

    def send_connection_open
      @frame_io.write_frame(
        AMQ::Protocol::Connection::Open.encode(@vhost).encode
      )
    end

    def send_connection_close
      @frame_io.write_frame(
        AMQ::Protocol::Connection::Close.encode(200, "Goodbye", 0, 0).encode
      )
      # Wait at most 5s for CloseOk — skip if socket already dead.
      Async::Task.current.with_timeout(5) do
        wait_channel0_method(AMQ::Protocol::Connection::CloseOk)
      end
    rescue Async::TimeoutError, ConnectionError, ChannelError, IOError
      # Broker didn't respond — proceed with forced close.
    end

    def negotiate_heartbeat(broker_hb)
      return @heartbeat if broker_hb == 0
      return broker_hb  if @heartbeat == 0
      [@heartbeat, broker_hb].min
    end

    def negotiate_frame_max(broker_fm)
      return @frame_max if broker_fm == 0
      [@frame_max, broker_fm].min
    end

    def start_heartbeat_task
      interval = @negotiated_hb || 60
      return if interval == 0
      timeout  = interval * 2
      # Seed the timestamp now; the on_frame callback will keep it fresh.
      @last_frame_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      @heartbeat_task = Async::Task.current.async do
        loop do
          sleep interval
          break unless open?
          @frame_io.write_heartbeat
          last = @last_frame_at
          next unless last   # not yet seeded — skip this tick
          elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - last
          if elapsed > timeout
            trigger_recovery(HeartbeatTimeoutError.new("No frame received in #{elapsed.round(1)}s (timeout #{timeout}s)"))
            break
          end
        end
      end
    end

    def next_channel_id
      @channel_mutex.acquire do
        id = @next_channel_id
        @next_channel_id += 1
        id
      end
    end

    def recover_loop
      delay = RECOVERY_INITIAL
      attempts = 0

      loop do
        break if @closed_by_user

        attempts += 1
        jitter    = delay * RECOVERY_JITTER * (rand * 2 - 1)
        wait_secs = delay + jitter

        # Sleep until the delay expires OR session.close signals @recovery_wakeup.
        @recovery_wakeup = Async::Condition.new
        Async::Task.current.with_timeout(wait_secs) do
          @recovery_wakeup.wait
        end rescue nil   # TimeoutError is normal; Condition#signal raises nothing
        @recovery_wakeup = nil

        break if @closed_by_user

        @logger.info("Recovery attempt #{attempts} (delay was #{delay.round(1)}s)...")
        begin
          # Wrap the entire reconnect in a timeout so a dead socket doesn't hang forever.
          Async::Task.current.with_timeout(CONNECT_TIMEOUT) do
            raw_socket = open_socket
            @frame_io  = build_frame_io(raw_socket)
            @frame_io.start
            handshake
          end
          @heartbeat_task&.cancel rescue nil
          start_heartbeat_task
          start_channel0_monitor_task
          @state = :open
          @recovery_in_progress = false
          # NOTE: keep @recovery_task non-nil until reopen_channels completes so
          # that session.close can still cancel this task (and therefore interrupt
          # any wait_for calls inside reopen_after_recovery) if the user closes
          # the session while channels are being reopened.
          @logger.info("Recovery successful after #{attempts} attempt(s)")

          # Re-open channels and re-register consumers
          reopen_channels
          @recovery_task = nil
          return
        rescue => e
          @frame_io&.stop rescue nil
          @logger.warn("Recovery attempt #{attempts} failed: #{e.class}: #{e.message}")
          delay = [delay * 2, RECOVERY_MAX].min
          break if @closed_by_user
        end
      end
    ensure
      # Make sure state is consistent if we exit for any reason
      @recovery_in_progress = false if @closed_by_user
    end

    def reopen_channels
      @channels.each_value do |channel|
        channel.reopen_after_recovery(@frame_io) rescue nil
      end
    end

    # Dedicated long-lived task that drains the channel-0 queue after the
    # AMQP handshake completes.  Handles connection.blocked / connection.unblocked
    # by delegating to FrameIO's blocked-state gate so write_frame yields
    # automatically when the broker is resource-constrained.
    def start_channel0_monitor_task
      @channel0_task = Async::Task.current.async { channel0_monitor_loop }
    end

    def channel0_monitor_loop
      queue = @frame_io.channel_queue(0)
      loop do
        msg = queue.pop
        break if msg.nil?
        next unless msg[0] == :method
        method = msg[1]
        case method
        when AMQ::Protocol::Connection::Blocked
          @frame_io&.set_blocked(method.reason)
        when AMQ::Protocol::Connection::Unblocked
          @frame_io&.set_unblocked
        end
        # All other channel-0 methods during normal operation (e.g. stray HeartbeatFrames
        # routed here) are intentionally ignored; the handshake path uses wait_channel0_method
        # directly and does not go through this loop.
      end
    rescue => e
      @logger.debug("Channel-0 monitor exited: #{e.class}: #{e.message}")
    end
  end
end
