require "async"
require "async/condition"
require "async/semaphore"
require "socket"
require "openssl"
require "uri"
require "logger"
require "amq/uri"
require_relative "errors"
require_relative "frame_io"
require_relative "channel"
require_relative "sasl"

module AsyncRabbitMQ
  # Represents one AMQP connection to a RabbitMQ broker.
  #
  # Recovery: exponential backoff with configurable interval, max interval,
  # and retry limit. Re-registers consumers after reconnect.
  #
  # Pool interface: implements reusable?, viable?, concurrency, close for Async::Pool.
  class Session
    CONNECT_TIMEOUT     = 30     # seconds for AMQP handshake
    RPC_TIMEOUT         = 15     # seconds to wait for a synchronous channel reply (nil = forever)
    RECOVERY_INITIAL    = 1.0    # seconds
    RECOVERY_MAX        = 30.0   # seconds
    RECOVERY_JITTER     = 0.25   # ±25%
    PROTOCOL_HEADER     = "AMQP\x00\x00\x09\x01".b.freeze

    attr_reader :host, :port, :vhost, :username, :addresses

    # Build a Session from one or more AMQP URI strings.
    #
    #   Session.from_uri("amqp://user:pass@rabbit:5672/myvhost")
    #   Session.from_uri("amqps://rabbit/myvhost", tls_context: ctx)
    #   Session.from_uri("amqp://rabbit1:5672/vh", "amqp://rabbit2:5672/vh")
    #
    # When multiple URIs are given, credentials, vhost, and TLS settings are
    # taken from the first URI.  Each URI contributes a host:port pair to the
    # +addresses+ list used for failover.
    #
    # Keyword arguments override anything parsed from the URI(s).
    #
    # The standard RabbitMQ URI query parameters are honoured: +heartbeat+,
    # +connection_timeout+, +channel_max+, +auth_mechanism+ and, for amqps://,
    # +verify+, +cacertfile+, +certfile+ and +keyfile+ (which build a
    # +tls_context+ unless one is passed explicitly).
    def self.from_uri(*uri_strings, **kwargs)
      raise ArgumentError, "at least one URI string is required" if uri_strings.empty?

      first_opts = uri_options(uri_strings.first)
      address_list = uri_strings.map do |u|
        parsed = uri_options(u)
        "#{parsed[:host] || 'localhost'}:#{parsed[:port]}"
      end

      # Connection-level settings come from the first URI; addresses from all.
      opts = first_opts.except(:host, :port)
      opts[:addresses] = address_list
      new(**opts.merge(kwargs))
    end

    # Translate an amqp:// or amqps:// URI into Session.new keyword arguments.
    #
    # Parsing is delegated to AMQ::URI from amq-protocol, which percent-decodes
    # the credentials and vhost (so "pa+ss" stays "pa+ss") and validates the
    # scheme, the single-segment vhost path and the TLS-only query parameters.
    def self.uri_options(uri_string)
      parsed = AMQ::URI.parse(uri_string)
      query  = ::URI.parse(uri_string).query
      params = query ? ::URI.decode_www_form(query).to_h : {}

      opts = { tls: !!parsed[:ssl], port: parsed[:port] }
      opts[:host]     = parsed[:host] if parsed[:host]
      opts[:username] = parsed[:user] if parsed[:user]
      opts[:password] = parsed[:pass] if parsed[:pass]
      # "amqp://host/" yields an empty vhost; treat it as the default "/" rather
      # than the (rarely intended) vhost literally named "".
      opts[:vhost] = parsed[:vhost].empty? ? "/" : parsed[:vhost] if parsed.key?(:vhost)

      opts[:heartbeat]       = Integer(params["heartbeat"])          if params.key?("heartbeat")
      opts[:connect_timeout] = Integer(params["connection_timeout"]) if params.key?("connection_timeout")
      opts[:channel_max]     = Integer(params["channel_max"])        if params.key?("channel_max")
      opts[:auth_mechanism]  = params["auth_mechanism"]              if params.key?("auth_mechanism")

      if opts[:tls] && %w[verify cacertfile certfile keyfile].any? { |k| params.key?(k) }
        opts[:tls_context] = tls_context_from_uri(parsed, params)
      end

      opts
    end

    # Build an OpenSSL context from the TLS query parameters of an amqps:// URI.
    # Peer verification stays on unless the URI says verify=false explicitly.
    def self.tls_context_from_uri(parsed, params)
      require "openssl"
      verify = params.key?("verify") ? params["verify"] != "false" : true
      ctx = OpenSSL::SSL::SSLContext.new
      ctx.set_params(verify_mode: verify ? OpenSSL::SSL::VERIFY_PEER : OpenSSL::SSL::VERIFY_NONE)
      ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION
      ctx.ca_file = parsed[:cacertfile] if parsed[:cacertfile]
      ctx.cert    = OpenSSL::X509::Certificate.new(File.read(parsed[:certfile])) if parsed[:certfile]
      ctx.key     = OpenSSL::PKey.read(File.read(parsed[:keyfile]))              if parsed[:keyfile]
      ctx
    end

    private_class_method :uri_options, :tls_context_from_uri

    def initialize(
      host: "localhost",
      port: 5672,
      hosts: nil,
      addresses: nil,
      hosts_shuffle_strategy: :shuffle,
      vhost: "/",
      username: "guest",
      password: "guest",
      tls: false,
      tls_context: nil,
      heartbeat: 60,
      frame_max: 131_072,
      channel_max: 2047,
      connect_timeout: CONNECT_TIMEOUT,
      rpc_timeout: RPC_TIMEOUT,
      auth_mechanism: nil,
      connection_name: nil,
      auto_recover: true,
      recovery_attempts: nil,
      recovery_interval: RECOVERY_INITIAL,
      recovery_max_interval: RECOVERY_MAX,
      logger: Logger.new($stdout)
    )
      @addresses = build_address_list(host, port, hosts, addresses)
      @host                 = @addresses.first[0]
      @port                 = @addresses.first[1]
      @hosts_shuffle_strategy = hosts_shuffle_strategy
      @vhost                = vhost
      @username             = username
      @password             = password
      @tls                  = tls
      @tls_context          = tls_context
      @heartbeat            = heartbeat
      @frame_max            = frame_max
      @channel_max          = channel_max
      @connect_timeout      = connect_timeout
      @rpc_timeout          = rpc_timeout
      @auth_mechanism       = auth_mechanism
      @connection_name      = connection_name
      @auto_recover         = auto_recover
      @recovery_attempts    = recovery_attempts    # nil = unlimited
      @recovery_interval    = recovery_interval
      @recovery_max_interval = recovery_max_interval
      @logger               = logger

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
      @on_blocked              = nil
      @on_unblocked            = nil
      @update_secret_condition = nil
      @on_recovery_attempt     = nil
      @on_recovery             = nil
      @on_recovery_exhausted   = nil
    end

    # Connect and complete AMQP handshake. Raises ConnectionTimeoutError if
    # the handshake does not complete within +connect_timeout+ seconds.
    # Raises NotOpenError if already connected.
    def connect
      raise NotOpenError, "Session is already connected" if open?
      @connecting        = true
      @closed_by_user    = false   # a previous failed connect must not disable recovery
      @session_root_task = Async::Task.current

      last_error = nil
      shuffled_addresses.each do |target_host, target_port|
        begin
          Async::Task.current.with_timeout(@connect_timeout) do
            raw_socket = open_socket(target_host, target_port)
            @frame_io  = build_frame_io(raw_socket)
            @frame_io.start
            handshake
            start_heartbeat_task
            start_channel0_monitor_task
            @host  = target_host
            @port  = target_port
            @state = :open
          end
          @connecting = false
          return
        rescue Async::TimeoutError => e
          @frame_io&.stop rescue nil
          @frame_io = nil
          last_error = e
        rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, Errno::EHOSTUNREACH, Errno::ECONNRESET,
               Errno::EPIPE, EOFError, IOError, SocketError => e
          # TCP connect failed, or the peer dropped the connection mid-handshake.
          @frame_io&.stop rescue nil
          @frame_io = nil
          last_error = e
        rescue OpenSSL::SSL::SSLError => e
          @frame_io&.stop rescue nil
          @frame_io = nil
          last_error = e
        rescue AuthenticationError
          # Credentials or vhost access refused: other nodes will say the same.
          cleanup_after_failed_connect
          raise
        rescue ConnectionError, ChannelError => e
          # The broker refused the handshake (e.g. 530 NOT_ALLOWED for a missing
          # vhost, 320 CONNECTION_FORCED from a node in maintenance). Stop the
          # reader/writer tasks on this socket and try the next address.
          @frame_io&.stop rescue nil
          @frame_io = nil
          last_error = e
        end
      end

      # All addresses exhausted
      cleanup_after_failed_connect
      tried = @addresses.map { |h, p| "#{h}:#{p}" }.join(", ")
      case last_error
      when Async::TimeoutError
        raise ConnectionTimeoutError, "AMQP handshake did not complete within #{@connect_timeout}s (tried #{tried})"
      when OpenSSL::SSL::SSLError
        raise ConnectionTimeoutError, "TLS handshake failed (tried #{tried}) — #{last_error.message}"
      when ConnectionError, ChannelError
        raise last_error
      else
        raise ConnectionTimeoutError, "Could not connect to any host (tried #{tried}) — #{last_error&.message}"
      end
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
      @channels.values.each { |ch| ch.mark_closed!(conn_error) rescue nil }
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
    # +pool_size+ bounds concurrent consumer-handler fibers on the channel
    # (Bunny-parity: default 1). basic_qos will auto-adjust it to prefetch_count.
    def open_channel(pool_size: 1)
      raise NotOpenError, "Session is not open" unless open?
      channel_id = next_channel_id
      channel    = Channel.new(channel_id, self, @frame_io, frame_max: @negotiated_fm || @frame_max, logger: @logger,
                               pool_size: pool_size, rpc_timeout: @rpc_timeout)
      @channel_mutex.acquire { @channels[channel_id] = channel }
      channel.open
      channel
    end

    # Open a channel, yield it to the block, and ensure it is closed afterward.
    def with_channel(pool_size: 1)
      ch = open_channel(pool_size: pool_size)
      begin
        yield ch
      ensure
        ch.close rescue nil
      end
    end

    # Rotate the secret this connection authenticated with, without
    # reconnecting (connection.update-secret, RabbitMQ 3.8+). Used with the
    # OAuth 2 auth backend to hand the broker a refreshed access token before
    # the current one expires. The new secret is also used for reconnects.
    # Raises ConnectionError if the broker refuses (e.g. an auth backend that
    # does not support secret updates closes the connection).
    def update_secret(new_secret, reason = "secret update")
      raise NotOpenError, "Session is not open" unless open?

      cond = @update_secret_condition = Async::Condition.new
      @frame_io.write_frame(AMQ::Protocol::Connection::UpdateSecret.encode(new_secret, reason).encode)
      result = Async::Task.current.with_timeout(@rpc_timeout || RPC_TIMEOUT) { cond.wait }
      raise result if result.is_a?(Exception)

      @password = new_secret
      true
    rescue Async::TimeoutError
      raise RpcTimeoutError, "No reply to connection.update-secret within #{@rpc_timeout || RPC_TIMEOUT}s"
    ensure
      @update_secret_condition = nil
    end

    # Check whether a queue exists on the broker without creating it.
    # Opens a temporary channel and performs a passive declare.
    def queue_exists?(name)
      with_channel { |ch| ch.queue(name, passive: true) }
      true
    rescue ChannelError
      false
    end

    # Check whether an exchange exists on the broker without creating it.
    # Opens a temporary channel and performs a passive declare.
    def exchange_exists?(name)
      with_channel { |ch| ch.exchange(name, passive: true) }
      true
    rescue ChannelError
      false
    end

    # Register a callback invoked when the broker sends connection.blocked.
    # The block receives the reason string from the broker.
    def on_blocked(&block)
      @on_blocked = block
    end

    # Register a callback invoked when the broker sends connection.unblocked.
    def on_unblocked(&block)
      @on_unblocked = block
    end

    # Register a callback invoked at the start of each recovery attempt.
    # The block receives the attempt number (1-based).
    def on_recovery_attempt(&block)
      @on_recovery_attempt = block
    end

    # Register a callback invoked after recovery succeeds.
    # The block receives the session.
    def on_recovery(&block)
      @on_recovery = block
    end

    # Register a callback invoked when recovery attempts are exhausted.
    # Only fires when recovery_attempts is set to a finite number.
    # The block receives the session.
    def on_recovery_exhausted(&block)
      @on_recovery_exhausted = block
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

      if @connecting
        # The handshake is in flight in another fiber; hand it the IO error so
        # it fails now (as a connect failure) rather than sitting in
        # wait_channel0_method until the connect timeout expires.
        q0 = @frame_io&.channel_queue(0)
        q0&.push([:method, error]) rescue nil
        return
      end

      unless @auto_recover
        @state = :closed
        conn_error = ConnectionError.new(code: 0, text: "Connection lost: #{error.message}")
        @channels.values.each { |ch| ch.mark_closed!(conn_error) rescue nil }
        @frame_io&.stop rescue nil
        return
      end

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

      # Waits in flight raise ConnectionError instead of hanging; new operations
      # on the channels park until they are reopened after reconnect.
      conn_error = ConnectionError.new(code: 0, text: "Connection lost: #{error.message}")
      @channels.values.each { |ch| ch.mark_recovering!(conn_error) rescue nil }
      @update_secret_condition&.signal(conn_error)

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

    # Build the canonical [host, port] list from the various input forms.
    #
    #   addresses: ["rabbit1:5672", "rabbit2:5673"]    -> [["rabbit1",5672], ["rabbit2",5673]]
    #   hosts: ["rabbit1", "rabbit2"], port: 5672       -> [["rabbit1",5672], ["rabbit2",5672]]
    #   host: "rabbit1", port: 5672 (default)           -> [["rabbit1",5672]]
    def build_address_list(host, port, hosts, addresses)
      if addresses && !addresses.empty?
        addresses.map do |addr|
          h, p = addr.to_s.split(":", 2)
          [h, p ? p.to_i : port]
        end
      elsif hosts && !hosts.empty?
        hosts.map { |h| [h.to_s, port] }
      else
        [[host.to_s, port]]
      end
    end

    # Return the address list in the order they should be tried for this
    # connect/recovery cycle.
    def shuffled_addresses
      case @hosts_shuffle_strategy
      when :shuffle then @addresses.shuffle
      when :none    then @addresses.dup
      when Proc     then @hosts_shuffle_strategy.call(@addresses)
      else               @addresses.shuffle
      end
    end

    # Stop any in-flight frame_io / recovery tasks spawned during a failed connect.
    def cleanup_after_failed_connect
      @closed_by_user = true
      @connecting     = false
      @recovery_task&.cancel rescue nil
      @recovery_task = nil
      @heartbeat_task&.cancel rescue nil
      @channel0_task&.cancel rescue nil
      @frame_io&.stop rescue nil
      @frame_io = nil
      @state = :closed
    end

    def open_socket(target_host = @host, target_port = @port)
      if @tls
        require "openssl"
        ctx        = @tls_context || build_tls_context
        raw        = TCPSocket.new(target_host, target_port)
        ssl        = OpenSSL::SSL::SSLSocket.new(raw, ctx)
        ssl.hostname = target_host
        ssl.connect
        ssl
      else
        TCPSocket.new(target_host, target_port)
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
      @negotiated_cmax = negotiate_channel_max(msg.channel_max)
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
        # An exception pushed directly by trigger_recovery to unblock this wait
        # (ConnectionError during recovery, or the raw IO error during connect).
        raise method if method.is_a?(Exception)
        if expected_classes.any? { |c| method.is_a?(c) }
          return method
        elsif method.is_a?(AMQ::Protocol::Connection::Close)
          code = method.reply_code
          text = method.reply_text
          # connection.close is always connection-level. 403 ACCESS_REFUSED here
          # means the credentials or the vhost access were rejected.
          if code == 403
            raise AuthenticationError.new(code: code, text: text)
          else
            raise ConnectionError.new(code: code, text: text)
          end
        end
      end
    end

    def send_connection_start_ok(sasl)
      props = {
        "product"      => "async-rabbitmq",
        "version"      => AsyncRabbitMQ::VERSION,
        "platform"     => "Ruby #{RUBY_VERSION}",
        "information"  => "https://github.com/womblep/async-rabbitmq",
        # Extensions this client understands. authentication_failure_close makes
        # RabbitMQ answer bad credentials with connection.close 403 instead of
        # silently dropping the TCP connection (https://www.rabbitmq.com/docs/auth-notification).
        "capabilities" => {
          "publisher_confirms"           => true,
          "consumer_cancel_notify"       => true,
          "exchange_exchange_bindings"   => true,
          "basic.nack"                   => true,
          "connection.blocked"           => true,
          "authentication_failure_close" => true,
        },
      }
      props["connection_name"] = @connection_name if @connection_name
      @frame_io.write_frame(
        AMQ::Protocol::Connection::StartOk.encode(
          props,
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
      # Bound the whole handshake, write included: the broker may never answer,
      # and the write queue may be full behind a stalled socket.
      Async::Task.current.with_timeout(5) do
        @frame_io.write_frame(
          AMQ::Protocol::Connection::Close.encode(200, "Goodbye", 0, 0).encode
        )
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

    # 0 means "no limit" on either side; otherwise the lower value wins.
    def negotiate_channel_max(broker_cmax)
      return (@channel_max == 0 ? 2047 : @channel_max) if broker_cmax == 0
      return broker_cmax if @channel_max == 0
      [@channel_max, broker_cmax].min
    end

    # The negotiated value T is the heartbeat *timeout*. RabbitMQ and the
    # reference clients send a heartbeat every T/2 and treat the peer as dead
    # after roughly two missed heartbeats, so we send at T/2 as well and
    # declare the broker dead when nothing has arrived for 2×T.
    def start_heartbeat_task
      timeout = @negotiated_hb || 60
      return if timeout == 0
      interval   = timeout / 2.0
      dead_after = timeout * 2
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
          if elapsed > dead_after
            trigger_recovery(HeartbeatTimeoutError.new("No frame received in #{elapsed.round(1)}s (timeout #{dead_after}s)"))
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
      delay = @recovery_interval
      attempts = 0

      loop do
        break if @closed_by_user

        attempts += 1

        # Check retry limit (nil = unlimited)
        if @recovery_attempts && attempts > @recovery_attempts
          @logger.warn("Recovery exhausted after #{@recovery_attempts} attempt(s)")
          @state = :closed
          @recovery_in_progress = false
          exhausted = ConnectionError.new(code: 0, text: "Recovery exhausted after #{@recovery_attempts} attempt(s)")
          @channels.values.each { |ch| ch.mark_closed!(exhausted) rescue nil }
          @on_recovery_exhausted&.call(self)
          return
        end

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
        @on_recovery_attempt&.call(attempts)

        connected = false
        shuffled_addresses.each do |target_host, target_port|
          break if @closed_by_user
          begin
            # Wrap the entire reconnect in a timeout so a dead socket doesn't hang forever.
            Async::Task.current.with_timeout(@connect_timeout) do
              raw_socket = open_socket(target_host, target_port)
              @frame_io  = build_frame_io(raw_socket)
              @frame_io.start
              handshake
            end
            @host = target_host
            @port = target_port
            connected = true
            break
          rescue AuthenticationError => e
            # The credentials no longer work; retrying cannot help.
            @frame_io&.stop rescue nil
            @frame_io = nil
            @logger.error("Recovery abandoned: #{e.message}")
            @state = :closed
            @recovery_in_progress = false
            @channels.values.each { |ch| ch.mark_closed!(e) rescue nil }
            @on_recovery_exhausted&.call(self)
            @recovery_task = nil
            return
          rescue => e
            @frame_io&.stop rescue nil
            @logger.debug("Recovery: #{target_host}:#{target_port} failed — #{e.class}: #{e.message}")
          end
        end

        if connected
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
          @on_recovery&.call(self)
          @recovery_task = nil
          return
        else
          @logger.warn("Recovery attempt #{attempts} failed: no reachable host")
          delay = [delay * 2, @recovery_max_interval].min
          break if @closed_by_user
        end
      end
    ensure
      # Make sure state is consistent if we exit for any reason
      @recovery_in_progress = false if @closed_by_user
    end

    def reopen_channels
      @channels.values.each do |channel|
        begin
          channel.reopen_after_recovery(@frame_io)
        rescue => e
          # Fail the channel loudly rather than leave its parked callers hanging.
          @logger.error("Channel #{channel.channel_id} could not be reopened after recovery: #{e.class}: #{e.message}")
          channel.mark_closed!(ChannelError.new("Channel could not be reopened after recovery: #{e.message}",
                                                channel_id: channel.channel_id))
          channel_closed(channel.channel_id)
        end
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
          @on_blocked&.call(method.reason)
        when AMQ::Protocol::Connection::Unblocked
          @frame_io&.set_unblocked
          @on_unblocked&.call
        when AMQ::Protocol::Connection::UpdateSecretOk
          @update_secret_condition&.signal(method)
        when AMQ::Protocol::Connection::Close
          # FrameIO has already answered with close-ok and triggered recovery;
          # fail a pending update_secret with the broker's reason.
          @update_secret_condition&.signal(ConnectionError.new(code: method.reply_code, text: method.reply_text))
        end
        # Other channel-0 methods during normal operation are intentionally
        # ignored; the handshake uses wait_channel0_method, not this loop.
      end
    rescue => e
      @logger.debug("Channel-0 monitor exited: #{e.class}: #{e.message}")
    end
  end
end
