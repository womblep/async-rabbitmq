# frozen_string_literal: true

require "async"
require "async/promise"
require "async/condition"
require "console"
require "amq/protocol"
require "openssl"
require "socket"

require_relative "version"
require_relative "error"
require_relative "sasl"
require_relative "channel"
require_relative "channel_id_allocator"
require_relative "protocol/framer"

module Async
  module RabbitMQ
    # One connection to one broker node: the socket, the handshake, the
    # heartbeat, and the channels multiplexed over it.
    #
    #   connection = Async::RabbitMQ::Connection.open(endpoint)
    #   channel    = connection.open_channel
    #   ...
    #   connection.close
    #
    # A connection is disposable. It is never repaired: when it fails, for any
    # reason, it closes, every channel on it closes with the same error, every
    # fiber waiting on any of them is released with it, and #wait returns. A
    # Client notices and opens another. That is the whole of this class's part
    # in recovery, and it is why nothing here has a "recovering" state.
    class Connection
      # How long a connection.close waits for its close-ok.
      CLOSE_TIMEOUT = 5

      CLIENT_PROPERTIES = {
        "product"     => "async-rabbitmq",
        "version"     => VERSION,
        "platform"    => "Ruby #{RUBY_VERSION}",
        "information" => "https://github.com/womblep/async-rabbitmq",
        # Extensions this client understands. authentication_failure_close has
        # the broker answer bad credentials with a connection.close 403, where
        # it would otherwise just drop the socket.
        "capabilities" => {
          "publisher_confirms"           => true,
          "consumer_cancel_notify"       => true,
          "exchange_exchange_bindings"   => true,
          "basic.nack"                   => true,
          "connection.blocked"           => true,
          "authentication_failure_close" => true
        }.freeze
      }.freeze

      # Connect to +endpoint+ and complete the handshake, within the
      # endpoint's +timeout+. The reader and heartbeat tasks are children of
      # +parent+ and transient: they never keep it alive, and they end when
      # the connection closes.
      def self.open(endpoint, parent: Async::Task.current)
        connection = nil
        begin
          Async::Task.current.with_timeout(endpoint.timeout) do
            connection = new(endpoint, endpoint.connect)
            connection.handshake
          end
        rescue Async::TimeoutError
          connection&.close(ConnectTimeoutError.new("Could not connect to #{endpoint} within #{endpoint.timeout}s"))
          raise ConnectTimeoutError, "Could not connect to #{endpoint} within #{endpoint.timeout}s"
        rescue Exception => error # rubocop:disable Lint/RescueException
          # Any way out but success, a cancel included, must not leave a socket.
          connection&.close(error.is_a?(StandardError) ? error : nil)
          # One kind of error for "could not connect", whatever layer said so:
          # the resolver, the socket or TLS.
          case error
          when SystemCallError, IOError, SocketError, OpenSSL::SSL::SSLError
            raise ConnectionError, "Could not connect to #{endpoint}: #{error.class}: #{error.message}"
          end
          raise
        end
        connection.start(parent: parent)
        connection
      end

      attr :endpoint

      # Negotiated with the broker during the handshake.
      attr :frame_max, :channel_max, :heartbeat

      # What the broker said about itself in connection.start.
      attr :server_properties

      # Why the connection closed; nil while it is open or when it was closed here.
      attr :error

      def initialize(endpoint, socket)
        @endpoint  = endpoint
        @framer    = Protocol::Framer.new(socket)
        @channels  = {}
        @ids       = nil
        @state     = :connecting # then :open, :closing, :closed
        @error     = nil
        @finished  = Async::Promise.new
        @close_ok  = nil
        @blocked   = nil # the broker's reason while it has publishing blocked
        @unblocked = nil # Promise for one blocked episode
        @reader    = nil
        @heartbeater = nil
        @flusher   = nil
        @flush_wanted  = Async::Condition.new
        @flush_pending = false
        @last_read = now
      end

      def open?
        @state == :open
      end

      def closed?
        @state == :closed
      end

      # Seconds a Channel#call waits for its reply.
      def call_timeout
        @endpoint.call_timeout
      end

      # How many channels are open on this connection.
      def channel_count
        @channels.size
      end

      # The reason the broker gave for blocking publishers (a resource alarm),
      # or nil when it has not.
      def blocked
        @blocked
      end

      def blocked?
        !@blocked.nil?
      end

      # Block until the connection has closed. Returns why: the error, or nil
      # if it was closed here.
      def wait
        @finished.wait
      end

      # Open a channel, with +delegate+ to receive what the broker sends on it
      # (see Channel::Delegate).
      def open_channel(delegate = nil)
        raise closed_error unless open?

        id = @ids.allocate or
          raise ChannelLimitError, "All #{@channel_max} channels on #{@endpoint} are in use"
        channel = @channels[id] = Channel.new(self, id, delegate)
        begin
          channel.open
        rescue Exception # rubocop:disable Lint/RescueException
          # Not opened, or not known to be: an id whose open may still be
          # answered cannot be handed to the next caller. A channel that timed
          # out has closed itself and holds its id until the broker answers.
          release(channel) unless channel.closed?
          raise
        end
      end

      # Close the connection. With no +error+ this is the orderly close: the
      # broker is told and given a moment to agree. With one it is immediate.
      # Either way every channel is closed and everything waiting is released.
      def close(error = nil)
        return if @state == :closed || @state == :closing

        orderly = error.nil? && @state == :open
        @state  = :closing
        say_goodbye if orderly
        finish(error)
        nil
      end

      def inspect
        "#<#{self.class.name} #{@endpoint} #{@state} channels=#{@channels.size}>"
      end

      # -----------------------------------------------------------------------
      # Internal: used by Channel, and by .open.
      # -----------------------------------------------------------------------

      # Send whole frames. Raises ConnectionError if the connection has closed
      # or closes in the attempt.
      #
      # The frames go into the stream's buffer, and the flusher task puts them
      # on the wire the next time the reactor comes round to it. Everything
      # written in between - a hundred fibers each publishing one message, say
      # - goes out in one write to the socket, where sending each as it came
      # would cost a system call apiece. A fiber that writes and then waits
      # for the reply loses nothing: its waiting is what lets the flusher run.
      def write(bytes)
        raise closed_error if @state == :closed

        # The stream sends the buffer itself once it is large, from this
        # fiber. A cancel that landed part way through that would leave half a
        # frame on the wire and the stream beyond use, so it is held off.
        uninterrupted { @framer.write(bytes, flush: false) }
        unless @flush_pending
          @flush_pending = true
          @flush_wanted.signal
        end
      rescue IOError, SystemCallError => error
        failure = ConnectionError.new("Connection to #{@endpoint} lost: #{error.class}: #{error.message}")
        finish(failure)
        raise failure
      end

      # Send a message, waiting first for as long as the broker has publishing
      # blocked. Acks, closes and everything else are never held back, so a
      # resource alarm cannot stop a consumer settling or a shutdown finishing.
      def write_content(bytes)
        wait_unblocked
        write(bytes)
      end

      # Wait for as long as the broker has publishing blocked. Raises
      # ConnectionError if the connection closes first.
      def wait_unblocked
        while @blocked
          outcome = (@unblocked ||= Async::Promise.new).wait
          raise outcome if outcome.is_a?(Exception)
        end
      end

      # Give a closed channel's id back.
      def release(channel)
        return unless @channels[channel.id].equal?(channel)

        @channels.delete(channel.id)
        @ids&.release(channel.id)
      end

      # The AMQP handshake: protocol header, start, tune, open. Run on the
      # calling fiber, before there is a reader task.
      def handshake
        @framer.write_protocol_header

        start = read_method(AMQ::Protocol::Connection::Start)
        @server_properties = start.server_properties
        sasl = SASL.negotiate(start.mechanisms, preferred: @endpoint.auth_mechanism,
                                                username: @endpoint.username, password: @endpoint.password,
                                                prefer_external: @endpoint.client_certificate?)
        properties = CLIENT_PROPERTIES
        properties = properties.merge("connection_name" => @endpoint.connection_name) if @endpoint.connection_name
        @framer.write(AMQ::Protocol::Connection::StartOk.encode(properties, sasl.mechanism_name,
                                                                sasl.initial_response, "en_US").encode)

        # The broker may put challenges before the tune.
        tune = read_method(AMQ::Protocol::Connection::Tune, AMQ::Protocol::Connection::Secure)
        while tune.is_a?(AMQ::Protocol::Connection::Secure)
          @framer.write(AMQ::Protocol::Connection::SecureOk.encode(sasl.challenge_response(tune.challenge)).encode)
          tune = read_method(AMQ::Protocol::Connection::Tune, AMQ::Protocol::Connection::Secure)
        end
        @heartbeat   = negotiate(@endpoint.heartbeat, tune.heartbeat)
        @frame_max   = negotiate(@endpoint.frame_max, tune.frame_max)
        @channel_max = negotiate(@endpoint.channel_max, tune.channel_max)
        @channel_max = 2047 if @channel_max.zero?
        @framer.write(AMQ::Protocol::Connection::TuneOk.encode(@channel_max, @frame_max, @heartbeat).encode)

        @framer.write(AMQ::Protocol::Connection::Open.encode(@endpoint.vhost).encode)
        read_method(AMQ::Protocol::Connection::OpenOk)

        @ids   = ChannelIdAllocator.new(@channel_max)
        @state = :open
        self
      end

      # Start the reader and the heartbeat.
      def start(parent: Async::Task.current)
        @last_read   = now
        @reader      = parent.async(transient: true) { read_frames }
        @heartbeater = parent.async(transient: true) { send_heartbeats } if @heartbeat.positive?
        @flusher     = parent.async(transient: true) { flush_writes }
        self
      end

      private

      # async renamed stop to cancel and defer_stop to defer_cancel; the old
      # names still work but warn. Either spelling is used as it is found.
      if Async::Task.method_defined?(:defer_cancel)
        def uninterrupted(&block) = Async::Task.current.defer_cancel(&block)
        def cancel(task) = task.cancel
      else
        def uninterrupted(&block) = Async::Task.current.defer_stop(&block)
        def cancel(task) = task.stop
      end

      def now
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      # 0 means "no limit" or "none" from either side; otherwise the lower wins.
      def negotiate(ours, theirs)
        return ours   if theirs.zero?
        return theirs if ours.zero?

        [ours, theirs].min
      end

      # During the handshake: the next method on channel 0, which must be one
      # of +expected+. The broker refuses a connection by closing it.
      def read_method(*expected)
        loop do
          frame = @framer.read_frame
          next unless frame.is_a?(AMQ::Protocol::MethodFrame)

          method = frame.decode_payload
          return method if expected.any? { |klass| method.is_a?(klass) }
          next unless method.is_a?(AMQ::Protocol::Connection::Close)

          refused = method.reply_code == 403 ? AuthenticationError : ConnectionError
          raise refused.new(code: method.reply_code, text: method.reply_text)
        end
      rescue EOFError, IOError, SystemCallError => error
        # A broker without authentication_failure_close, or one that dislikes
        # the protocol header, just hangs up.
        raise ConnectionError, "#{@endpoint} closed the connection during the handshake (#{error.class})"
      end

      def read_frames
        failure = nil
        loop do
          frame = @framer.read_frame
          @last_read = now
          receive(frame)
        end
      rescue EOFError, IOError, SystemCallError => error
        # Nothing to report if this is our own close taking the socket away.
        failure = ConnectionError.new("Connection to #{@endpoint} lost: #{error.class}: #{error.message}")
      rescue Error => error
        failure = error
      rescue StandardError => error
        Console.error(self, "Reader failed", exception: error)
        failure = ConnectionError.new("Connection to #{@endpoint} failed: #{error.class}: #{error.message}")
      ensure
        # However the reader ends, the connection is over: nothing else would
        # ever notice a frame again.
        finish(failure || @error) unless @state == :closed
      end

      # Put on the wire what #write has buffered. While this is in the middle
      # of a write to a socket that is full, the stream's lock is held, and
      # the fibers writing wait behind it: that is the backpressure.
      def flush_writes
        loop do
          @flush_wanted.wait until @flush_pending
          @flush_pending = false
          @framer.flush
        end
      rescue IOError, SystemCallError => error
        finish(ConnectionError.new("Connection to #{@endpoint} lost: #{error.class}: #{error.message}"))
      end

      def receive(frame)
        channel_id = frame.channel
        return receive_connection_method(frame.decode_payload) if channel_id.zero? && frame.is_a?(AMQ::Protocol::MethodFrame)
        return if channel_id.zero? # a heartbeat; arriving at all is its whole message

        @channels[channel_id]&.receive(frame)
      end

      def receive_connection_method(method)
        case method
        when AMQ::Protocol::Connection::Close
          # Answered at once: without a close-ok the broker waits out a
          # timeout of its own before it lets the socket go.
          @framer.write(AMQ::Protocol::Connection::CloseOk.encode.encode) rescue nil
          refused = method.reply_code == 403 ? AuthenticationError : ConnectionError
          finish(refused.new(code: method.reply_code, text: method.reply_text))
        when AMQ::Protocol::Connection::CloseOk
          @close_ok&.resolve(true)
        when AMQ::Protocol::Connection::Blocked
          @blocked = method.reason.to_s
          Console.warn(self, "Broker has blocked publishing", endpoint: @endpoint.to_s, reason: @blocked)
        when AMQ::Protocol::Connection::Unblocked
          @blocked = nil
          release_blocked(nil)
          Console.info(self, "Broker has unblocked publishing", endpoint: @endpoint.to_s)
        end
      end

      def release_blocked(outcome)
        promise    = @unblocked
        @unblocked = nil
        promise&.resolve(outcome)
      end

      # The negotiated heartbeat T is a timeout. Like the broker and the
      # reference clients, send one every T/2 and call the peer dead after
      # 2T of silence.
      def send_heartbeats
        interval   = @heartbeat / 2.0
        dead_after = @heartbeat * 2
        loop do
          sleep interval
          break unless @state == :open

          # Liveness first. The write below can queue behind one that is stuck
          # on a full send buffer after a partition; checked afterwards, this
          # task would be stuck with it and nothing would call the peer dead.
          silence = now - @last_read
          if silence > dead_after
            finish(HeartbeatTimeoutError.new("Nothing from #{@endpoint} for #{silence.round(1)}s " \
                                             "(heartbeat timeout #{@heartbeat}s)"))
            break
          end

          begin
            # Bounded by the whole dead-peer window, not by the interval: a
            # large message going out over a slow link can fairly hold the
            # stream for longer than one interval.
            Async::Task.current.with_timeout(dead_after) { @framer.write_heartbeat }
          rescue Async::TimeoutError
            finish(HeartbeatTimeoutError.new("Could not write to #{@endpoint} for #{dead_after}s; it is not reading"))
            break
          rescue IOError, SystemCallError => error
            finish(ConnectionError.new("Connection to #{@endpoint} lost: #{error.class}: #{error.message}"))
            break
          end
        end
      end

      # Tell the broker, and give it CLOSE_TIMEOUT to agree. A broker that
      # does not answer is not waited for: the socket is closed regardless.
      def say_goodbye
        @close_ok = Async::Promise.new
        @framer.write(AMQ::Protocol::Connection::Close.encode(200, "Goodbye", 0, 0).encode)
        @close_ok.wait?(timeout: CLOSE_TIMEOUT)
      rescue IOError, SystemCallError, Error
        # Already gone.
      end

      # The one place a connection ends. Idempotent: a reader that fails, a
      # heartbeat that times out and a close from the application can all
      # arrive together, and the first wins.
      def finish(error)
        return if @state == :closed

        @state = :closed
        @error = error
        @framer.close rescue nil
        @blocked = nil
        release_blocked(closed_error)
        channels  = @channels.values
        @channels = {}
        channels.each { |channel| channel.connection_closed(closed_error) }
        @finished.resolve(error)
        current = Async::Task.current?
        [@reader, @heartbeater, @flusher].each { |task| cancel(task) if task && !task.equal?(current) }
      end

      def closed_error
        @error.is_a?(ConnectionError) ? @error : ConnectionError.new("Connection to #{@endpoint} is closed")
      end
    end
  end
end
