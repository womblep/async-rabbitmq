require "async"
require "async/queue"
require "async/limited_queue"
require "async/semaphore"
require "socket"
require "amq/protocol"
require_relative "errors"

module AsyncRabbitMQ
  # Handles low-level AMQP frame reading and writing over a native Ruby socket.
  # async 2.x's fiber scheduler makes blocking socket reads yield automatically.
  #
  # Architecture:
  #   Reader Task — reads frames, routes to per-channel Async::Queue.
  #   Writer Task — drains a bounded Async::LimitedQueue, serializes all writes.
  #   Heartbeat   — writes directly via Async::Semaphore (bypasses write queue).
  #
  # AMQ::Protocol::Frame.decode raises NotImplementedError (abstract stub).
  # Frame reading uses manual 3-step IO instead.
  class FrameIO
    FRAME_HEADER_SIZE = 7
    FRAME_TERMINATOR  = "\xCE".b.freeze
    WRITE_QUEUE_LIMIT = 1024

    # AMQP soft-error codes: broker closes the channel, connection stays alive.
    SOFT_ERROR_CODES = [403, 404, 405, 406].freeze

    def initialize(socket, logger: Logger.new($stdout, level: Logger::WARN))
      @socket        = socket
      @logger        = logger
      @channels      = {}
      @channel_sem   = Async::Semaphore.new(1)
      @write_queue   = Async::LimitedQueue.new(WRITE_QUEUE_LIMIT)
      @socket_sem    = Async::Semaphore.new(1)
      @running       = false
      @reader_task   = nil
      @writer_task   = nil
      @content_state = {}
      @on_frame      = nil   # optional proc called each time a frame is read
    end

    # Set a callback invoked immediately after each frame is successfully read.
    # Used by Session to update its heartbeat timestamp.
    def on_frame=(proc)
      @on_frame = proc
    end

    def register_channel(channel_id)
      @channel_sem.acquire do
        q = @channels[channel_id] = Async::Queue.new
        q
      end
    end

    def unregister_channel(channel_id)
      @channel_sem.acquire do
        @channels.delete(channel_id)
        @content_state.delete(channel_id)
      end
    end

    def channel_queue(channel_id)
      @channels[channel_id]
    end

    def start(task: Async::Task.current)
      @running = true
      @writer_task = task.async { writer_loop }
      @reader_task = task.async { reader_loop }
    end

    def stop
      @running = false
      @socket.close rescue nil
      # Push nil sentinel to every channel queue FIRST so blocked poppers
      # (e.g. wait_channel0_method) are unblocked before we cancel tasks.
      # Cancelling the CURRENT task raises Async::Cancel (< Exception, not
      # StandardError), which bypasses inline `rescue nil` and would skip
      # the push if we did it after.
      @channels.each_value { |q| q.push(nil) rescue nil }
      @writer_task&.cancel rescue nil
      @reader_task&.cancel rescue nil
    end

    # Enqueue raw frame bytes for writing. Yields the fiber when queue is full.
    def write_frame(data)
      @write_queue.push(data)
    end

    # Write heartbeat directly to socket, bypassing the write queue so
    # heartbeats are never starved by backpressure.
    # Silently swallows IO errors — the reader_loop will detect and recover.
    def write_heartbeat
      heartbeat = AMQ::Protocol::HeartbeatFrame.encode
      @socket_sem.acquire { @socket.write(heartbeat) }
    rescue IOError, Errno::EPIPE, Errno::EBADF
      # Socket is closed; reader_loop will raise and call trigger_recovery.
    end

    private

    def writer_loop
      while @running
        data = @write_queue.pop
        @socket_sem.acquire { @socket.write(data) }
      end
    rescue IOError, Errno::EPIPE, Errno::EBADF, Errno::ECONNRESET
      # Socket closed — reader_loop will detect and call trigger_recovery.
    rescue => e
      @logger.error("FrameIO writer: #{e.class}: #{e.message}")
    end

    def reader_loop
      while @running
        frame = read_frame
        @on_frame.call if @on_frame
        dispatch(frame)
      end
    rescue EOFError, Errno::ECONNRESET, Errno::EPIPE, IOError, Errno::EBADF => e
      @logger.warn("FrameIO reader closed: #{e.class}")
      trigger_recovery(e)
    rescue AMQ::Protocol::Error => e
      @logger.error("FrameIO AMQP error: #{e.class}: #{e.message}")
      trigger_recovery(e)
    rescue => e
      @logger.error("FrameIO reader error: #{e.class}: #{e.message}")
      trigger_recovery(e)
    end

    # Read one AMQP frame via manual 3-step IO.
    # Returns an AMQ::Protocol::MethodFrame / HeaderFrame / BodyFrame / HeartbeatFrame.
    def read_frame
      header_bytes             = read_exactly(FRAME_HEADER_SIZE)
      type_int, channel_id,
        payload_size           = decode_header(header_bytes)
      payload                  = read_exactly(payload_size)
      terminator               = read_exactly(1)

      unless terminator == FRAME_TERMINATOR
        raise AMQ::Protocol::Error,
              "Invalid frame terminator: #{terminator.inspect} (expected 0xCE)"
      end

      frame_class = AMQ::Protocol::Frame::CLASSES[type_int]
      raise AMQ::Protocol::Error, "Unknown frame type: #{type_int}" unless frame_class

      frame_class.new(payload, channel_id)
    end

    # Read exactly n bytes, yielding the fiber at each blocking read.
    def read_exactly(n)
      return "".b if n == 0
      buf = "".b
      while buf.bytesize < n
        chunk = @socket.read(n - buf.bytesize)
        raise EOFError, "Socket closed mid-frame" if chunk.nil? || chunk.empty?
        buf << chunk
      end
      buf
    end

    # Decode the 7-byte AMQP frame header.
    # Layout: type(1) + channel(2) + payload_size(4)
    def decode_header(bytes)
      frame_type   = bytes.getbyte(0)
      channel_id   = (bytes.getbyte(1) << 8) | bytes.getbyte(2)
      payload_size = (bytes.getbyte(3) << 24) | (bytes.getbyte(4) << 16) |
                     (bytes.getbyte(5) << 8)  |  bytes.getbyte(6)
      [frame_type, channel_id, payload_size]
    end

    def dispatch(frame_obj)
      channel_id = frame_obj.channel

      case frame_obj
      when AMQ::Protocol::MethodFrame
        method = frame_obj.decode_payload

        # Broker-initiated Connection::Close must be acknowledged immediately.
        # Without a timely CloseOk the broker waits (up to ~30 s) before
        # force-closing the TCP connection, e.g. when a vhost is deleted while
        # a connection is still open.  We send CloseOk synchronously here,
        # then route the method to channel 0 (so wait_channel0_method can also
        # see it and raise a proper error if it is active), and finally trigger
        # recovery so the session reconnects.
        if channel_id == 0 && method.is_a?(AMQ::Protocol::Connection::Close)
          begin
            @socket_sem.acquire do
              @socket.write(AMQ::Protocol::Connection::CloseOk.encode.encode)
            end
          rescue => e
            @logger.warn("FrameIO: could not send Connection::CloseOk — #{e.message}")
          end
          route_to_channel(0, [:method, method])
          trigger_recovery(
            ConnectionError.new(code: method.reply_code, text: method.reply_text)
          )
          return
        end

        route_to_channel(channel_id, [:method, method])

      when AMQ::Protocol::HeaderFrame
        @content_state[channel_id] = {
          header:     frame_obj,
          body_parts: [],
          remaining:  frame_obj.body_size,
        }

      when AMQ::Protocol::BodyFrame
        state = @content_state[channel_id]
        unless state
          @logger.warn("Body frame on channel #{channel_id} without content header — dropped")
          return
        end
        chunk = frame_obj.payload
        state[:body_parts] << chunk
        state[:remaining]  -= chunk.bytesize

        if state[:remaining] <= 0
          body = state[:body_parts].join.b
          @content_state.delete(channel_id)
          route_to_channel(channel_id, [:content, state[:header], body])
        end

      when AMQ::Protocol::HeartbeatFrame
        route_to_channel(0, [:heartbeat])

      else
        @logger.warn("Unknown frame type #{frame_obj.class} on channel #{channel_id} — dropped")
      end
    end

    def route_to_channel(channel_id, message)
      queue = @channels[channel_id]
      if queue
        queue.push(message)
      else
        @logger.debug("No queue for channel #{channel_id} — frame dropped (#{message.first})")
      end
    end

    # Overridden by Session to initiate reconnect.
    def trigger_recovery(error)
      @running = false
    end
  end
end
