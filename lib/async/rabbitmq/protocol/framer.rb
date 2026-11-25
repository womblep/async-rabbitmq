# frozen_string_literal: true

require "amq/protocol"
require "io/stream"

require_relative "../error"

module Async
  module RabbitMQ
    module Protocol
      # Reads and writes AMQP 0-9-1 frames on a stream. It knows the framing
      # and nothing else: no tasks, no channels, no state beyond the stream's
      # buffers. Encoding and decoding of what is inside a frame is
      # amq-protocol's.
      class Framer
        # The eight bytes a client opens with: "AMQP", then 0, 0, 9, 1.
        PROTOCOL_HEADER = "AMQP\x00\x00\x09\x01".b.freeze

        HEADER_SIZE = 7
        TERMINATOR  = "\xCE".b.freeze

        HEARTBEAT = AMQ::Protocol::HeartbeatFrame.encode.freeze

        def initialize(io)
          @stream = ::IO::Stream(io)
        end

        attr :stream

        # The next frame: an AMQ::Protocol::MethodFrame, HeaderFrame, BodyFrame
        # or HeartbeatFrame. Raises EOFError when the peer has closed, and
        # ProtocolError when what arrived is not a frame.
        def read_frame
          header = @stream.read_exactly(HEADER_SIZE)
          type, channel, size = AMQ::Protocol::Frame.decode_header(header)
          payload    = size.zero? ? "".b : @stream.read_exactly(size)
          terminator = @stream.read_exactly(1)
          unless terminator == TERMINATOR
            raise ProtocolError, "Frame did not end with 0xCE (got #{terminator.inspect})"
          end

          AMQ::Protocol::Frame::CLASSES[AMQ::Protocol::Frame::TYPES[type]].new(payload, channel)
        rescue AMQ::Protocol::Error => error
          raise ProtocolError, "Invalid frame from the broker: #{error.message}"
        end

        # Write +bytes+, which must be whole frames. The stream appends under a
        # lock of its own, so frames written from different fibers never
        # interleave. Without +flush+ they wait in the stream's buffer for a
        # #flush, or for the buffer to fill.
        def write(bytes, flush: true)
          @stream.write(bytes, flush: flush)
        end

        # Send whatever is waiting in the buffer.
        def flush
          @stream.flush
        end

        def write_protocol_header
          write(PROTOCOL_HEADER)
        end

        def write_heartbeat
          write(HEARTBEAT)
        end

        def close
          @stream.close
        end

        def closed?
          @stream.closed?
        end
      end
    end
  end
end
