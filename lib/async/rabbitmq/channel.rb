# frozen_string_literal: true

require "async"
require "async/promise"
require "async/semaphore"
require "amq/protocol"

require_relative "error"

module Async
  module RabbitMQ
    # One AMQP channel on a Connection. Internal: applications never hold one.
    # A Publisher or a Consumer owns exactly one, and that is the point of the
    # design - a channel has one job and one owner, so nothing on it has to be
    # shared, and when it fails it fails one thing.
    #
    # It does three things:
    #
    # - #call sends a request and waits for the broker's reply. AMQP's
    #   requests carry nothing to match a reply to, so they go one at a time.
    # - #write sends something that has no reply: a publish, an ack.
    # - what the broker sends unasked - deliveries, returns, confirms, a
    #   cancel, a close - is handed to the channel's +delegate+.
    #
    # A channel is never repaired. A reply that does not come in time closes
    # it, because the reply could still arrive and would be taken for the
    # answer to the next request; a channel the broker closes stays closed.
    # Its owner opens another.
    class Channel
      # What a delegate may implement. Every method is called on the
      # connection's reader fiber and must not wait for anything: the reader
      # is what would deliver the thing waited for.
      module Delegate
        # A message for a consumer on this channel.
        def deliver(method, header, body) end

        # A mandatory message that could not be routed, sent back.
        def returned(method, header, body) end

        # A basic.ack or basic.nack for something published under confirms.
        def confirmed(method) end

        # The broker cancelled a consumer (basic.cancel).
        def cancelled(method) end

        # The channel is closed. +error+ says why: a ChannelError when the
        # broker closed it, a ConnectionError when the connection went, a
        # TimeoutError when a reply did not come, nil when it was closed here.
        def closed(error) end
      end

      CONTENT_METHODS = [
        AMQ::Protocol::Basic::Deliver, AMQ::Protocol::Basic::Return, AMQ::Protocol::Basic::GetOk
      ].freeze

      # How long a channel.close waits for its close-ok.
      CLOSE_TIMEOUT = 5

      attr :id, :connection

      # Why the channel closed; nil while it is open or when it was closed here.
      attr :error

      attr_accessor :delegate

      def initialize(connection, id, delegate = nil)
        @connection = connection
        @id         = id
        @delegate   = delegate
        @calls      = Async::Semaphore.new(1)
        @reply      = nil # the Promise the call in flight is waiting on
        @expected   = nil
        @closed     = false
        @error      = nil
        # Set once a close has been sent after a timeout: everything but the
        # close-ok is then somebody else's late answer, and is dropped.
        @abandoned  = false
        @content_method = nil
        @content_header = nil
        @content_body   = nil
        @content_left   = 0
      end

      def closed?
        @closed
      end

      def open?
        !@closed
      end

      # Internal: Connection#open_channel.
      def open
        call(AMQ::Protocol::Channel::Open.encode(@id, ""), AMQ::Protocol::Channel::OpenOk)
        self
      end

      # Send +frame+ and return the reply, which must be one of +expected+.
      # Raises ChannelError if the broker closes the channel instead,
      # ConnectionError if the connection goes, and TimeoutError - closing the
      # channel - if nothing comes within +timeout+ seconds.
      def call(frame, *expected, timeout: @connection.call_timeout)
        @calls.acquire do
          raise closed_error if @closed

          promise   = @reply = Async::Promise.new
          @expected = expected
          begin
            @connection.write(frame.encode)
            # wait? and not with_timeout: a timeout raised into this fiber by
            # something the caller wrapped around us must not be taken for ours.
            abandon!(expected, timeout) if promise.wait?(timeout: timeout).nil?
            promise.wait
          ensure
            @reply = nil if @reply.equal?(promise)
          end
        end
      end

      # Send frames that have no reply.
      def write(bytes)
        raise closed_error if @closed

        @connection.write(bytes)
      end

      # Send a message. Waits while the broker has the connection blocked.
      def write_content(bytes)
        raise closed_error if @closed

        @connection.write_content(bytes)
      end

      # Close the channel. The delegate is not told: it asked.
      def close
        return if @closed

        begin
          call(AMQ::Protocol::Channel::Close.encode(@id, 200, "Goodbye", 0, 0),
               AMQ::Protocol::Channel::CloseOk, timeout: CLOSE_TIMEOUT)
        rescue Error
          # Closed by something else meanwhile, or the broker never answered.
          # Either way there is nothing more to do to it.
        end
        finish(nil, notify: false) unless @closed
        nil
      end

      def inspect
        "#<#{self.class.name} #{@id} #{@closed ? 'closed' : 'open'}>"
      end

      # -----------------------------------------------------------------------
      # Internal: called by the Connection, on its reader fiber.
      # -----------------------------------------------------------------------

      def receive(frame)
        case frame
        when AMQ::Protocol::MethodFrame then receive_method(frame.decode_payload)
        when AMQ::Protocol::HeaderFrame then receive_header(frame)
        when AMQ::Protocol::BodyFrame   then receive_body(frame)
        end
      end

      # The connection is gone, and this channel with it.
      def connection_closed(error)
        finish(error, release: false) unless @closed
      end

      private

      def receive_method(method)
        if @abandoned
          release_abandoned if method.is_a?(AMQ::Protocol::Channel::CloseOk)
          return
        end

        case method
        when AMQ::Protocol::Channel::Close
          # Acknowledged before anything else: the broker holds the channel id
          # until it hears, and the owner will want another channel.
          @connection.write(AMQ::Protocol::Channel::CloseOk.encode(@id).encode) rescue nil
          finish(ChannelError.new(code: method.reply_code, text: method.reply_text, method: method))
        when *CONTENT_METHODS
          @content_method = method
        when AMQ::Protocol::Basic::Ack, AMQ::Protocol::Basic::Nack
          notify { @delegate&.confirmed(method) }
        when AMQ::Protocol::Basic::Cancel
          notify { @delegate&.cancelled(method) }
        else
          reply(method)
        end
      end

      def reply(method)
        promise = @reply
        if promise && @expected.any? { |klass| method.is_a?(klass) }
          @reply = nil
          promise.resolve(method)
        else
          Console.debug(self, "Unexpected method on channel #{@id}", method: method.class.name)
        end
      end

      def receive_header(frame)
        return if @abandoned || @content_method.nil?

        @content_header = frame
        @content_left   = frame.body_size
        # A zero-length body has no body frames at all.
        return complete_content("".b) if @content_left.zero?

        @content_body = nil
      end

      def receive_body(frame)
        return if @abandoned || @content_header.nil?

        chunk = frame.payload
        @content_left -= chunk.bytesize
        # Most bodies fit one frame, so the common case keeps the payload as
        # it came and copies nothing.
        if @content_body.nil?
          return complete_content(chunk) if @content_left <= 0

          @content_body = String.new(chunk, capacity: chunk.bytesize + @content_left, encoding: Encoding::BINARY)
        else
          @content_body << chunk
          complete_content(@content_body) if @content_left <= 0
        end
      end

      def complete_content(body)
        method = @content_method
        header = @content_header
        @content_method = @content_header = @content_body = nil

        case method
        when AMQ::Protocol::Basic::Deliver then notify { @delegate&.deliver(method, header, body) }
        when AMQ::Protocol::Basic::Return  then notify { @delegate&.returned(method, header, body) }
        end
      end

      # A delegate that raises must not take the reader, and so the whole
      # connection, down with it.
      def notify
        yield
      rescue StandardError => error
        Console.error(self, "Channel #{@id}: delegate raised", exception: error)
      end

      # No reply in time. The request may yet be answered, and that answer
      # would be handed to whoever calls next, so the channel is finished. It
      # is closed on the broker too, and its id kept back until the broker
      # agrees it is closed: until then anything arriving for the id belongs
      # to this channel's past.
      def abandon!(expected, timeout)
        wanted = expected.map { |klass| klass.name.split("::").last(2).join(".") }.join(" or ")
        error  = TimeoutError.new("No #{wanted} from the broker within #{timeout}s on channel #{@id}")
        @abandoned = true
        begin
          @connection.write(AMQ::Protocol::Channel::Close.encode(@id, 200, "Request timed out", 0, 0).encode)
        rescue Error
          # The connection is going too; it will release the id.
        end
        finish(error, release: false)
        raise error
      end

      def release_abandoned
        @connection.release(self)
      end

      def finish(error, release: true, notify: true)
        return if @closed

        @closed = true
        @error  = error
        promise = @reply
        @reply  = nil
        @content_method = @content_header = @content_body = nil
        @connection.release(self) if release
        promise&.reject(error || closed_error) unless promise&.resolved?
        notify { @delegate&.closed(error) } if notify
      end

      def closed_error
        return @error if @error.is_a?(ConnectionError)

        ClosedError.new("Channel #{@id} is closed#{@error ? ": #{@error.message}" : ''}")
      end
    end
  end
end
