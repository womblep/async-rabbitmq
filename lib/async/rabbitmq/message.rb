# frozen_string_literal: true

require "amq/protocol"

require_relative "error"

module Async
  module RabbitMQ
    # One delivered message, and the means to settle it.
    #
    #   consumer.each do |message|
    #     process(message.body)
    #     message.ack
    #   end
    #
    # A message is settled at most once. The first of #ack, #nack and #reject
    # wins and the rest return false without sending anything, so a handler
    # that acks and an error path that then nacks cannot draw the "unknown
    # delivery tag" that would close the consumer's channel.
    #
    # A message belongs to the channel it arrived on. Once that channel has
    # gone - the connection was lost - the broker has put the message back on
    # its queue and will deliver it again, so this copy is #stale?: settling
    # it sends nothing and returns false.
    class Message
      # Content properties readable as methods. Each returns nil when the
      # publisher did not set it.
      PROPERTIES = %i[
        content_type content_encoding priority correlation_id reply_to expiration
        message_id timestamp type user_id app_id delivery_mode
      ].freeze

      P = AMQ::Protocol
      EMPTY_HASH = {}.freeze
      private_constant :P, :EMPTY_HASH

      attr :body

      # How this message was settled: :ack, :nack, :reject, :stale (its
      # channel had gone, so there was nothing to settle), or nil while it is
      # still outstanding.
      attr :settlement

      # Internal: +method+ is the basic.deliver the message arrived with and
      # +header+ its content header frame.
      def initialize(channel, method, header, body)
        @channel    = channel
        @method     = method
        @header     = header
        @body       = body
        @properties = nil
        @settlement = nil
      end

      def delivery_tag
        @method.delivery_tag
      end

      def exchange
        @method.exchange
      end

      def routing_key
        @method.routing_key
      end

      # True if the broker has delivered this message before.
      def redelivered?
        @method.redelivered ? true : false
      end

      def consumer_tag
        @method.consumer_tag
      end

      # The content properties as a Hash.
      def properties
        @properties ||= @header.properties || EMPTY_HASH
      end

      # The application headers table; empty when the publisher sent none.
      def headers
        properties[:headers] || EMPTY_HASH
      end

      PROPERTIES.each do |name|
        define_method(name) { properties[name] }
      end

      def persistent?
        delivery_mode == 2
      end

      # When the message was published, as a Time: from the +timestamp_in_ms+
      # header if it has one, which a Publisher sets, and otherwise from the
      # +timestamp+ property, which is only good to the second. Nil if the
      # publisher set neither.
      def published_at
        milliseconds = headers["timestamp_in_ms"]
        return Time.at(milliseconds / 1000r) if milliseconds.is_a?(Integer)

        stamp = properties[:timestamp]
        stamp.is_a?(Integer) ? Time.at(stamp) : stamp
      end

      # Seconds since the message was published, as a Float: how long it took
      # to get here, plus however long it has been held. It compares this
      # machine's clock with the publisher's, so it is as good as the two
      # agree, and never less than zero. Nil if the publisher did not say
      # when it published.
      def elapsed
        published = published_at or return nil

        elapsed = Process.clock_gettime(Process::CLOCK_REALTIME) - published.to_r
        elapsed.negative? ? 0.0 : elapsed.to_f
      end

      # True once the message needs nothing further: it was acked, nacked or
      # rejected, or it went stale.
      def settled?
        !@settlement.nil?
      end

      # True when the channel the message arrived on has gone. The broker has
      # requeued it.
      def stale?
        @channel.closed?
      end

      # Acknowledge the message. Returns true if the ack was sent; false if
      # the message was already settled or is stale.
      def ack
        settle(:ack) { P::Basic::Ack.encode(@channel.id, @method.delivery_tag, false) }
      end

      # Negatively acknowledge the message. With +requeue: true+ (the default)
      # the broker puts it back on the queue; with false it is dead-lettered
      # if the queue has a dead-letter exchange and discarded if not.
      def nack(requeue: true)
        settle(:nack) { P::Basic::Nack.encode(@channel.id, @method.delivery_tag, false, requeue) }
      end

      # Reject the message: AMQP's original single-message form of #nack,
      # with the same meaning for +requeue+.
      def reject(requeue: true)
        settle(:reject) { P::Basic::Reject.encode(@channel.id, @method.delivery_tag, requeue) }
      end

      def inspect
        "#<#{self.class.name} delivery_tag=#{delivery_tag} exchange=#{exchange.inspect} " \
          "routing_key=#{routing_key.inspect} bytes=#{@body.bytesize} settlement=#{@settlement.inspect}>"
      end

      private

      # The mark goes down before the frame is written: the write can wait,
      # and a second fiber settling the same message meanwhile must find it
      # taken. If the write does not finish - this fiber was stopped while it
      # waited - nothing was sent, so the mark is lifted and the message can
      # be settled again. That is why it is an ensure and not a rescue: a
      # stop is not a StandardError, and a consumer that is shutting down has
      # to be able to put back what its stopped handlers left behind.
      def settle(how)
        return false if @settlement

        if @channel.closed?
          @settlement = :stale
          return false
        end

        @settlement = how
        finished = false
        begin
          @channel.write(yield.encode)
          finished = true
        rescue Error
          # The channel went under the write: stale after all.
          @settlement = :stale
          return false
        ensure
          @settlement = nil unless finished || @settlement == :stale
        end
        true
      end
    end
  end
end
