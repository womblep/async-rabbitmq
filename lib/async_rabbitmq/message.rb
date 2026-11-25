module AsyncRabbitMQ
  # One delivered message, and the means to settle it.
  #
  # It wraps what a consumer block or Queue#pop is handed as three separate
  # arguments - the delivery method, the content header and the body - and
  # remembers the channel they arrived on:
  #
  #   queue.subscribe(manual_ack: true) do |delivery_info, header, body|
  #     message = AsyncRabbitMQ::Message.new(channel, delivery_info, header, body)
  #     process(message.body)
  #     message.ack
  #   end
  #
  # A message is settled at most once. The first of #ack, #nack and #reject
  # wins and the rest return false without writing anything, so a handler that
  # acks and an error path that then nacks cannot draw the 406 PRECONDITION_FAILED
  # ("unknown delivery tag") that closes the channel and takes its consumers
  # with it.
  #
  # A message delivered before a reconnect belongs to a connection that no
  # longer exists. The broker has requeued it and will deliver it again, so
  # settling the old copy is dropped rather than sent (see VersionedDeliveryTag),
  # and #settlement reads :stale.
  class Message
    # Content properties readable as methods. Each returns nil when the
    # publisher did not set it.
    PROPERTIES = %i[
      content_type content_encoding priority correlation_id reply_to expiration
      message_id timestamp type user_id app_id delivery_mode
    ].freeze

    EMPTY_HASH = {}.freeze
    private_constant :EMPTY_HASH

    attr_reader :channel, :delivery_info, :header, :body

    # How this message was settled: :ack, :nack, :reject, :stale (it was
    # delivered on an earlier connection, so there was nothing to settle), or
    # nil while it is still outstanding.
    attr_reader :settlement

    # +delivery_info+ is the Basic::Deliver or Basic::GetOk the message arrived
    # with, and +header+ its content header. Pass +manual_ack: false+ for a
    # message from an automatic-ack consumer or a +pop(manual_ack: false)+: the
    # broker considers it settled already, and acknowledging it would close
    # the channel.
    def initialize(channel, delivery_info, header, body, manual_ack: true)
      @channel       = channel
      @delivery_info = delivery_info
      @header        = header
      @body          = body
      @manual_ack    = manual_ack
      @settlement    = nil
    end

    def delivery_tag
      @delivery_info.delivery_tag
    end

    def exchange
      @delivery_info.exchange
    end

    def routing_key
      @delivery_info.routing_key
    end

    def redelivered?
      @delivery_info.redelivered ? true : false
    end

    # The tag of the consumer this was delivered to; nil for a message fetched
    # with basic.get, which has no consumer.
    def consumer_tag
      @delivery_info.consumer_tag if @delivery_info.respond_to?(:consumer_tag)
    end

    # The content properties as a Hash.
    def properties
      (@header.properties if @header.respond_to?(:properties)) || EMPTY_HASH
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

    # False for a message the broker settled on delivery (automatic ack).
    def manual_ack?
      @manual_ack
    end

    # True once the message needs nothing further: it was acked, nacked or
    # rejected, it went stale, or it never needed acknowledging.
    def settled?
      !@manual_ack || !@settlement.nil?
    end

    # True when the message was delivered on a connection, or a life of its
    # channel, that has since been replaced. The broker requeued it.
    def stale?
      tag = delivery_tag
      tag.is_a?(VersionedDeliveryTag) && tag.stale?(@channel.delivery_generation)
    end

    # Acknowledge the message. Returns true if the ack was sent; false if the
    # message was already settled, went stale, or never needed one.
    def ack
      settle(:ack) { @channel.basic_ack(delivery_tag) }
    end

    # Negatively acknowledge the message. With +requeue: true+ (the default)
    # the broker puts it back on the queue; with false it is dead-lettered if
    # the queue has a dead-letter exchange and discarded if not.
    def nack(requeue: true)
      settle(:nack) { @channel.basic_nack(delivery_tag, requeue: requeue) }
    end

    # Reject the message: AMQP's original single-message form of #nack, with
    # the same meaning for +requeue+.
    def reject(requeue: true)
      settle(:reject) { @channel.basic_reject(delivery_tag, requeue: requeue) }
    end

    # The (delivery_info, header, body) triple a Bunny-style block receives.
    def to_a
      [@delivery_info, @header, @body]
    end
    alias deconstruct to_a

    def inspect
      "#<#{self.class.name} delivery_tag=#{delivery_tag.to_i} exchange=#{exchange.inspect} " \
        "routing_key=#{routing_key.inspect} bytes=#{@body.to_s.bytesize} " \
        "settlement=#{(@manual_ack ? @settlement : :auto).inspect}>"
    end

    private

    # The mark goes down before the frame is written: the write can yield (a
    # full write queue, a channel parked for recovery), and a second fiber
    # settling the same message in that window must find it taken. If the
    # write does not finish, nothing was sent, so the mark is lifted and the
    # message can be settled again. That covers this fiber being stopped while
    # parked as well as an error, which is why it is an ensure and not a
    # rescue: a stop is not a StandardError, and a consumer that is shutting
    # down has to be able to nack what its stopped handlers left behind.
    def settle(how)
      return false if settled?

      @settlement = how
      finished = false
      begin
        sent = yield
        finished = true
      ensure
        @settlement = nil unless finished
      end
      @settlement = :stale unless sent
      sent ? true : false
    end
  end
end
