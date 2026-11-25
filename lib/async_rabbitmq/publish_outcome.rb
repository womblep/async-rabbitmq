module AsyncRabbitMQ
  # What became of one publish, returned by Channel#publish_confirmed.
  #
  #   outcome = channel.publish_confirmed("hello", routing_key: "orders")
  #   outcome.wait   # => :acked, or raises
  #
  # #wait raises the reason the message did not make it:
  #
  # - MessageNacked: the broker refused it (a full queue with
  #   +x-overflow: reject-publish+, or an internal broker error).
  # - MessageReturned: it was published +mandatory: true+ and no queue was
  #   bound to receive it.
  # - ChannelError, ConnectionError or NotOpenError: the channel went before
  #   the broker answered, so the broker may or may not have the message.
  # - ConfirmTimeoutError: +timeout:+ ran out first. The outcome is still
  #   pending and can be waited on again.
  #
  # Each publish has an outcome of its own, so concurrent publishers on one
  # channel never see each other's failures, which Channel#wait_for_confirms
  # cannot offer: it answers for everything published on the channel since it
  # was last called.
  #
  # A connection loss with automatic recovery does not fail the outcome. The
  # message is published again on the new connection and the outcome goes on
  # waiting for that copy, under the new #delivery_tag.
  class PublishOutcome
    attr_reader :channel_id, :exchange, :routing_key

    # The publisher-confirm sequence number of this publish on its channel. It
    # changes if the message is published again after a reconnect, because the
    # broker restarts the numbering.
    attr_reader :delivery_tag

    # :pending, :acked, :nacked, :returned or :failed.
    attr_reader :status

    # The exception #wait raises; nil while pending and when acked.
    attr_reader :error

    # Internal: the value of the correlation header a mandatory publish
    # carries, which is how a basic.return is matched to its publish.
    attr_reader :publish_id

    def initialize(channel_id:, exchange:, routing_key:, publish_id: nil)
      @channel_id   = channel_id
      @exchange     = exchange
      @routing_key  = routing_key
      @publish_id   = publish_id
      @delivery_tag = nil
      @status       = :pending
      @error        = nil
      @promise      = Async::Promise.new
    end

    # Block the calling fiber until the broker has answered. Returns :acked, or
    # raises as described on the class. +timeout+ is in seconds; nil waits for
    # as long as it takes.
    def wait(timeout: nil)
      # wait? rather than wait: a timeout comes back as nil instead of an
      # Async::TimeoutError, which would be indistinguishable here from one
      # raised into this fiber by a with_timeout the caller wrapped around us.
      if @promise.wait?(timeout: timeout).nil?
        raise ConfirmTimeoutError.new(unconfirmed_tags: [@delivery_tag], channel_id: @channel_id)
      end

      @promise.wait
    end

    def pending?
      @status == :pending
    end

    def acked?
      @status == :acked
    end

    def nacked?
      @status == :nacked
    end

    def returned?
      @status == :returned
    end

    # True when the channel went before the broker answered.
    def failed?
      @status == :failed
    end

    def inspect
      "#<#{self.class.name} #{@status} delivery_tag=#{@delivery_tag.inspect} " \
        "exchange=#{@exchange.inspect} routing_key=#{@routing_key.inspect}>"
    end

    # -------------------------------------------------------------------------
    # Internal: driven by the Channel that issued this outcome. The first
    # resolution wins. That ordering matters for a returned message, which the
    # broker follows with an ordinary ack: the return has to stand.
    # -------------------------------------------------------------------------

    attr_writer :delivery_tag

    def ack!
      return unless pending?

      @status = :acked
      @promise.resolve(:acked)
    end

    def nack!(error)
      finish(:nacked, error)
    end

    def return!(error)
      finish(:returned, error)
    end

    def fail!(error)
      finish(:failed, error)
    end

    private

    def finish(status, error)
      return unless pending?

      @status = status
      @error  = error
      @promise.reject(error)
    end
  end
end
