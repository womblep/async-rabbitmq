module AsyncRabbitMQ
  # Represents an AMQP queue bound to a channel.
  # Created via channel.queue("name", durable: true).
  class Queue
    attr_reader :name, :message_count, :consumer_count

    def initialize(name, message_count, consumer_count, channel, durable: false, exclusive: false, auto_delete: false)
      @name           = name
      @message_count  = message_count
      @consumer_count = consumer_count
      @channel        = channel
      @durable        = durable
      @exclusive      = exclusive
      @auto_delete    = auto_delete
    end

    def durable?
      @durable
    end

    def exclusive?
      @exclusive
    end

    def auto_delete?
      @auto_delete
    end

    def server_named?
      @name.start_with?("amq.gen-")
    end

    # Re-declare with passive: true to refresh message_count and consumer_count.
    def status
      q = @channel.queue(@name, passive: true)
      @message_count  = q.message_count
      @consumer_count = q.consumer_count
      { message_count: @message_count, consumer_count: @consumer_count }
    end

    def subscribe(manual_ack: false, **opts, &block)
      @channel.basic_consume(@name, manual_ack: manual_ack, **opts, &block)
    end

    def publish(payload, routing_key: @name, **opts)
      @channel.basic_publish(payload, routing_key: routing_key, **opts)
    end

    def bind(exchange:, routing_key: "", arguments: {})
      @channel.queue_bind(@name, exchange: exchange, routing_key: routing_key, arguments: arguments)
      self
    end

    def unbind(exchange:, routing_key: "", arguments: {})
      @channel.queue_unbind(@name, exchange: exchange, routing_key: routing_key, arguments: arguments)
      self
    end

    def delete(if_unused: false, if_empty: false)
      @channel.queue_delete(@name, if_unused: if_unused, if_empty: if_empty)
    end

    def purge
      @channel.queue_purge(@name)
    end
  end
end
