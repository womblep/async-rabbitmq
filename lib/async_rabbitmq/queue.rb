module AsyncRabbitMQ
  # Represents an AMQP queue bound to a channel.
  # Created via channel.queue("name", durable: true).
  class Queue
    attr_reader :name, :message_count, :consumer_count

    def initialize(name, message_count, consumer_count, channel)
      @name           = name
      @message_count  = message_count
      @consumer_count = consumer_count
      @channel        = channel
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
