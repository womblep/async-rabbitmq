module AsyncRabbitMQ
  # Represents an AMQP exchange bound to a channel.
  # Created via channel.exchange("name", type: :direct, durable: true).
  class Exchange
    attr_reader :name, :type

    def initialize(name, type, channel)
      @name    = name
      @type    = type
      @channel = channel
    end

    def publish(payload, routing_key: "", **opts)
      @channel.basic_publish(payload, exchange: @name, routing_key: routing_key, **opts)
    end

    def delete(if_unused: false)
      @channel.exchange_delete(@name, if_unused: if_unused)
    end

    def bind(destination:, routing_key: "", arguments: {})
      @channel.exchange_bind(destination: destination, source: @name,
                             routing_key: routing_key, arguments: arguments)
      self
    end

    def unbind(destination:, routing_key: "", arguments: {})
      @channel.exchange_unbind(destination: destination, source: @name,
                               routing_key: routing_key, arguments: arguments)
      self
    end
  end
end
