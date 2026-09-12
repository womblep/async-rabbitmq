module AsyncRabbitMQ
  # Represents an AMQP exchange bound to a channel.
  # Created via channel.exchange("name", type: :direct, durable: true).
  class Exchange
    attr_reader :name, :type

    # Built-in and commonly used exchange types (Bunny parity).
    TYPE_DIRECT          = "direct"
    TYPE_FANOUT          = "fanout"
    TYPE_TOPIC           = "topic"
    TYPE_HEADERS         = "headers"
    TYPE_CONSISTENT_HASH = "x-consistent-hash"
    TYPE_MODULUS_HASH    = "x-modulus-hash"
    TYPE_RANDOM          = "x-random"
    TYPE_LOCAL_RANDOM    = "x-local-random"

    PREDEFINED_EXCHANGES = %w[
      amq.direct amq.fanout amq.topic amq.headers amq.match amq.rabbitmq.trace
    ].freeze

    def initialize(name, type, channel, durable: false, auto_delete: false, internal: false)
      @name        = name
      @type        = type
      @channel     = channel
      @durable     = durable
      @auto_delete = auto_delete
      @internal    = internal
    end

    def durable?
      @durable
    end

    def auto_delete?
      @auto_delete
    end

    def internal?
      @internal
    end

    def predefined?
      @name == "" || PREDEFINED_EXCHANGES.include?(@name)
    end

    def on_return(&block)
      @channel.on_return(&block)
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
