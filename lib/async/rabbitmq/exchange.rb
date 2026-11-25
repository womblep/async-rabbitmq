# frozen_string_literal: true

module Async
  module RabbitMQ
    # An exchange declared through a Client, or the default exchange.
    #
    #   events = client.exchange("events", :topic)
    #   audit  = client.exchange("audit", :fanout).bind(events, routing_key: "#")
    #
    # Like Queue, a name and the operations on it, with no connection of its own.
    class Exchange
      TYPES = %i[direct fanout topic headers].freeze

      attr :name, :type, :arguments

      def initialize(client, name, type, durable:, auto_delete:, internal:, arguments:)
        @client      = client
        @name        = name
        @type        = type
        @durable     = durable
        @auto_delete = auto_delete
        @internal    = internal
        @arguments   = arguments
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

      # The nameless exchange every broker has, which routes a message to the
      # queue named by its routing key.
      def default?
        @name.empty?
      end

      # Bind this exchange to +source+, so that what is published there and
      # matches reaches here too.
      def bind(source, routing_key: "", arguments: {})
        @client.bind_exchange(@name, exchange_name(source), routing_key, arguments)
        self
      end

      def unbind(source, routing_key: "", arguments: {})
        @client.unbind_exchange(@name, exchange_name(source), routing_key, arguments)
        self
      end

      def exists?
        @client.exchange?(@name)
      end

      # A Publisher to this exchange. See Client#publisher.
      def publisher(**options, &block)
        @client.publisher(@name, **options, &block)
      end

      # Delete the exchange, and have the client forget it.
      def delete(if_unused: false)
        @client.delete_exchange(@name, if_unused: if_unused)
      end

      def to_s
        @name
      end

      def inspect
        "#<#{self.class.name} #{default? ? '(default)' : @name.inspect} #{@type}>"
      end

      private

      def exchange_name(exchange)
        exchange.respond_to?(:name) ? exchange.name : exchange.to_s
      end
    end
  end
end
