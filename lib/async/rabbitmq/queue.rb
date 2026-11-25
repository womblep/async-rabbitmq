# frozen_string_literal: true

module Async
  module RabbitMQ
    # A queue declared through a Client.
    #
    #   orders = client.queue("orders")
    #   orders.bind(events, routing_key: "order.*")
    #   orders.message_count
    #
    # It is a name and what was asked for, with the operations that act on the
    # queue. It holds no connection: each operation goes through the client,
    # so it stays usable across reconnects.
    class Queue
      # What the broker reports for a queue: messages ready for delivery, and
      # consumers attached.
      Status = Struct.new(:message_count, :consumer_count)

      attr :name, :arguments

      def initialize(client, name, durable:, auto_delete:, arguments:)
        @client      = client
        @name        = name
        @durable     = durable
        @auto_delete = auto_delete
        @arguments   = arguments
      end

      def durable?
        @durable
      end

      def auto_delete?
        @auto_delete
      end

      # Bind this queue to +exchange+ (an Exchange or its name).
      def bind(exchange, routing_key: "", arguments: {})
        @client.bind_queue(@name, exchange_name(exchange), routing_key, arguments)
        self
      end

      def unbind(exchange, routing_key: "", arguments: {})
        @client.unbind_queue(@name, exchange_name(exchange), routing_key, arguments)
        self
      end

      # Messages ready and consumers attached, as the broker has them now.
      # Raises ChannelError (not_found?) if the queue no longer exists.
      def status
        @client.queue_status(@name)
      end

      def message_count
        status.message_count
      end

      def consumer_count
        status.consumer_count
      end

      def exists?
        @client.queue?(@name)
      end

      # Remove every message that is ready. Returns how many there were.
      def purge
        @client.purge_queue(@name)
      end

      # Delete the queue, and have the client forget it. Returns how many
      # messages went with it.
      def delete(if_unused: false, if_empty: false)
        @client.delete_queue(@name, if_unused: if_unused, if_empty: if_empty)
      end

      def to_s
        @name
      end

      def inspect
        "#<#{self.class.name} #{@name.inspect}#{' durable' if @durable}#{' auto_delete' if @auto_delete}>"
      end

      private

      def exchange_name(exchange)
        exchange.respond_to?(:name) ? exchange.name : exchange.to_s
      end
    end
  end
end
