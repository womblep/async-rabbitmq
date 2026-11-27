# frozen_string_literal: true

module Async
  module RabbitMQ
    # What a Client has been asked to declare, kept so that it can be declared
    # again on a new connection. A reconnect may land on a broker that has
    # forgotten things - one that restarted and lost what was not durable, or
    # another node that never had it - and what the application asked for
    # should be there regardless.
    #
    # Only what was declared through the client is here. A queue a Consumer
    # declares for itself (an exclusive one) belongs to that consumer, which
    # declares it again when it re-attaches.
    #
    # Deleting something through the client forgets it. Something deleted
    # behind the client's back is not known to be gone, and comes back on the
    # next reconnect; Client#forget is for the application that knows.
    #
    # A queue may be held: left out of what is declared again, without being
    # forgotten. A consumer the broker has cancelled holds its queue until it
    # knows whether the queue is still there, so that a connection lost in
    # the meantime does not put back a queue somebody has just deleted.
    class Topology
      ExchangeRecord = Struct.new(:name, :type, :durable, :auto_delete, :internal, :arguments)
      QueueRecord    = Struct.new(:name, :durable, :auto_delete, :arguments)
      BindingRecord  = Struct.new(:destination, :source, :routing_key, :arguments)

      def initialize
        @exchanges         = {}
        @queues            = {}
        @queue_bindings    = []
        @exchange_bindings = []
        @held              = Hash.new(0)
      end

      # Counted: a queue with two consumers recovering is held until both
      # have let go.
      def hold_queue(name)
        @held[name] += 1
      end

      def release_queue(name)
        @held.delete(name) if (@held[name] -= 1) <= 0
      end

      def held?(name) = @held.key?(name)

      def exchanges         = @exchanges.values
      def queues            = @queues.values
      def queue_bindings    = @queue_bindings.dup
      def exchange_bindings = @exchange_bindings.dup

      def empty?
        @exchanges.empty? && @queues.empty? && @queue_bindings.empty? && @exchange_bindings.empty?
      end

      def record_exchange(name, type, durable:, auto_delete:, internal:, arguments:)
        @exchanges[name] = ExchangeRecord.new(name, type, durable, auto_delete, internal, arguments)
      end

      def record_queue(name, durable:, auto_delete:, arguments:)
        @queues[name] = QueueRecord.new(name, durable, auto_delete, arguments)
      end

      def record_queue_binding(queue, exchange, routing_key, arguments)
        record = BindingRecord.new(queue, exchange, routing_key, arguments)
        @queue_bindings << record unless @queue_bindings.include?(record)
      end

      def record_exchange_binding(destination, source, routing_key, arguments)
        record = BindingRecord.new(destination, source, routing_key, arguments)
        @exchange_bindings << record unless @exchange_bindings.include?(record)
      end

      def delete_queue_binding(queue, exchange, routing_key, arguments)
        @queue_bindings.delete(BindingRecord.new(queue, exchange, routing_key, arguments))
      end

      def delete_exchange_binding(destination, source, routing_key, arguments)
        @exchange_bindings.delete(BindingRecord.new(destination, source, routing_key, arguments))
      end

      # Forget a queue and every binding to it.
      def delete_queue(name)
        @queues.delete(name)
        @queue_bindings.reject! { |binding| binding.destination == name }
      end

      # Forget an exchange and every binding that names it, at either end.
      def delete_exchange(name)
        @exchanges.delete(name)
        @queue_bindings.reject! { |binding| binding.source == name }
        @exchange_bindings.reject! { |binding| binding.source == name || binding.destination == name }
      end
    end
  end
end
