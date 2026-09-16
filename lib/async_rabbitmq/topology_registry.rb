module AsyncRabbitMQ
  # Records the exchanges, queues and bindings declared through a Session so
  # they can be re-declared after a reconnect ("topology recovery", as in the
  # RabbitMQ Java client and Bunny 3.0). Passive declares are not recorded,
  # deletes and unbinds remove their records, and a server-named queue that
  # comes back under a new name has its bindings and Queue objects renamed.
  #
  # The registry mirrors what the broker will do, not which channel declared
  # what: a durable queue declared on a channel that was then closed still
  # exists and is still recovered, and so is an exclusive queue, which lives as
  # long as the connection. What a channel closing does remove is the
  # consumers on it, and the broker deletes an auto-delete queue when its last
  # consumer goes, so consumers are recorded too and an auto-delete queue is
  # forgotten with its last one, by cancel, broker cancel or channel close.
  # The bindings go with it, and an auto-delete exchange that just lost its
  # last binding goes as well, again as the broker does. Consumers on other
  # connections are not visible here; if one keeps such a queue alive, all
  # that happens is that recovery does not re-declare a queue that exists.
  #
  # An optional +filter+ object may implement any of filter_exchanges(list),
  # filter_queues(list), filter_queue_bindings(list) and
  # filter_exchange_bindings(list); each receives the recorded entries and
  # returns the subset to recover.
  class TopologyRegistry
    RecordedExchange = Struct.new(:channel_id, :name, :type, :durable, :auto_delete, :internal, :arguments,
                                  keyword_init: true)
    RecordedQueue    = Struct.new(:channel_id, :name, :durable, :exclusive, :auto_delete, :arguments, :server_named,
                                  :objects, keyword_init: true)
    RecordedQueueBinding    = Struct.new(:channel_id, :queue, :exchange, :routing_key, :arguments, keyword_init: true)
    RecordedExchangeBinding = Struct.new(:channel_id, :source, :destination, :routing_key, :arguments, keyword_init: true)

    attr_reader :filter

    def initialize(filter: nil)
      @filter            = filter
      @exchanges         = {}   # name => RecordedExchange
      @queues            = {}   # name => RecordedQueue
      @queue_bindings    = []
      @exchange_bindings = []
      @consumers         = {}   # consumer tag => queue name
    end

    # --- recording -----------------------------------------------------------

    def record_exchange(channel_id, name, type, durable:, auto_delete:, internal:, arguments:)
      return if name.empty? || AsyncRabbitMQ::Exchange::PREDEFINED_EXCHANGES.include?(name)
      @exchanges[name] = RecordedExchange.new(channel_id: channel_id, name: name, type: type.to_s, durable: durable,
                                              auto_delete: auto_delete, internal: internal, arguments: arguments.dup)
    end

    def delete_exchange(name)
      @exchanges.delete(name)
      @queue_bindings.reject! { |b| b.exchange == name }
      gone, @exchange_bindings = @exchange_bindings.partition { |b| b.source == name || b.destination == name }
      # An exchange bound to the deleted one may itself have been auto-delete
      # and kept alive only by that binding.
      gone.each { |b| prune_auto_delete_exchange(b.source) if b.destination == name }
    end

    def record_queue(channel_id, name, durable:, exclusive:, auto_delete:, arguments:, server_named:, object: nil)
      existing = @queues[name]
      rec = RecordedQueue.new(channel_id: channel_id, name: name, durable: durable, exclusive: exclusive,
                              auto_delete: auto_delete, arguments: arguments.dup, server_named: server_named,
                              objects: existing ? existing.objects : [])
      rec.objects << object if object && !rec.objects.include?(object)
      @queues[name] = rec
    end

    def delete_queue(name)
      @queues.delete(name)
      gone, @queue_bindings = @queue_bindings.partition { |b| b.queue == name }
      gone.each { |b| prune_auto_delete_exchange(b.exchange) }
    end

    # A server-named queue was re-declared under a different name.
    def rename_queue(old_name, new_name)
      rec = @queues.delete(old_name) or return
      rec.name = new_name
      @queues[new_name] = rec
      @queue_bindings.each { |b| b.queue = new_name if b.queue == old_name }
      @consumers.transform_values! { |queue| queue == old_name ? new_name : queue }
      rec.objects.each { |q| q.update_name_to(new_name) }
    end

    def record_queue_binding(channel_id, queue:, exchange:, routing_key:, arguments:)
      delete_queue_binding(queue: queue, exchange: exchange, routing_key: routing_key, arguments: arguments, prune: false)
      @queue_bindings << RecordedQueueBinding.new(channel_id: channel_id, queue: queue, exchange: exchange,
                                                  routing_key: routing_key, arguments: arguments.dup)
    end

    def delete_queue_binding(queue:, exchange:, routing_key:, arguments:, prune: true)
      @queue_bindings.reject! do |b|
        b.queue == queue && b.exchange == exchange && b.routing_key == routing_key && b.arguments == arguments
      end
      prune_auto_delete_exchange(exchange) if prune
    end

    def record_exchange_binding(channel_id, source:, destination:, routing_key:, arguments:)
      delete_exchange_binding(source: source, destination: destination, routing_key: routing_key,
                              arguments: arguments, prune: false)
      @exchange_bindings << RecordedExchangeBinding.new(channel_id: channel_id, source: source, destination: destination,
                                                        routing_key: routing_key, arguments: arguments.dup)
    end

    def delete_exchange_binding(source:, destination:, routing_key:, arguments:, prune: true)
      @exchange_bindings.reject! do |b|
        b.source == source && b.destination == destination && b.routing_key == routing_key && b.arguments == arguments
      end
      prune_auto_delete_exchange(source) if prune
    end

    # --- consumers -----------------------------------------------------------

    def record_consumer(consumer_tag, queue_name)
      @consumers[consumer_tag] = queue_name
    end

    # The consumer is gone: cancelled by the client or the broker, or its
    # channel closed. If it was the last one on an auto-delete queue, the
    # broker deletes the queue, so forget the queue and what hung off it.
    def delete_consumer(consumer_tag)
      queue_name = @consumers.delete(consumer_tag) or return
      prune_auto_delete_queue(queue_name)
    end

    # consumer tag => queue name, for inspection.
    def consumers
      @consumers.dup
    end

    # --- what to recover (filtered) -----------------------------------------

    def exchanges
      apply(:filter_exchanges, @exchanges.values)
    end

    def queues
      apply(:filter_queues, @queues.values)
    end

    def queue_bindings
      apply(:filter_queue_bindings, @queue_bindings.dup)
    end

    def exchange_bindings
      apply(:filter_exchange_bindings, @exchange_bindings.dup)
    end

    def empty?
      @exchanges.empty? && @queues.empty? && @queue_bindings.empty? && @exchange_bindings.empty?
    end

    # Forget everything. For a connection whose channels have all been
    # dropped: nothing client-side holds what it declared, so nothing is
    # re-declared when it reconnects.
    def clear
      @exchanges.clear
      @queues.clear
      @queue_bindings.clear
      @exchange_bindings.clear
      @consumers.clear
    end

    private

    def prune_auto_delete_queue(name)
      rec = @queues[name]
      return unless rec&.auto_delete
      return if @consumers.value?(name)

      delete_queue(name)
    end

    def prune_auto_delete_exchange(name)
      rec = @exchanges[name]
      return unless rec&.auto_delete
      return if @queue_bindings.any? { |b| b.exchange == name } || @exchange_bindings.any? { |b| b.source == name }

      delete_exchange(name)
    end

    def apply(filter_method, list)
      @filter.respond_to?(filter_method) ? @filter.public_send(filter_method, list) : list
    end
  end
end
