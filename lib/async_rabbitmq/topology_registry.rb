module AsyncRabbitMQ
  # Records the exchanges, queues and bindings declared through a Session so
  # they can be re-declared after a reconnect ("topology recovery", as in the
  # RabbitMQ Java client and Bunny 3.0). Passive declares are not recorded,
  # deletes and unbinds remove their records, and a server-named queue that
  # comes back under a new name has its bindings and Queue objects renamed.
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
    end

    # --- recording -----------------------------------------------------------

    def record_exchange(channel_id, name, type, durable:, auto_delete:, internal:, arguments:)
      return if name.empty? || AsyncRabbitMQ::Exchange::PREDEFINED_EXCHANGES.include?(name)
      @exchanges[name] = RecordedExchange.new(channel_id: channel_id, name: name, type: type.to_s, durable: durable,
                                              auto_delete: auto_delete, internal: internal, arguments: arguments.dup)
    end

    def delete_exchange(name)
      @exchanges.delete(name)
      @queue_bindings.reject!    { |b| b.exchange == name }
      @exchange_bindings.reject! { |b| b.source == name || b.destination == name }
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
      @queue_bindings.reject! { |b| b.queue == name }
    end

    # A server-named queue was re-declared under a different name.
    def rename_queue(old_name, new_name)
      rec = @queues.delete(old_name) or return
      rec.name = new_name
      @queues[new_name] = rec
      @queue_bindings.each { |b| b.queue = new_name if b.queue == old_name }
      rec.objects.each { |q| q.update_name_to(new_name) }
    end

    def record_queue_binding(channel_id, queue:, exchange:, routing_key:, arguments:)
      delete_queue_binding(queue: queue, exchange: exchange, routing_key: routing_key, arguments: arguments)
      @queue_bindings << RecordedQueueBinding.new(channel_id: channel_id, queue: queue, exchange: exchange,
                                                  routing_key: routing_key, arguments: arguments.dup)
    end

    def delete_queue_binding(queue:, exchange:, routing_key:, arguments:)
      @queue_bindings.reject! do |b|
        b.queue == queue && b.exchange == exchange && b.routing_key == routing_key && b.arguments == arguments
      end
    end

    def record_exchange_binding(channel_id, source:, destination:, routing_key:, arguments:)
      delete_exchange_binding(source: source, destination: destination, routing_key: routing_key, arguments: arguments)
      @exchange_bindings << RecordedExchangeBinding.new(channel_id: channel_id, source: source, destination: destination,
                                                        routing_key: routing_key, arguments: arguments.dup)
    end

    def delete_exchange_binding(source:, destination:, routing_key:, arguments:)
      @exchange_bindings.reject! do |b|
        b.source == source && b.destination == destination && b.routing_key == routing_key && b.arguments == arguments
      end
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

    private

    def apply(filter_method, list)
      @filter.respond_to?(filter_method) ? @filter.public_send(filter_method, list) : list
    end
  end
end
