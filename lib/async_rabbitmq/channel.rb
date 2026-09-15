require "async"
require "async/condition"
require "async/semaphore"
require_relative "errors"

module AsyncRabbitMQ
  # Represents an AMQP channel multiplexed over a Session's connection.
  #
  # Duck-typed stream interface: #each (yields deliveries) and #write (publishes).
  # Does NOT inherit Async::IO::Stream — a channel is logical, not physical IO.
  class Channel
    include Instrumented

    attr_reader :channel_id, :pool_size

    # +pool_size+ bounds the number of consumer-handler fibers that can run
    # concurrently on this channel (Bunny-parity: default 1 = serialized).
    # Resizable at runtime via #pool_size=; basic_qos adjusts it automatically
    # when prefetch_count > 0 so the two stay coupled.
    def initialize(channel_id, session, frame_io, frame_max:, logger:, pool_size: 1, rpc_timeout: nil)
      @channel_id   = channel_id
      @session      = session
      @notifier     = session.respond_to?(:notifier) ? session.notifier : nil
      @frame_io     = frame_io
      @frame_max    = frame_max
      @logger       = logger
      @rpc_timeout  = rpc_timeout   # seconds a synchronous operation waits for its reply; nil = forever

      @state             = :closed
      @queue             = nil
      @consumers         = {}        # consumer_tag => {queue_name:, block:, manual_ack:}
      @each_waiters      = {}        # consumer_tag => Async::Condition, fibers blocked in #each
      @return_handler    = nil
      @delivery_tag      = 0
      @pending_confirms  = {}        # delivery_tag => delivery_tag, awaiting basic.ack/nack
      @nacked_tags       = []        # tags the broker rejected since confirm_select (Bunny: nacked_set)
      @only_acks         = true      # false once a nack arrives; read and reset by wait_for_confirms
      @confirms_enabled  = false
      @tracking          = false     # confirm_select(tracking: true): backpressure + MessageNacked
      @outstanding_limit = nil       # max unconfirmed messages before basic_publish parks
      @slot_condition    = nil       # publishers waiting for an outstanding slot
      @nacked_this_cycle = []        # nacks since the last wait_for_confirms, for MessageNacked
      @tx_mode           = false
      @prefetch          = nil       # last basic_qos settings, restored on reopen
      @confirm_condition = nil
      @flow_active       = true
      @on_cancel         = nil
      @on_error          = nil
      @mutex             = Async::Semaphore.new(1)
      @publish_sem       = Async::Semaphore.new(1)   # one publish's frames go out contiguously
      @rpc_sem           = Async::Semaphore.new(1)   # one request/reply in flight per channel
      @reply_condition   = nil
      @content_condition = nil
      @pending_content   = nil
      @recovered_condition = nil     # fibers parked while the connection recovers
      @pool_size         = validate_pool_size!(pool_size)
      @pool_sem          = Async::Semaphore.new(@pool_size)
    end

    # Resize the consumer-handler concurrency cap. Shrinking does not evict
    # already-running handlers; new deliveries park until permits free up.
    def pool_size=(n)
      @pool_size      = validate_pool_size!(n)
      @pool_sem.limit = @pool_size
    end

    def open?
      @state == :open
    end

    def closed?
      @state == :closed
    end

    # True while the session is reconnecting; operations park until reopened.
    def recovering?
      @state == :recovering
    end

    def open
      @queue = @frame_io.register_channel(@channel_id)
      # Start dispatch task BEFORE waiting so it can process the OpenOk reply.
      start_dispatch_task
      rpc(AMQ::Protocol::Channel::Open.encode(@channel_id, ""), AMQ::Protocol::Channel::OpenOk, check_open: false)
      @state = :open
      instrument("channel.open") { { channel: @channel_id } }
      self
    end

    def close
      if recovering?
        # Give the channel up rather than letting recovery reopen it.
        mark_closed!(NotOpenError.new("Channel #{@channel_id} closed during recovery"))
        forget_consumers_in_topology
        @session.channel_closed(@channel_id)
        return
      end
      return unless open?
      rpc(AMQ::Protocol::Channel::Close.encode(@channel_id, 200, "Goodbye", 0, 0), AMQ::Protocol::Channel::CloseOk)
      @state = :closed
      # Closing the channel cancelled its consumers on the broker side; an
      # auto-delete queue that just lost its last one is gone with it.
      forget_consumers_in_topology
      instrument("channel.closed") { { channel: @channel_id, reason: :user } }
      @session.channel_closed(@channel_id)
      @queue&.push(nil)  # wake dispatch_loop so it can detect :closed and exit
      wake_each_waiters
    end

    # -------------------------------------------------------------------------
    # Queue
    # -------------------------------------------------------------------------

    # Pass an empty string as +name+ to let the broker generate a unique name
    # (returned in the Queue object). AMQP 0-9-1 spec §3.1.2.
    def queue(name, passive: false, durable: false, exclusive: false, auto_delete: false, arguments: {})
      resp = rpc(
        AMQ::Protocol::Queue::Declare.encode(@channel_id, name, passive, durable, exclusive, auto_delete, false, arguments),
        AMQ::Protocol::Queue::DeclareOk
      )
      q = Queue.new(resp.queue, resp.message_count, resp.consumer_count, self,
                    durable: durable, exclusive: exclusive, auto_delete: auto_delete)
      unless passive
        topology&.record_queue(@channel_id, resp.queue, durable: durable, exclusive: exclusive, auto_delete: auto_delete,
                                            arguments: arguments, server_named: name.to_s.empty?, object: q)
      end
      q
    end

    # Server-named queue that lives for this connection only (exclusive, auto-delete).
    def temporary_queue(**opts)
      queue("", exclusive: true, auto_delete: true, **opts)
    end

    # Durable, non-exclusive, non-auto-delete queue of the given type
    # (Queue::Types::CLASSIC, QUORUM or STREAM). A name is required: a
    # server-named durable queue makes no sense. Durability, exclusivity and
    # auto-delete are fixed; +passive+ and +arguments+ are honoured.
    def durable_queue(name, type = Queue::Types::CLASSIC, passive: false, arguments: {})
      if name.nil? || name.to_s.empty?
        raise ArgumentError, "queue name must not be nil or empty (server-named durable queues make no sense)"
      end
      args = type.to_s == Queue::Types::CLASSIC ? arguments : { "x-queue-type" => type.to_s }.merge(arguments)
      queue(name, passive: passive, durable: true, exclusive: false, auto_delete: false, arguments: args)
    end

    def quorum_queue(name, **opts)
      durable_queue(name, Queue::Types::QUORUM, **opts)
    end

    # A RabbitMQ stream, usable over AMQP 0-9-1 as a durable queue. Consuming
    # from it needs basic_qos and an "x-stream-offset" consumer argument.
    def stream(name, **opts)
      durable_queue(name, Queue::Types::STREAM, **opts)
    end

    def queue_delete(name, if_unused: false, if_empty: false)
      rpc(AMQ::Protocol::Queue::Delete.encode(@channel_id, name, if_unused, if_empty, false), AMQ::Protocol::Queue::DeleteOk)
        .tap { topology&.delete_queue(name) }
    end

    def queue_purge(name)
      rpc(AMQ::Protocol::Queue::Purge.encode(@channel_id, name, false), AMQ::Protocol::Queue::PurgeOk)
    end

    def queue_bind(queue_name, exchange:, routing_key: "", arguments: {})
      rpc(
        AMQ::Protocol::Queue::Bind.encode(@channel_id, queue_name, exchange, routing_key, false, arguments),
        AMQ::Protocol::Queue::BindOk
      ).tap do
        topology&.record_queue_binding(@channel_id, queue: queue_name, exchange: exchange,
                                                    routing_key: routing_key, arguments: arguments)
      end
    end

    def queue_unbind(queue_name, exchange:, routing_key: "", arguments: {})
      rpc(
        AMQ::Protocol::Queue::Unbind.encode(@channel_id, queue_name, exchange, routing_key, arguments),
        AMQ::Protocol::Queue::UnbindOk
      ).tap do
        topology&.delete_queue_binding(queue: queue_name, exchange: exchange, routing_key: routing_key, arguments: arguments)
      end
    end

    # -------------------------------------------------------------------------
    # Exchange
    # -------------------------------------------------------------------------

    def exchange(name, type: :direct, passive: false, durable: false, auto_delete: false, internal: false, arguments: {})
      rpc(
        AMQ::Protocol::Exchange::Declare.encode(@channel_id, name, type.to_s, passive, durable, auto_delete, internal, false, arguments),
        AMQ::Protocol::Exchange::DeclareOk
      )
      unless passive
        topology&.record_exchange(@channel_id, name, type, durable: durable, auto_delete: auto_delete,
                                                     internal: internal, arguments: arguments)
      end
      Exchange.new(name, type, self, durable: durable, auto_delete: auto_delete, internal: internal)
    end

    def direct(name, **opts)
      exchange(name, type: :direct, **opts)
    end

    def fanout(name, **opts)
      exchange(name, type: :fanout, **opts)
    end

    def topic(name, **opts)
      exchange(name, type: :topic, **opts)
    end

    def headers(name, **opts)
      exchange(name, type: :headers, **opts)
    end

    def default_exchange
      Exchange.new("", :direct, self, durable: true, auto_delete: false, internal: false)
    end

    def exchange_delete(name, if_unused: false)
      rpc(AMQ::Protocol::Exchange::Delete.encode(@channel_id, name, if_unused, false), AMQ::Protocol::Exchange::DeleteOk)
        .tap { topology&.delete_exchange(name) }
    end

    def exchange_bind(destination:, source:, routing_key: "", arguments: {})
      rpc(
        AMQ::Protocol::Exchange::Bind.encode(@channel_id, destination, source, routing_key, false, arguments),
        AMQ::Protocol::Exchange::BindOk
      ).tap do
        topology&.record_exchange_binding(@channel_id, source: source, destination: destination,
                                                       routing_key: routing_key, arguments: arguments)
      end
    end

    def exchange_unbind(destination:, source:, routing_key: "", arguments: {})
      rpc(
        AMQ::Protocol::Exchange::Unbind.encode(@channel_id, destination, source, routing_key, false, arguments),
        AMQ::Protocol::Exchange::UnbindOk
      ).tap do
        topology&.delete_exchange_binding(source: source, destination: destination, routing_key: routing_key, arguments: arguments)
      end
    end

    # -------------------------------------------------------------------------
    # Basic operations
    # -------------------------------------------------------------------------

    # Publish one message. Options: exchange:, routing_key:, mandatory:,
    # persistent: (delivery_mode 2) and the standard AMQP properties
    # (content_type:, content_encoding:, headers:, priority:, correlation_id:,
    # reply_to:, expiration:, message_id:, timestamp:, type:, user_id:, app_id:)
    # plus a raw properties: hash. Returns the confirm delivery tag when the
    # channel is in confirm mode, nil otherwise.
    def basic_publish(payload, exchange: "", routing_key: "", **opts)
      bytes = encode_publish(payload, exchange: exchange, routing_key: routing_key, **opts)
      # One publish's frames must reach the wire contiguously and confirm tags
      # must follow wire order, so the write and the tag assignment happen
      # under one semaphore (write_frame can yield on a full queue or the
      # connection.blocked gate).
      tag = @publish_sem.acquire do
        assert_open!
        wait_for_outstanding_slot(1)
        @frame_io.write_frame(bytes, publish: true)
        reserve_confirm_tag(bytes) if @confirms_enabled
      end
      instrument("message.published") do
        { channel: @channel_id, exchange: exchange, routing_key: routing_key, count: 1,
          bytes: bytes.bytesize, delivery_tag: tag }
      end
      tag
    end

    # Duck-typed #write for stream composability.
    alias write basic_publish

    # Publish many messages with one write. All payloads share +opts+ (see
    # basic_publish). The frames are encoded into a single buffer and handed
    # to the writer once; under confirms the tag range is reserved as a block
    # and the tags are returned (nil otherwise). Batches of a few hundred to a
    # few thousand messages give the best throughput (Bunny 3.0 parity).
    def basic_publish_batch(payloads, exchange: "", routing_key: "", **opts)
      raise ArgumentError, "payloads must be an Array of message bodies" unless payloads.is_a?(Array)
      return nil if payloads.empty?

      encoded = payloads.map { |p| encode_publish(p, exchange: exchange, routing_key: routing_key, **opts) }
      tags = @publish_sem.acquire do
        assert_open!
        wait_for_outstanding_slot(encoded.size)
        @frame_io.write_frame(encoded.join, publish: true)
        encoded.map { |bytes| reserve_confirm_tag(bytes) } if @confirms_enabled
      end
      instrument("message.published") do
        { channel: @channel_id, exchange: exchange, routing_key: routing_key, count: encoded.size,
          bytes: encoded.sum(&:bytesize), delivery_tag: tags&.last }
      end
      tags
    end

    # Synchronously fetch one message: [delivery_info, header, body], or nil if
    # the queue is empty. Defaults to manual acknowledgement (as Bunny does):
    # a message fetched and then dropped by the caller is requeued, not lost.
    # Pass manual_ack: false to have the broker discard it on delivery.
    def basic_get(queue_name, manual_ack: true)
      msg, content = @rpc_sem.acquire do
        assert_open!
        @frame_io.write_frame(AMQ::Protocol::Basic::Get.encode(@channel_id, queue_name, !manual_ack).encode)
        m = wait_for_any(AMQ::Protocol::Basic::GetOk, AMQ::Protocol::Basic::GetEmpty)
        # After GetOk the content header + body follow on this channel.
        [m, m.is_a?(AMQ::Protocol::Basic::GetOk) ? wait_content : nil]
      end
      return nil if content.nil?

      [msg, content[:header], content[:body]]
    end

    def basic_ack(delivery_tag, multiple: false)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Ack.encode(@channel_id, delivery_tag, multiple).encode
      )
    end

    def basic_nack(delivery_tag, multiple: false, requeue: true)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Nack.encode(@channel_id, delivery_tag, multiple, requeue).encode
      )
    end

    def basic_reject(delivery_tag, requeue: true)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Reject.encode(@channel_id, delivery_tag, requeue).encode
      )
    end

    def basic_qos(prefetch_count:, prefetch_size: 0, global: false)
      rpc(AMQ::Protocol::Basic::Qos.encode(@channel_id, prefetch_size, prefetch_count, global), AMQ::Protocol::Basic::QosOk)
      @prefetch = { count: prefetch_count, size: prefetch_size, global: global }   # restored on reopen
      # Keep the handler pool coupled to prefetch: no point buffering 10 unacked
      # messages at the broker if only 1 can run at a time. prefetch_count == 0
      # means "unlimited" in AMQP; leave the pool alone so the user can still cap it.
      self.pool_size = prefetch_count if prefetch_count > 0
    end

    # Start a consumer. The block runs in a new Async::Task per delivery,
    # gated by the channel's pool_size semaphore so at most +pool_size+
    # handlers run concurrently across all consumers on this channel.
    # Returns the consumer tag.
    def basic_consume(queue_name, consumer_tag: "", manual_ack: false, exclusive: false, arguments: {}, &block)
      resp = rpc(
        AMQ::Protocol::Basic::Consume.encode(@channel_id, queue_name, consumer_tag, false, !manual_ack, exclusive, false, arguments),
        AMQ::Protocol::Basic::ConsumeOk
      )
      @consumers[resp.consumer_tag] = { queue_name: queue_name, block: block, manual_ack: manual_ack }
      topology&.record_consumer(resp.consumer_tag, queue_name)
      instrument("consumer.registered") do
        { channel: @channel_id, queue: queue_name, consumer_tag: resp.consumer_tag, manual_ack: manual_ack }
      end
      resp.consumer_tag
    end

    def basic_cancel(consumer_tag)
      rpc(AMQ::Protocol::Basic::Cancel.encode(@channel_id, consumer_tag, false), AMQ::Protocol::Basic::CancelOk)
      cancelled = @consumers.delete(consumer_tag)
      topology&.delete_consumer(consumer_tag)
      instrument("consumer.cancelled") do
        { channel: @channel_id, consumer_tag: consumer_tag, queue: cancelled&.dig(:queue_name), reason: :client }
      end
      wake_each_waiters(consumer_tag)
    end

    # Ask the broker to redeliver all unacknowledged messages on this channel.
    # RabbitMQ only supports requeue: true; requeue: false raises a channel error.
    def basic_recover(requeue: true)
      rpc(AMQ::Protocol::Basic::Recover.encode(@channel_id, requeue), AMQ::Protocol::Basic::RecoverOk)
    end

    # -------------------------------------------------------------------------
    # Publisher confirms
    # -------------------------------------------------------------------------

    DEFAULT_OUTSTANDING_LIMIT = 1000

    # Enable publisher confirms. With +tracking: true+ (Bunny 3.0 parity):
    # basic_publish parks while +outstanding_limit+ messages are unconfirmed
    # (default 1000, the sweet spot in Bunny's benchmarks), giving natural
    # backpressure, and wait_for_confirms raises MessageNacked instead of
    # returning false when the broker rejected a message.
    def confirm_select(tracking: false, outstanding_limit: nil)
      raise ArgumentError, "outstanding_limit requires tracking: true" if outstanding_limit && !tracking
      if outstanding_limit && !(outstanding_limit.is_a?(Integer) && outstanding_limit.positive?)
        raise ArgumentError, "outstanding_limit must be a positive Integer (got #{outstanding_limit.inspect})"
      end

      rpc(AMQ::Protocol::Confirm::Select.encode(@channel_id, false), AMQ::Protocol::Confirm::SelectOk)
      @confirms_enabled  = true
      @tracking          = tracking
      @outstanding_limit = tracking ? (outstanding_limit || DEFAULT_OUTSTANDING_LIMIT) : nil
      @delivery_tag      = 0
      @pending_confirms  = {}
      @nacked_tags       = []
      @nacked_this_cycle = []
      @only_acks         = true
      @confirm_condition = Async::Condition.new
    end

    def tracking_confirms?
      @tracking
    end

    attr_reader :outstanding_limit

    # Block the current fiber until every published message has been acked or
    # nacked by the broker. Returns true if all of them were acked since the
    # previous call, false if at least one was nacked (see #nacked_tags).
    # Raises ConnectionError if the session disconnects while waiting.
    def wait_for_confirms
      until @pending_confirms.empty?
        # Lazily (re)created: interrupt_wait! clears it on connection loss and a
        # caller may arrive before reopen_after_recovery has re-selected confirms.
        @confirm_condition ||= Async::Condition.new
        @confirm_condition.wait
        raise ConnectionError, "Session disconnected while waiting for confirms" unless open?
      end
      result       = @only_acks
      @only_acks   = true
      cycle_nacked = @nacked_this_cycle
      @nacked_this_cycle = []
      raise MessageNacked.new(nacked_tags: cycle_nacked, channel_id: @channel_id) if @tracking && !result
      result
    end

    # Delivery tags the broker rejected with basic.nack since confirm_select.
    def nacked_tags
      @nacked_tags.dup
    end

    # Delivery tags published but not yet acked or nacked.
    def unconfirmed_tags
      @pending_confirms.keys
    end

    # -------------------------------------------------------------------------
    # Transactions (tx.*): publishes and acks on this channel are held by the
    # broker until tx_commit, or discarded by tx_rollback. A channel cannot be
    # both transactional and in confirm mode (the broker rejects the switch).
    # Transactional mode is restored after connection recovery; work that was
    # uncommitted when the connection dropped is lost, as with any client.
    # -------------------------------------------------------------------------

    def tx_select
      rpc(AMQ::Protocol::Tx::Select.encode(@channel_id), AMQ::Protocol::Tx::SelectOk)
      @tx_mode = true
    end

    def tx_commit
      rpc(AMQ::Protocol::Tx::Commit.encode(@channel_id), AMQ::Protocol::Tx::CommitOk)
    end

    def tx_rollback
      rpc(AMQ::Protocol::Tx::Rollback.encode(@channel_id), AMQ::Protocol::Tx::RollbackOk)
    end

    def using_tx?
      @tx_mode
    end

    # -------------------------------------------------------------------------
    # Return handler
    # -------------------------------------------------------------------------

    def on_return(&block)
      @return_handler = block
    end

    attr_reader :return_handler

    # Register a callback invoked when the broker cancels a consumer server-side
    # (e.g. the queue is deleted or an HA failover occurs).
    # The block receives the consumer_tag that was cancelled.
    def on_cancel(&block)
      @on_cancel = block
    end

    # Register a callback invoked when the broker closes this channel due to an
    # error (e.g. 404 queue not found). The block receives the channel and the
    # AMQ::Protocol::Channel::Close method frame.
    def on_error(&block)
      @on_error = block
    end

    # -------------------------------------------------------------------------
    # Flow control
    # -------------------------------------------------------------------------

    def flow(active)
      rpc(AMQ::Protocol::Channel::Flow.encode(@channel_id, active), AMQ::Protocol::Channel::FlowOk)
      @flow_active = active
    end

    # -------------------------------------------------------------------------
    # Duck-typed stream: #each yields deliveries
    # -------------------------------------------------------------------------

    # Consume from +queue_name+, running +block+ for every delivery, and block
    # the calling fiber until the consumer goes away: the channel is closed,
    # the broker cancels the consumer (e.g. the queue is deleted) or closes the
    # channel. A connection loss with automatic recovery is transparent: the
    # consumer is re-registered and #each keeps waiting.
    def each(queue_name, manual_ack: false, &block)
      assert_open!
      tag  = basic_consume(queue_name, manual_ack: manual_ack, &block)
      cond = @each_waiters[tag] = Async::Condition.new
      cond.wait
      nil
    ensure
      if tag
        @each_waiters.delete(tag)
        # Cancelled early (e.g. the calling task was stopped): tidy the consumer.
        basic_cancel(tag) rescue nil if open? && @consumers.key?(tag)
      end
    end

    # -------------------------------------------------------------------------
    # Internal: connection state transitions driven by Session
    # -------------------------------------------------------------------------

    # The connection was lost and recovery is starting. Waits in flight fail
    # with +error+; new publishes and RPCs park until #reopen_after_recovery
    # (see #assert_open!); consumers are re-registered on reopen.
    def mark_recovering!(error)
      @state = :recovering
      interrupt_wait!(error)
    end

    # The connection is gone for good: recovery disabled, exhausted, refused,
    # or the session was closed. Everything waiting or parked raises +error+
    # and #each callers return.
    def mark_closed!(error)
      @state = :closed
      interrupt_wait!(error)
      resume_parked!(error)
      wake_each_waiters
    end

    # Unblock any fibers waiting on reply, content or confirm conditions
    # (used on connection loss, and again if the connection dies mid-recovery).
    def interrupt_wait!(error = nil)
      error ||= ConnectionError.new(code: 0, text: "Connection lost during recovery")
      @reply_condition&.signal(error)
      @reply_condition   = nil
      @content_condition&.signal(error)
      @content_condition = nil
      @confirm_condition&.signal(error)
      @confirm_condition = nil
      release_outstanding_slots(error)
      @queue&.push(nil) rescue nil
    rescue => e
      # ignore — best-effort unblock
    end

    # -------------------------------------------------------------------------
    # Internal: called by Session after reconnect
    # -------------------------------------------------------------------------

    # Internal, after Session has reopened every channel and replayed the
    # topology: release parked publishers/RPCs first (they may hold the
    # semaphores basic_consume needs), then re-register the consumers.
    def finish_recovery!
      @state = :open
      # After the session has replayed the topology, so a message addressed to a
      # queue or exchange the broker lost is not sent into the void before it is
      # re-declared. Before the parked callers, so the older messages go first.
      republish_unconfirmed if @confirms_enabled
      resume_parked!(:open)
      re_register_consumers
    end

    # Internal, topology recovery. A re-declare the broker rejects closes the
    # channel; the failure is logged and the channel reopened so the remaining
    # entities can still be recovered (Bunny 3.2 behaviour).
    def recover_exchange(x)
      recover_entity("exchange #{x.name}") do
        send_and_wait(AMQ::Protocol::Exchange::Declare.encode(@channel_id, x.name, x.type, false, x.durable,
                                                              x.auto_delete, x.internal, false, x.arguments),
                      AMQ::Protocol::Exchange::DeclareOk)
      end
    end

    def recover_queue(q)
      recover_entity("queue #{q.name}") do
        ok = send_and_wait(AMQ::Protocol::Queue::Declare.encode(@channel_id, q.server_named ? "" : q.name, false,
                                                                q.durable, q.exclusive, q.auto_delete, false, q.arguments),
                           AMQ::Protocol::Queue::DeclareOk)
        @session.queue_renamed(q.name, ok.queue) if ok.queue != q.name
      end
    end

    def recover_queue_binding(b)
      recover_entity("binding #{b.exchange} -> #{b.queue}") do
        send_and_wait(AMQ::Protocol::Queue::Bind.encode(@channel_id, b.queue, b.exchange, b.routing_key, false, b.arguments),
                      AMQ::Protocol::Queue::BindOk)
      end
    end

    def recover_exchange_binding(b)
      recover_entity("exchange binding #{b.source} -> #{b.destination}") do
        send_and_wait(AMQ::Protocol::Exchange::Bind.encode(@channel_id, b.destination, b.source, b.routing_key, false, b.arguments),
                      AMQ::Protocol::Exchange::BindOk)
      end
    end

    # Internal: a server-named queue this channel consumes from came back under
    # a new name after topology recovery.
    def rename_consumer_queue(old_name, new_name)
      @consumers.each_value { |c| c[:queue_name] = new_name if c[:queue_name] == old_name }
    end

    # The consumers on this channel no longer exist on the broker (the channel
    # is closed, by us or by it). @consumers itself is kept so that
    # reopen(recover_consumers: true) can register them again, which records
    # them again.
    def forget_consumers_in_topology
      return unless (registry = topology)

      @consumers.each_key { |tag| registry.delete_consumer(tag) }
    end

    # Reopen a channel the broker closed (e.g. delivery-ack timeout, unknown
    # delivery tag) on the same connection, keeping its id. Prefetch, confirm
    # mode and transactional mode are restored, and messages still unconfirmed
    # at the time of the close are re-published. The old consumers are dropped
    # unless +recover_consumers+ is true. (Bunny 3.0 parity.)
    def reopen(recover_consumers: false)
      unless closed?
        raise NotOpenError, "Channel #{@channel_id} is #{@state}; only a closed channel can be reopened"
      end
      @consumers.clear unless recover_consumers
      @session.reopen_channel(self)   # re-registers the id, then calls reopen_on
      # Only this channel was closed, so the topology is intact and anything
      # left unconfirmed can go straight back out.
      republish_unconfirmed if @confirms_enabled
      re_register_consumers if recover_consumers
      self
    end

    # Internal: (re)open this channel on +frame_io+ and restore its settings.
    # Uses direct sends: nothing else can be on the wire for this channel yet,
    # and a fiber parked inside #rpc may be holding @rpc_sem. During connection
    # recovery the session passes state: :recovering so callers stay parked
    # until the topology has been replayed (see #finish_recovery!).
    def reopen_on(frame_io, state: :open)
      @frame_io = frame_io
      @queue    = @frame_io.register_channel(@channel_id)
      start_dispatch_task
      send_and_wait(AMQ::Protocol::Channel::Open.encode(@channel_id, ""), AMQ::Protocol::Channel::OpenOk)
      @state = state

      if (p = @prefetch)
        send_and_wait(AMQ::Protocol::Basic::Qos.encode(@channel_id, p[:size], p[:count], p[:global]), AMQ::Protocol::Basic::QosOk)
      end
      # Confirm mode is restored here, but the unconfirmed messages are NOT sent
      # yet: the topology they are addressed to may not be back (see
      # #finish_recovery!, and #reopen for the single-channel case).
      if @confirms_enabled
        send_and_wait(AMQ::Protocol::Confirm::Select.encode(@channel_id, false), AMQ::Protocol::Confirm::SelectOk)
      end
      send_and_wait(AMQ::Protocol::Tx::Select.encode(@channel_id), AMQ::Protocol::Tx::SelectOk) if @tx_mode
    end

    private

    # -------------------------------------------------------------------------
    # Frame dispatch
    # -------------------------------------------------------------------------

    def start_dispatch_task
      Async::Task.current.async { dispatch_loop }
    end

    def dispatch_loop
      loop do
        msg = @queue.pop
        break if msg.nil?
        handle_message(msg)
      end
    rescue => e
      @logger.error("Channel #{@channel_id} dispatch error: #{e.class}: #{e.message}")
    end

    def handle_message(msg)
      type, *rest = msg

      case type
      when :method
        handle_method(rest[0])
      when :content
        # Content for a basic.get: hand it straight to the waiting fiber, or
        # park it if the getter has not reached wait_content yet. It must never
        # be left behind once consumed, or the next basic_get would receive
        # the previous message's body.
        content = { header: rest[0], body: rest[1] }
        if (cond = @content_condition)
          @content_condition = nil
          cond.signal(content)
        else
          @pending_content = content
        end
      when :heartbeat
        # connection-level, ignore at channel layer
      end
    end

    def handle_method(method)
      case method
      when AMQ::Protocol::Basic::Deliver
        # Pop content directly — must not suspend dispatch_loop via wait_content.
        # After Deliver, the broker sends header+body frames immediately on this channel.
        content_msg = @queue.pop
        _, header, body = content_msg
        entry = @consumers[method.consumer_tag]
        if entry
          Async do
            @pool_sem.acquire do
              started = instrument_clock
              begin
                entry[:block].call(method, header, body)
              ensure
                if started
                  instrument("message.consumed") do
                    { channel: @channel_id, queue: entry[:queue_name], consumer_tag: method.consumer_tag,
                      bytes: body.to_s.bytesize, redelivered: method.redelivered,
                      duration: instrument_elapsed(started) }
                  end
                end
              end
            end
          end
        else
          @logger.warn("Delivery on channel #{@channel_id} for unknown consumer #{method.consumer_tag}")
        end

      when AMQ::Protocol::Basic::Return
        # Same pattern: pop content directly.
        content_msg = @queue.pop
        _, header, body = content_msg
        instrument("message.returned") do
          { channel: @channel_id, exchange: method.exchange, routing_key: method.routing_key,
            code: method.reply_code, text: method.reply_text, bytes: body.to_s.bytesize }
        end
        if @return_handler
          Async { @return_handler.call(method, header, body) }
        else
          @logger.warn("Unhandled basic.return on channel #{@channel_id} — register on_return to handle")
        end

      when AMQ::Protocol::Basic::Ack
        handle_confirm_ack(method)

      when AMQ::Protocol::Basic::Nack
        handle_confirm_nack(method)

      when AMQ::Protocol::Channel::Close
        handle_channel_close(method)

      when AMQ::Protocol::Basic::Cancel
        # Server-initiated consumer cancel (e.g. queue deleted, HA failover).
        # Remove from @consumers so deliveries are no longer dispatched to a dead block.
        entry = @consumers.delete(method.consumer_tag)
        topology&.delete_consumer(method.consumer_tag)
        if entry
          @logger.warn("Channel #{@channel_id}: broker cancelled consumer #{method.consumer_tag}")
          instrument("consumer.cancelled") do
            { channel: @channel_id, consumer_tag: method.consumer_tag, queue: entry[:queue_name], reason: :broker }
          end
          @on_cancel&.call(method.consumer_tag)
        end
        wake_each_waiters(method.consumer_tag)
        # No CancelOk to send for server-initiated cancel (no-wait is implicit).

      when AMQ::Protocol::Channel::Flow
        # Server-initiated flow control — broker throttling this channel.
        @flow_active = method.active
        @frame_io.write_frame(AMQ::Protocol::Channel::FlowOk.encode(@channel_id, method.active).encode)

      when AMQ::Protocol::Connection::Blocked,
           AMQ::Protocol::Connection::Unblocked
        # These arrive on channel 0 and are handled by the Session channel-0 monitor.
        # They should never reach a Channel object — log and ignore defensively.
        @logger.debug("Channel #{@channel_id}: ignoring connection-level #{method.class} frame")

      else
        # Wake any fiber waiting on this method type
        @reply_condition&.signal(method)
        @reply_condition = nil
        # Yield so the newly-woken fiber (e.g. basic_consume registering its consumer)
        # can run before dispatch_loop processes the next queued message.
        Async::Task.current.yield
      end
    end

    def handle_confirm_ack(method)
      @mutex.acquire do
        if method.multiple
          @pending_confirms.reject! { |tag, _| tag <= method.delivery_tag }
        else
          @pending_confirms.delete(method.delivery_tag)
        end
        @confirm_condition&.signal
        release_outstanding_slots
      end
      instrument("message.confirmed") do
        { channel: @channel_id, delivery_tag: method.delivery_tag, multiple: method.multiple, acked: true }
      end
    end

    # A nack resolves the tag(s) like an ack does, but the rejection is recorded
    # so wait_for_confirms can report it instead of claiming success.
    def handle_confirm_nack(method)
      @mutex.acquire do
        rejected = if method.multiple
          @pending_confirms.keys.select { |tag| tag <= method.delivery_tag }
        else
          [method.delivery_tag]
        end
        rejected.each { |tag| @pending_confirms.delete(tag) }
        @nacked_tags.concat(rejected)
        @nacked_this_cycle.concat(rejected)
        @only_acks = false
        @confirm_condition&.signal
        release_outstanding_slots
      end
      instrument("message.confirmed") do
        { channel: @channel_id, delivery_tag: method.delivery_tag, multiple: method.multiple, acked: false }
      end
    end

    def handle_channel_close(method)
      code = method.reply_code
      text = method.reply_text
      @frame_io.write_frame(
        AMQ::Protocol::Channel::CloseOk.encode(@channel_id).encode
      )
      @state = :closed
      forget_consumers_in_topology
      @session.channel_closed(@channel_id)
      instrument("channel.closed") { { channel: @channel_id, reason: :broker, code: code, text: text } }
      @on_error&.call(self, method)

      error = if FrameIO::SOFT_ERROR_CODES.include?(code)
        ChannelError.new(code: code, text: text, channel_id: @channel_id, close_method: method)
      else
        ConnectionError.new(code: code, text: text)
      end

      # Wake any waiting fiber with the error
      @reply_condition&.signal(error)
      @reply_condition = nil
      # Stop dispatch_loop
      @queue&.push(nil)
      wake_each_waiters
    end

    # Release fibers blocked in #each for one consumer, or for all of them.
    def wake_each_waiters(tag = nil)
      waiters = tag ? [@each_waiters.delete(tag)].compact : @each_waiters.values.tap { @each_waiters.clear }
      waiters.each { |cond| cond.signal rescue nil }
    end

    # -------------------------------------------------------------------------
    # Synchronous wait helpers
    # -------------------------------------------------------------------------

    # Send one method frame and wait for its reply. Serialised per channel:
    # AMQP 0-9-1 replies carry no correlation id, so a second request in flight
    # on the same channel would be handed the first one's answer.
    def rpc(frame, *expected_classes, check_open: true)
      started = instrument_clock
      reply   = rpc_without_instrumentation(frame, *expected_classes, check_open: check_open)
      if started
        instrument("channel.rpc") do
          # amq-protocol method classes report their AMQP name, e.g. "queue.declare-ok".
          { channel: @channel_id, method: reply.class.name, duration: instrument_elapsed(started) }
        end
      end
      reply
    end

    def rpc_without_instrumentation(frame, *expected_classes, check_open: true)
      return send_and_wait(frame, *expected_classes) unless check_open

      # Park before taking the semaphore so that reopen_after_recovery (which
      # bypasses it) is never blocked by a waiter holding it.
      wait_for_recovery! if recovering?
      @rpc_sem.acquire do
        assert_open!
        send_and_wait(frame, *expected_classes)
      end
    end

    def send_and_wait(frame, *expected_classes)
      @frame_io.write_frame(frame.encode)
      wait_for_any(*expected_classes)
    end

    def topology
      @session.respond_to?(:topology) ? @session.topology : nil
    end

    def recover_entity(what)
      yield
    rescue ChannelError => e
      @logger.error("Channel #{@channel_id}: could not recover #{what}: #{e.message}")
      # The broker closed this channel; put it back so the rest can continue.
      @session.reopen_channel(self)
    end

    # Encode one basic.publish (method + header + body frames) into a single
    # byte string, so a message always hits the write queue in one piece.
    def encode_publish(payload, exchange:, routing_key:, mandatory: false, persistent: false,
                       content_type: nil, content_encoding: nil, headers: nil, priority: nil,
                       correlation_id: nil, reply_to: nil, expiration: nil, message_id: nil,
                       timestamp: nil, type: nil, user_id: nil, app_id: nil, properties: {})
      payload_bytes = payload.is_a?(String) ? payload.b : payload
      props = { delivery_mode: persistent ? 2 : 1 }
      props[:content_type]     = content_type     if content_type
      props[:content_encoding] = content_encoding if content_encoding
      props[:headers]          = headers          if headers
      props[:priority]         = priority         if priority
      props[:correlation_id]   = correlation_id   if correlation_id
      props[:reply_to]         = reply_to         if reply_to
      props[:expiration]       = expiration       if expiration
      props[:message_id]       = message_id       if message_id
      props[:timestamp]        = timestamp        if timestamp
      props[:type]             = type             if type
      props[:user_id]          = user_id          if user_id
      props[:app_id]           = app_id           if app_id
      props.merge!(properties)

      # Basic::Publish.encode splits the body across frames of at most frame_max.
      AMQ::Protocol::Basic::Publish.encode(
        @channel_id, payload_bytes, props, exchange, routing_key, mandatory, false, @frame_max
      ).map(&:encode).join
    end

    # Under confirms: take the next delivery tag and keep the encoded message
    # until the broker acks it, so it can be re-published after a reconnect.
    def reserve_confirm_tag(bytes)
      @delivery_tag += 1
      @pending_confirms[@delivery_tag] = bytes
      @delivery_tag
    end

    # Confirm tracking backpressure: park until there is room for +needed+ more
    # unconfirmed messages under outstanding_limit (a batch larger than the
    # limit waits for the channel to be fully confirmed). Acks and nacks wake
    # the waiters; a connection loss or close fails them.
    def wait_for_outstanding_slot(needed)
      return unless @outstanding_limit

      target = [@outstanding_limit - needed, 0].max
      while @pending_confirms.size > target
        @slot_condition ||= Async::Condition.new
        outcome = wait_with_timeout(@slot_condition) do
          @slot_condition = nil
          "No publisher confirm freed a slot within #{@rpc_timeout}s " \
          "(#{@pending_confirms.size} outstanding, limit #{@outstanding_limit}) on channel #{@channel_id}"
        end
        raise outcome if outcome.is_a?(Exception)
        assert_open!
      end
    end

    def release_outstanding_slots(outcome = nil)
      cond = @slot_condition
      @slot_condition = nil
      cond&.signal(outcome)
    end

    # Messages published under confirms whose ack never arrived are sent again
    # on the reopened channel with fresh delivery tags (numbering restarts at 1).
    # A message the broker had in fact accepted before the drop is delivered
    # twice: the usual at-least-once trade-off of confirms across a reconnect.
    def republish_unconfirmed
      pending            = @pending_confirms.values
      @pending_confirms  = {}
      @delivery_tag      = 0
      @confirm_condition ||= Async::Condition.new   # keep one an early waiter created
      pending.each do |bytes|
        @frame_io.write_frame(bytes, publish: true)
        @delivery_tag += 1
        @pending_confirms[@delivery_tag] = bytes
      end
      @logger.info("Channel #{@channel_id}: re-published #{pending.size} unconfirmed message(s) after recovery") unless pending.empty?
    end

    def wait_for_recovery!
      @recovered_condition ||= Async::Condition.new
      outcome = @recovered_condition.wait
      raise outcome if outcome.is_a?(Exception)
    end

    def resume_parked!(outcome)
      cond = @recovered_condition
      @recovered_condition = nil
      cond&.signal(outcome)
    end

    def wait_for_any(*expected_classes)
      condition = @reply_condition = Async::Condition.new
      result    = wait_with_timeout(condition) do
        @reply_condition = nil if @reply_condition.equal?(condition)
        # amq-protocol method classes report their AMQP name, e.g. "queue.declare-ok".
        "No reply to #{expected_classes.map(&:name).join('/')} within #{@rpc_timeout}s on channel #{@channel_id}"
      end

      raise result if result.is_a?(Exception)

      unless expected_classes.any? { |c| result.is_a?(c) }
        raise ChannelError.new("Expected #{expected_classes.join(' or ')} but got #{result.class}",
                               channel_id: @channel_id)
      end
      result
    end

    def wait_content
      if @pending_content
        content = @pending_content
        @pending_content = nil
        return content
      end

      condition = @content_condition = Async::Condition.new
      wait_with_timeout(condition) do
        @content_condition = nil if @content_condition.equal?(condition)
        "No message content received within #{@rpc_timeout}s on channel #{@channel_id}"
      end
    end

    # Wait on +condition+, bounded by the channel's rpc_timeout. On expiry the
    # block tidies the waiter slot and returns the message for RpcTimeoutError.
    def wait_with_timeout(condition)
      return condition.wait unless @rpc_timeout

      Async::Task.current.with_timeout(@rpc_timeout) { condition.wait }
    rescue Async::TimeoutError
      raise RpcTimeoutError, yield
    end

    def re_register_consumers
      @consumers.each do |tag, entry|
        begin
          basic_consume(entry[:queue_name], consumer_tag: tag, manual_ack: entry[:manual_ack], &entry[:block])
        rescue => e
          # Typically 404: the queue is gone and was not recovered. The broker
          # closes the channel on that, so say so instead of failing silently.
          @logger.error("Channel #{@channel_id}: could not re-register consumer #{tag} on #{entry[:queue_name]}: #{e.class}: #{e.message}")
        end
      end
    end

    # Raise unless the channel is open. While the connection is being recovered
    # the caller is parked instead and resumes once the channel is reopened, so
    # publishes and RPCs issued during an outage neither fail nor vanish; if
    # recovery is abandoned they raise the final error.
    def assert_open!
      wait_for_recovery! if recovering?
      raise NotOpenError, "Channel #{@channel_id} is not open" unless open?
    end

    def validate_pool_size!(n)
      raise ArgumentError, "pool_size must be a positive Integer (got #{n.inspect})" unless n.is_a?(Integer) && n > 0
      n
    end
  end
end
