require "async"
require "async/condition"
require "async/promise"
require "async/semaphore"
require_relative "errors"

module AsyncRabbitMQ
  # Represents an AMQP channel multiplexed over a Session's connection.
  #
  # Duck-typed stream interface: #each (yields deliveries) and #write (publishes).
  # Does NOT inherit Async::IO::Stream — a channel is logical, not physical IO.
  class Channel
    # What to do with a delivery whose handler raised. Chosen per consumer, or
    # returned by the on_handler_error hook to decide per message.
    #
    # :dead_letter  reject(requeue: false) -- one attempt, then the queue's
    #               dead-letter route. Safe on any queue, so it is the default:
    #               :retry on a queue with no delivery limit loops forever, and
    #               a default must not depend on configuration the client
    #               cannot see.
    # :retry        reject(requeue: true) -- hand it back and let the broker
    #               count. A quorum queue with x-delivery-limit retires it (and
    #               dead-letters it) once the limit is reached, with
    #               x-delayed-retry-* spacing the attempts.
    # :release      nack(requeue: true) -- hand it back WITHOUT counting the
    #               failure against the message. For a failure that is not the
    #               message's fault: shutting down, shedding load, a downstream
    #               outage. On a poison message this loops forever, since
    #               nothing advances towards x-delivery-limit.
    # :leave        settle nothing. Bunny's behaviour: the delivery stays
    #               unacknowledged and it is yours to resolve.
    #
    # :retry rejects and :release nacks, and the difference is the whole point.
    # The broker counts a delivery as failed only on basic.reject (AMQP 1.0
    # modified/delivery-failed); after basic.nack the x-delivery-count does not
    # move (AMQP 1.0 released). So reject means "this message failed, hold it
    # against it" and nack means "have it back, no blame". For a single
    # delivery the verbs are otherwise equivalent.
    ERROR_DISPOSITIONS = %i[dead_letter retry release leave].freeze

    DEFAULT_CONTENT_TYPE = "application/octet-stream".freeze

    # A handler is application code and can raise anything. Catching only
    # StandardError let a handler raising outside that hierarchy escape and take
    # the channel's frame dispatch with it, so the catch below is deliberately
    # as wide as Ruby allows - and these are what it has to put back.
    #
    # Async stops a task by raising Async::Stop into it, and Async::Stop is not
    # a StandardError precisely so that ordinary rescues leave it alone.
    # Swallowing it would leave handler workers unstoppable, and a channel close
    # or a reactor shutdown would hang on a parked handler. The signals beside
    # it are not this delivery's problem either: the process is exiting, was
    # interrupted, or has run out of memory.
    #
    # SystemStackError and ScriptError are deliberately absent. An accidental
    # infinite recursion in a handler is an ordinary bug and should fail that
    # one delivery, not silence the channel.
    PROPAGATE_FROM_HANDLER = [Async::Stop, SystemExit, SignalException, NoMemoryError].freeze

    # A message published under confirms whose fate is unknown: the broker
    # neither acked nor nacked it before the connection went. +payload+ and its
    # routing are kept so it can be published again on another connection.
    UnconfirmedMessage = Struct.new(:delivery_tag, :payload, :exchange, :routing_key, :options,
                                    keyword_init: true)

    # Where one publish call was addressed. Built once per call and shared by
    # every message in a batch, so the send path allocates one small pair per
    # message instead of a keyword struct and a copy of the options hash.
    PublishContext = Struct.new(:exchange, :routing_key, :options)

    # Fallback bound on the wait for channel.close-ok while resynchronising
    # after an RPC timeout, used when the session has no rpc_timeout set.
    RESYNC_TIMEOUT = 5

    # First wait before re-subscribing a consumer the broker cancelled, and the
    # ceiling the doubling stops at. Only used when the channel was asked to
    # recover cancelled consumers; see #recovers_cancelled_consumers?.
    RECOVER_CONSUMER_DELAY     = 1
    RECOVER_CONSUMER_MAX_DELAY = 30

    # A queue that is still declared and a queue that is gone both answer
    # NOT_FOUND to a passive declare, and only the reply text tells them apart.
    # Measured on RabbitMQ 4.4: a deleted queue gives "no queue 'q' in vhost
    # '/'", while a classic queue whose node has been stopped gives "queue 'q'
    # in vhost '/' process is stopped by supervisor" and comes back with its
    # node. Older brokers say "home node ... is down or inaccessible" for the
    # same thing. See #queue_missing?.
    QUEUE_UNAVAILABLE = /process is stopped|is down or inaccessible/

    # Pushed onto the work queue to retire one handler worker. Compared by
    # identity, so it can never collide with a delivery.
    WORKER_STOP = Object.new.freeze

    include Instrumented

    attr_reader :channel_id, :pool_size

    # How many times this channel has been opened on a connection: 0 until the
    # first reopen, then one more for each connection recovery or #reopen.
    #
    # The broker restarts both delivery tags and publisher confirm tags at 1 on
    # a reopened channel, so a tag only identifies a message together with the
    # generation it was issued in. Deliveries carry theirs (see
    # VersionedDeliveryTag); confirm tags returned by #basic_publish are plain
    # integers, so code keeping its own confirm bookkeeping across a reconnect
    # reads this.
    attr_reader :delivery_generation

    # +pool_size+ bounds the number of consumer-handler fibers that can run
    # concurrently on this channel (Bunny-parity: default 1 = serialized).
    # Resizable at runtime via #pool_size=; basic_qos adjusts it automatically
    # when prefetch_count > 0 so the two stay coupled.
    def initialize(channel_id, session, frame_io, frame_max:, logger:, pool_size: 1, rpc_timeout: nil,
                   recover_cancelled_consumers: false)
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
      @pending_confirms  = {}        # delivery_tag => [frames, payload, PublishContext]
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
      @on_handler_error  = nil
      @recover_cancelled_consumers = recover_cancelled_consumers
      @on_error_disposition = :dead_letter
      @resyncing         = false
      @resync_promise    = nil
      # Incremented on every reopen (connection recovery or a single-channel
      # reopen). Delivery tags are stamped with it so acks raised against a
      # previous generation can be dropped instead of hitting a live message.
      @delivery_generation = 0
      @unsettled         = {}        # delivery tags delivered to a manual-ack consumer, not yet settled
      @qos_warned        = false
      @mutex             = Async::Semaphore.new(1)
      @publish_sem       = Async::Semaphore.new(1)   # one publish's frames go out contiguously
      @rpc_sem           = Async::Semaphore.new(1)   # one request/reply in flight per channel
      @reply_promise     = nil
      @content_condition = nil
      @pending_content   = nil
      @recovery_promise  = nil       # per-episode latch; fibers park on it while the connection recovers
      @pool_size         = validate_pool_size!(pool_size)
      @work_queue        = nil       # Async::Queue of [method, header, body, entry]
      @workers           = []        # long-lived handler fibers draining it
    end

    # Resize the consumer-handler concurrency cap by adding or retiring
    # workers. Growing takes effect at once. Shrinking is a sentinel on the
    # work queue, so it retires a worker as soon as the queue drains to it:
    # immediately when idle, after the current backlog when busy. A handler
    # already running is never interrupted.
    def pool_size=(n)
      n = validate_pool_size!(n)
      return if n == @pool_size

      @pool_size = n
      resize_workers
    end

    # Deliveries handed to the handler workers but not yet picked up. Bunny
    # reports the same number as ConsumerWorkPool#backlog.
    def backlog
      @work_queue&.size || 0
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

    # True while the channel is being reopened — after an RPC timeout, or
    # through #reopen. Operations park until it is back, as they do during
    # recovery: a publish issued now would either be dropped by the broker
    # (the channel is closing) or overtake the unconfirmed messages being
    # replayed onto the new one, and a request's reply would be discarded as
    # the late one we are shedding.
    def resyncing?
      @state == :resyncing
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
      # A resync is reopening the channel in the background; it will land on
      # :open (or :closed if it failed, or :recovering if the connection then
      # dropped). Park until it settles, then close the live channel — rather
      # than falling through `return unless open?` and silently doing nothing
      # while the channel goes on consuming. If recovery is abandoned while we
      # wait, wait_for_recovery! raises the reason; the channel is closed
      # either way, which is what the caller asked for, so swallow it.
      begin
        wait_for_recovery! while resyncing?
      rescue => e
        @logger.debug("Channel #{@channel_id}: close saw recovery end while resyncing: #{e.class}")
        return
      end
      if recovering?
        # Give the channel up rather than letting recovery reopen it.
        drop!(NotOpenError.new("Channel #{@channel_id} closed during recovery"), reason: :user)
        return
      end
      return unless open?
      close_attempts = 0
      begin
        rpc(AMQ::Protocol::Channel::Close.encode(@channel_id, 200, "Goodbye", 0, 0), AMQ::Protocol::Channel::CloseOk)
      rescue RpcTimeoutError
        # No close-ok in time. rpc's ensure has already resynchronised the
        # channel: it is back :open with its consumers (or dead, its waiters
        # failed through the resync's own teardown). Nothing is left parked,
        # but the close did not happen — try it once more on the resynced
        # channel. A broker that answers neither attempt gets the error raised
        # to the caller, with the channel alive and usable; raising beats
        # pretending it closed, and beats force-forgetting an id the broker
        # still considers open, which a later channel.open would collide with.
        close_attempts += 1
        retry if close_attempts == 1 && open?
        # The resync can also leave the channel in a terminal state. Already
        # :closed means its teardown ran (waiters failed, id released): close
        # is done, not failed. :recovering means the connection went while the
        # resync waited; the user asked for the channel to be gone, so drop it
        # before the next recovery pass resurrects it, consumers and all.
        return if closed?
        unless open?
          drop!(NotOpenError.new("Channel #{@channel_id} closed during recovery"), reason: :user)
          return
        end
        raise
      rescue => e
        # The connection went while we waited (ConnectionError, NotOpenError
        # out of the resync/park machinery). The session's teardown has
        # already released this channel's waiters; the user still asked for
        # the channel to be gone, so give it up rather than let recovery
        # resurrect it. Already :closed means exactly that — done.
        drop!(NotOpenError.new("Channel #{@channel_id} closed during recovery"), reason: :user) unless closed?
        return
      end
      @state = :closed
      # Confirms outstanding at close will never be resolved now; a fiber in
      # wait_for_confirms or parked on an outstanding slot has to hear that
      # from us, or it waits forever.
      fail_waiters!(NotOpenError.new("Channel #{@channel_id} was closed"))
      # Closing the channel cancelled its consumers on the broker side; an
      # auto-delete queue that just lost its last one is gone with it.
      forget_consumers_in_topology
      instrument("channel.closed") { { channel: @channel_id, reason: :user } }
      @session.channel_closed(@channel_id)
      @queue&.push(nil)  # wake dispatch_loop so it can detect :closed and exit
      stop_workers
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
      bytes   = encode_publish(payload, exchange: exchange, routing_key: routing_key, **opts)
      context = @confirms_enabled ? publish_context(exchange, routing_key, opts) : nil
      # One publish's frames must reach the wire contiguously and confirm tags
      # must follow wire order, so the write and the tag assignment happen
      # under one semaphore (write_frame can yield on a full queue or the
      # connection.blocked gate).
      tag = @publish_sem.acquire do
        assert_open!
        wait_for_outstanding_slot(1)
        @frame_io.write_frame(bytes, publish: true)
        reserve_confirm_tag(bytes, payload, context) if @confirms_enabled
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
        if @confirms_enabled
          context = publish_context(exchange, routing_key, opts)
          payloads.each_with_index.map { |p, i| reserve_confirm_tag(encoded[i], p, context) }
        end
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
        # Reply slot registered before the write, as in send_and_wait: the
        # reply can be dispatched while write_frame yields on a full queue.
        promise = @reply_promise = Async::Promise.new
        @frame_io.write_frame(AMQ::Protocol::Basic::Get.encode(@channel_id, queue_name, !manual_ack).encode)
        m = wait_for_any(promise, AMQ::Protocol::Basic::GetOk, AMQ::Protocol::Basic::GetEmpty)
        stamp_delivery_tag(m) if m.is_a?(AMQ::Protocol::Basic::GetOk)
        # After GetOk the content header + body follow on this channel.
        [m, m.is_a?(AMQ::Protocol::Basic::GetOk) ? wait_content : nil]
      end
      return nil if content.nil?

      [msg, content[:header], content[:body]]
    end

    # Acknowledge a delivery. Returns true if the ack was sent, false if the
    # tag came from an earlier connection (or an earlier life of this channel)
    # and was dropped — see VersionedDeliveryTag.
    def basic_ack(delivery_tag, multiple: false)
      assert_open!
      return false if stale_delivery_tag?(delivery_tag, "ack")
      settle(delivery_tag, multiple: multiple)
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Ack.encode(@channel_id, delivery_tag.to_i, multiple).encode
      )
      true
    end

    def basic_nack(delivery_tag, multiple: false, requeue: true)
      assert_open!
      return false if stale_delivery_tag?(delivery_tag, "nack")
      settle(delivery_tag, multiple: multiple)
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Nack.encode(@channel_id, delivery_tag.to_i, multiple, requeue).encode
      )
      true
    end

    def basic_reject(delivery_tag, requeue: true)
      assert_open!
      return false if stale_delivery_tag?(delivery_tag, "reject")
      settle(delivery_tag)
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Reject.encode(@channel_id, delivery_tag.to_i, requeue).encode
      )
      true
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
    # +on_error+ decides what happens to a delivery whose handler raises; see
    # ERROR_DISPOSITIONS. nil takes the channel's default, set with
    # #on_error_disposition=.
    def basic_consume(queue_name, consumer_tag: "", manual_ack: false, exclusive: false,
                      arguments: {}, on_error: nil, &block)
      validate_disposition!(on_error) if on_error
      warn_unbounded_prefetch(queue_name, manual_ack)
      resp = rpc(
        AMQ::Protocol::Basic::Consume.encode(@channel_id, queue_name, consumer_tag, false, !manual_ack, exclusive, false, arguments),
        AMQ::Protocol::Basic::ConsumeOk
      )
      # Everything basic.consume was called with, so re_register_consumers can
      # reproduce the consumer exactly rather than a default-valued lookalike.
      @consumers[resp.consumer_tag] = { queue_name: queue_name, block: block,
                                        manual_ack: manual_ack, on_error: on_error,
                                        exclusive: exclusive, arguments: arguments }
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
    # +timeout+ in seconds bounds the whole wait; nil (the default) waits
    # forever, as before. A broker that accepts a publish and then never
    # confirms it otherwise parks the caller indefinitely.
    def wait_for_confirms(timeout: nil)
      deadline = timeout && (Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout)

      until @pending_confirms.empty?
        # A closed channel will never resolve them. The teardown paths signal
        # the waiters parked at that moment; a waiter arriving afterwards has
        # to hear it here, or it parks on a condition nothing will ever signal.
        raise NotOpenError, "Channel #{@channel_id} is not open" if closed?
        # Lazily (re)created: interrupt_wait! clears it on connection loss and a
        # caller may arrive before reopen_after_recovery has re-selected confirms.
        @confirm_condition ||= Async::Condition.new

        outcome = if deadline
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise ConfirmTimeoutError.new(unconfirmed_tags: @pending_confirms.keys, channel_id: @channel_id) if remaining <= 0

          begin
            Async::Task.current.with_timeout(remaining) { @confirm_condition.wait }
          rescue Async::TimeoutError
            raise ConfirmTimeoutError.new(unconfirmed_tags: @pending_confirms.keys, channel_id: @channel_id)
          end
        else
          @confirm_condition.wait
        end

        # A teardown path signalled us its error (broker channel close, user
        # close, connection loss): raise that, it names what actually happened.
        raise outcome if outcome.is_a?(Exception)
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

    # The messages published under confirms that the broker has not yet acked
    # or nacked, as UnconfirmedMessage records carrying the payload and where
    # it was going. Unlike the encoded frames kept for replay on this channel,
    # these can be republished anywhere — which is what a caller handed them by
    # Cluster#on_node_down needs to do when the node they were on is gone.
    def unconfirmed_messages
      @pending_confirms.map do |tag, (_bytes, payload, context)|
        UnconfirmedMessage.new(delivery_tag: tag, payload: payload,
                               exchange: context&.exchange, routing_key: context&.routing_key,
                               options: context ? context.options.dup : {})
      end
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
    #
    # The callback runs in a task of its own, so it may issue RPCs - a
    # re-consume, a declare - and an exception out of it is logged rather than
    # taken out on the channel.
    def on_cancel(&block)
      @on_cancel = block
    end

    # Re-subscribe a consumer the broker cancels, under the same consumer tag.
    #
    # Off by default, as it is in Bunny, and for a reason worth stating: the
    # broker cancels a consumer when it can no longer serve the queue, and one
    # of the ways that happens is somebody deleting the queue. A client that
    # recovers by re-declaring would hand back a queue nobody can get rid of
    # while a consumer is attached. So recovery here never declares anything.
    # It passively checks that the queue is still there and re-consumes if it
    # is; if the queue has gone it stops, and forgets the queue so that a later
    # connection recovery does not re-declare it either.
    #
    # Retries back off from RECOVER_CONSUMER_DELAY to RECOVER_CONSUMER_MAX_DELAY
    # and continue for as long as the queue exists and the channel is open.
    attr_writer :recover_cancelled_consumers

    def recovers_cancelled_consumers?
      @recover_cancelled_consumers
    end

    # Register a callback invoked when the broker closes this channel due to an
    # error (e.g. 404 queue not found). The block receives the channel and the
    # AMQ::Protocol::Channel::Close method frame.
    def on_error(&block)
      @on_error = block
    end

    # Register a callback invoked when a consumer block raises. The block
    # receives the exception, the Basic::Deliver method frame and the queue
    # name. The delivery has already been nacked (requeue: false) if the
    # consumer was registered with manual_ack: true.
    # The block receives the exception, the Basic::Deliver frame and the queue
    # name, and runs *before* the delivery is settled. Return one of
    # ERROR_DISPOSITIONS to decide this message's fate; return anything else
    # (or nothing) to take the consumer's or channel's configured default.
    def on_handler_error(&block)
      @on_handler_error = block
    end

    # The channel-wide default disposition for a handler that raises, used by
    # any consumer that did not set its own. See ERROR_DISPOSITIONS.
    attr_reader :on_error_disposition

    def on_error_disposition=(value)
      validate_disposition!(value)
      @on_error_disposition = value
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
    # Remaining keywords go to #basic_consume, so +on_error+, +exclusive+ and
    # +arguments+ work here as they do there.
    def each(queue_name, manual_ack: false, **opts, &block)
      assert_open!
      tag  = basic_consume(queue_name, manual_ack: manual_ack, **opts, &block)
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
      begin_recovery_wait!
      @state = :recovering
      interrupt_wait!(error)
    end

    # Start a park-and-wait episode (:recovering or :resyncing): a fresh
    # promise every arriving waiter latches onto. A promise, not a condition:
    # resolution is sticky, so a waiter that arrives after finish_recovery! has
    # already resolved the episode returns at once instead of parking on a
    # signal that will never come again — the lost-wakeup class that used to
    # need a re-check loop at every wait site. Every transition INTO
    # :recovering or :resyncing must come through here.
    #
    # A still-PENDING promise is kept, not replaced: fibers are parked on it,
    # and a replacement would orphan them forever — a resync that a connection
    # drop escalates into recovery is one episode to its waiters, resolved by
    # whichever teardown or finish ends it. Only a resolved (or absent) promise
    # means the previous episode is over and a new latch is needed.
    def begin_recovery_wait!
      @recovery_promise = Async::Promise.new if @recovery_promise.nil? || @recovery_promise.resolved?
    end

    # Give the channel up while the session is recovering, instead of letting
    # recovery reopen it: everything waiting or parked raises +error+, #each
    # callers return, the consumers are forgotten and the id goes back.
    def drop!(error, reason: :dropped)
      mark_closed!(error)
      forget_consumers_in_topology
      @session.channel_closed(@channel_id)
      instrument("channel.closed") { { channel: @channel_id, reason: reason } }
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
      fail_waiters!(error)
      @queue&.push(nil) rescue nil
      # The connection is gone, so the queued deliveries are too: the broker
      # requeues whatever was unacknowledged and sends it again on the new one.
      stop_workers(discard: true)
    rescue => e
      # ignore — best-effort unblock
    end

    # Wake every fiber parked on a reply, content, confirm or slot wait with
    # +error+. Every teardown path — connection loss, broker-sent channel
    # close, user close — must release all four: whichever one a path misses
    # parks its caller forever, since nothing else will ever signal it on a
    # channel that is gone. A confirm waiter with no timeout (the default) and
    # a publisher parked on an outstanding slot are the two with no clock of
    # their own.
    def fail_waiters!(error)
      # Reply and resync are one-shot promises: resolving with the error hands
      # it to the waiter through the same value path a real reply takes, and a
      # second resolution (a race between teardown paths) is a silent no-op.
      promise = @reply_promise
      @reply_promise = nil
      promise&.resolve(error)
      @content_condition&.signal(error)
      @content_condition = nil
      @confirm_condition&.signal(error)
      @confirm_condition = nil
      release_outstanding_slots(error)
      # A resync in progress is parked waiting for a close-ok that this
      # connection will never deliver. Wake it so it can see the channel is
      # now :recovering and hand the reopen to the recovery loop instead of
      # racing it (see resync_after_rpc_timeout). Nil the slot too, so the
      # dispatch filter stops discarding frames — recovery's channel.open-ok
      # among them.
      promise = @resync_promise
      @resync_promise = nil
      promise&.resolve(error)
    end

    # -------------------------------------------------------------------------
    # Internal: called by Session after reconnect
    # -------------------------------------------------------------------------

    # Internal, after Session has reopened every channel and replayed the
    # topology: release parked publishers/RPCs first (they may hold the
    # semaphores basic_consume needs), then re-register the consumers.
    def finish_recovery!
      # After the session has replayed the topology, so a message addressed to a
      # queue or exchange the broker lost is not sent into the void before it is
      # re-declared. Before the channel goes :open, so that a new publisher
      # cannot slip in if the replay yields on a full write queue and take a
      # delivery tag that no longer matches its place on the wire.
      republish_unconfirmed if @confirms_enabled
      @state = :open
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

    # Internal: a server-named queue came back under a new name — rewrite the
    # unconfirmed publishes that refer to it, in two places. The routing key,
    # when the publish went through the default exchange, whose routing key IS
    # the queue name: replayed unchanged it addresses a queue that no longer
    # exists, unroutable and silently dropped — the loss confirms exist to
    # prevent. And reply_to, whatever the exchange: in the RPC pattern the
    # request names the publisher's own server-named queue there, and a replay
    # carrying the old name sends the responder's answer to a dead queue. Only
    # those two are structurally tied to the queue name; a routing key on a
    # named exchange is the caller's semantics, and the renamed binding already
    # carries it to the right queue. The rewrite only updates the kept context:
    # republish_unconfirmed re-encodes the frames from it.
    def rename_unconfirmed_queue(old_name, new_name)
      @pending_confirms.each_value do |entry|
        ctx = entry[2]
        next unless ctx

        routing  = ctx.exchange.to_s.empty? && ctx.routing_key == old_name
        reply_to = ctx.options[:reply_to] == old_name ||
                   (ctx.options[:properties].is_a?(Hash) && ctx.options[:properties][:reply_to] == old_name)
        next unless routing || reply_to

        opts = ctx.options
        if reply_to
          opts = opts.dup
          opts[:reply_to] = new_name if opts[:reply_to] == old_name
          if opts[:properties].is_a?(Hash) && opts[:properties][:reply_to] == old_name
            opts[:properties] = opts[:properties].merge(reply_to: new_name)
          end
        end
        # Contexts are frozen and a batch shares one across its entries, so
        # replace rather than mutate. publish_context re-copies the options.
        entry[2] = publish_context(ctx.exchange, routing ? new_name : ctx.routing_key, opts)
      end
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
      # Reopened :resyncing, not :open: the replay below must reach the wire
      # before any new publish, or a tag no longer matches its position on it.
      begin_recovery_wait! # callers arriving while :resyncing park on this episode
      @session.reopen_channel(self, state: :resyncing)
      # Only this channel was closed, so the topology is intact and anything
      # left unconfirmed can go straight back out.
      republish_unconfirmed if @confirms_enabled
      @state = :open
      resume_parked!(:open)
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
      # The reconnect may have negotiated a different frame_max (a failover to
      # a differently configured node). Splitting frames at the old size would
      # hand the broker over-sized body frames — a 501 that kills the fresh
      # connection — so every encode from here on, replays included, uses the
      # live value.
      @frame_max = @session.frame_max if @session.respond_to?(:frame_max)
      # The broker restarts delivery tags at 1 on a reopened channel, so any
      # tag handed out before this point is now stale.
      @delivery_generation += 1
      # Content parked for a basic_get that never collected it belongs to the
      # old channel; leaving it would hand it to the next basic_get.
      @pending_content = nil
      # Whatever was outstanding belonged to the old channel; the broker
      # requeued it and will deliver it again with fresh tags.
      @unsettled.clear
      # Stop the dispatch loop bound to the old queue before swapping it out.
      # It is parked inside old_queue.pop, which captured that queue as its
      # receiver, so reassigning @queue alone would never wake it: it would
      # park there for the life of the reactor (one leaked fiber per resync).
      # The connection-recovery path already pushed this nil via interrupt_wait!
      # — a second one into an orphaned queue is harmless; the resync path,
      # which keeps the live connection, has pushed nothing, so this is its
      # only stop signal.
      @queue&.push(nil) rescue nil
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
    # Delivery tags
    # -------------------------------------------------------------------------

    # Replace the broker's raw tag with one that also carries this channel's
    # generation. Basic::Deliver and Basic::GetOk expose delivery_tag through
    # attr_reader, so setting the ivar is enough for handlers to pick it up.
    def stamp_delivery_tag(method)
      raw = method.instance_variable_get(:@delivery_tag)
      return if raw.is_a?(VersionedDeliveryTag)

      method.instance_variable_set(:@delivery_tag,
                                   VersionedDeliveryTag.new(raw, @delivery_generation))
    end

    # A tag from an earlier generation refers to a message on a connection (or
    # a life of this channel) that no longer exists. The broker restarts tag
    # numbering at 1, so sending it now would either ack an unrelated message
    # or draw a 406 that closes the channel and takes its consumers with it.
    def stale_delivery_tag?(delivery_tag, action)
      return false unless delivery_tag.is_a?(VersionedDeliveryTag)
      return false unless delivery_tag.stale?(@delivery_generation)

      @logger.warn(
        "Channel #{@channel_id}: dropped #{action} for delivery tag #{delivery_tag.to_i} from " \
        "generation #{delivery_tag.generation} (now #{@delivery_generation}) — the message was " \
        "redelivered on the new connection"
      )
      instrument("delivery_tag.stale") do
        { channel: @channel_id, action: action, delivery_tag: delivery_tag.to_i,
          generation: delivery_tag.generation, current_generation: @delivery_generation }
      end
      true
    end

    # Without a bound the broker sends as fast as it can and the backlog is
    # held in this process. What can bound it depends on the ack mode, so the
    # advice does too: RabbitMQ applies prefetch only to *unacknowledged*
    # messages, so on an auto-ack consumer basic_qos never engages and telling
    # the user to call it would be wrong. Warned once per channel.
    def warn_unbounded_prefetch(queue_name, manual_ack)
      return unless AsyncRabbitMQ.warn_unbounded_consumers
      return if @qos_warned
      # A prefetch answers the manual-ack case, so having set one is a reason to
      # stay quiet there. It does nothing for an auto-ack consumer, so it is not
      # a reason here: that is precisely the case where the caller believes the
      # consumer is bounded and it is not.
      return if manual_ack && @prefetch && @prefetch[:count].to_i > 0

      @qos_warned = true
      @logger.warn(
        if manual_ack
          "Channel #{@channel_id}: consuming from #{queue_name} without basic_qos. " \
          "The broker will send the whole queue as fast as it can and the backlog is " \
          "held in this process. Call basic_qos(prefetch_count: n) before " \
          "basic_consume to bound it."
        else
          "Channel #{@channel_id}: consuming from #{queue_name} with automatic acks. " \
          "The broker treats every message as acknowledged on send, so basic_qos " \
          "cannot bound this consumer and the whole queue is held in this process. " \
          "Use manual_ack: true with basic_qos(prefetch_count: n) to get " \
          "backpressure, or watch #backlog."
        end
      )
    end

    # Note a delivery as settled so nothing acks or nacks it a second time.
    # +multiple+ settles every outstanding tag up to and including this one.
    def settle(delivery_tag, multiple: false)
      tag = delivery_tag.to_i
      if multiple
        @unsettled.delete_if { |t, _| t <= tag }
      else
        @unsettled.delete(tag)
      end
    end

    # False once the handler (or anyone else) has acked, nacked or rejected it.
    def unsettled?(delivery_tag)
      @unsettled.key?(delivery_tag.to_i)
    end

    # A consumer block raised. Without this the delivery stays unacked for the
    # life of the connection and, once prefetch messages are stuck that way,
    # the consumer stops receiving anything at all.
    def handle_consumer_error(error, method, entry)
      @logger.error(
        "Channel #{@channel_id}: consumer #{method.consumer_tag} raised "         "#{error.class}: #{error.message}"
      )
      instrument("consumer.error") do
        { channel: @channel_id, consumer_tag: method.consumer_tag, queue: entry[:queue_name],
          error: error.class.name, message: error.message }
      end

      # The hook runs before the delivery is settled, so what it returns can
      # decide the disposition. Anything outside ERROR_DISPOSITIONS (including
      # nil, and whatever a hook that was not written to choose happens to
      # return) falls through to the configured default.
      chosen = call_handler_error_hook(error, method, entry)
      disposition = ERROR_DISPOSITIONS.include?(chosen) ? chosen : consumer_disposition(entry)
      return if disposition == :leave

      # Only a manual-ack delivery the handler has not already settled. A
      # handler that acks and then raises in whatever follows would otherwise
      # be rejecting a tag the broker has already resolved: that is a 406,
      # which closes the channel and takes its consumers with it.
      return unless entry[:manual_ack] && unsettled?(method.delivery_tag)

      begin
        if disposition == :release
          # nack, not reject: released rather than failed, so the broker does
          # not count it against the message.
          basic_nack(method.delivery_tag, requeue: true)
        else
          basic_reject(method.delivery_tag, requeue: disposition == :retry)
        end
      rescue *PROPAGATE_FROM_HANDLER
        raise
      rescue Exception => e # rubocop:disable Lint/RescueException
        @logger.error("Channel #{@channel_id}: could not settle after consumer error: #{e.class}: #{e.message}")
      end
    end

    # Returns whatever the hook returned, or nil if there is no hook or it
    # raised. A hook that blows up must not also lose the delivery.
    def call_handler_error_hook(error, method, entry)
      return nil unless @on_handler_error

      @on_handler_error.call(error, method, entry[:queue_name])
    rescue *PROPAGATE_FROM_HANDLER
      raise
    rescue Exception => e # rubocop:disable Lint/RescueException
      # The hook is the caller's code as much as the handler is, and it runs
      # while a delivery is still unsettled: letting it escape would strand the
      # message and kill the worker.
      @logger.error("Channel #{@channel_id}: on_handler_error hook raised #{e.class}: #{e.message}")
      nil
    end

    def consumer_disposition(entry)
      entry[:on_error] || @on_error_disposition
    end

    def validate_disposition!(value)
      return if ERROR_DISPOSITIONS.include?(value)

      raise ArgumentError,
            "on_error must be one of #{ERROR_DISPOSITIONS.map(&:inspect).join(', ')} (got #{value.inspect})"
    end

    # -------------------------------------------------------------------------
    # Frame dispatch
    # -------------------------------------------------------------------------

    def start_dispatch_task
      start_workers
      @session.spawn_background { dispatch_loop }
    end

    # Up to pool_size long-lived fibers drain the work queue for the life of
    # this channel generation. A backlogged delivery costs a 4-element array
    # (~48 bytes) rather than a parked Async::Task and its fiber stack (~15.7 KB
    # measured), so memory tracks the messages held, not the fibers holding
    # them. Bunny's ConsumerWorkPool is the same shape: workers draining an
    # unbounded ::Queue.
    #
    # The queue is made here; the workers are not. See #ensure_worker.
    def start_workers
      stop_workers(discard: true)
      @work_queue = Async::Queue.new
      @workers    = []
    end

    # Create a worker on demand, never up front.
    #
    # basic_qos couples pool_size to prefetch_count, so creating them eagerly
    # meant a caller asking for a prefetch of 500 got 500 fibers, and a caller
    # who then set pool_size back down - the documented escape hatch from that
    # coupling - retired all but pool_size of them without a single delivery
    # between them. A fiber's touched stack pages stay in the process RSS after
    # it exits, measured at ~12 KB apiece: 12.6 MB at prefetch 500 across two
    # channels, which is the whole of the consumer memory difference the 24-hour
    # soaks showed.
    #
    # Demand never exceeds pool_size, so the concurrency bound is unchanged: a
    # channel that genuinely runs pool_size handlers at once still gets there,
    # one delivery at a time.
    def ensure_worker(queue)
      return if queue.nil? || @workers.size >= @pool_size

      @workers << @session.spawn_background { worker_loop(queue) }
    end

    # Retire this generation's workers. The sentinel goes on the back of the
    # queue, so by default they finish the deliveries already queued before
    # standing down - Bunny's ConsumerWorkPool#shutdown drains the same way.
    #
    # +discard+ throws that backlog away instead, for when the connection
    # underneath it has gone: the broker requeues whatever was unacknowledged
    # and sends it again on the new one, so running these handlers too would
    # process every queued message twice. dequeue(timeout: 0) returns at once
    # on an empty queue, so the drain cannot suspend us mid-teardown.
    def stop_workers(discard: false)
      queue = @work_queue
      nil while discard && queue&.dequeue(timeout: 0)
      @workers.size.times { queue&.push(WORKER_STOP) }
      @workers    = []
      @work_queue = nil
    end

    # Only ever shrinks. Growing is left to #ensure_worker, so raising pool_size
    # costs nothing until there are deliveries that need the extra concurrency.
    def resize_workers
      queue  = @work_queue
      excess = @workers.size - @pool_size
      return if queue.nil? || !excess.positive?

      excess.times { queue.push(WORKER_STOP) }
      @workers.pop(excess)
    end

    # Workers are bound to the queue they were started on, never to @work_queue,
    # so a worker outliving its generation cannot start draining the next one.
    #
    # A worker spawned from dispatch_loop takes its first delivery inside
    # dispatch_loop's own turn, because Async runs a new task immediately rather
    # than scheduling it. That is safe: a handler that waits - on a confirm, on
    # an RPC reply - suspends, and suspending hands control back to
    # dispatch_loop, so the reply still arrives. The two reentrancy examples in
    # 57_consumer_backlog_spec.rb pin that. An earlier revision yielded here
    # first; it was removed because no test could show it preventing anything.
    def worker_loop(queue)
      loop do
        job = queue.dequeue
        break if job.nil? || job.equal?(WORKER_STOP)

        run_handler(*job)
      end
    end

    def run_handler(method, header, body, entry)
      started = instrument_clock
      begin
        entry[:block].call(method, header, body)
      rescue *PROPAGATE_FROM_HANDLER
        raise
      rescue Exception => e # rubocop:disable Lint/RescueException
        handle_consumer_error(e, method, entry)
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

    def dispatch_loop
      loop do
        msg = @queue.pop
        break if msg.nil?
        handle_message(msg)
      end
    rescue *PROPAGATE_FROM_HANDLER
      raise
    rescue => e
      @logger.error("Channel #{@channel_id} dispatch error: #{e.class}: #{e.message}")
    rescue Exception => e # rubocop:disable Lint/RescueException
      # Handlers are contained now, so nothing should reach here. If something
      # does, this channel is about to stop delivering anything at all - no
      # confirms, no RPC replies, no close-ok - and the one thing that must not
      # happen is for it to go quiet without saying so.
      @logger.error(
        "Channel #{@channel_id} dispatch died on #{e.class}: #{e.message}. " \
        "The channel will not deliver anything further."
      )
      raise
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
      # While resynchronising, everything except the close-ok we are waiting
      # for belongs to the request that timed out. Dropping it here is the
      # point of the reopen: it must not reach the next caller.
      if @resync_promise
        case method
        when AMQ::Protocol::Channel::CloseOk
          promise = @resync_promise
          @resync_promise = nil
          promise.resolve(:closed)
        when AMQ::Protocol::Basic::Ack
          # Confirms still in flight for messages published before the close.
          # Dropping them would strand their entries in @pending_confirms: they
          # would be republished as duplicates, and wait_for_confirms would
          # never be woken for them.
          handle_confirm_ack(method)
        when AMQ::Protocol::Basic::Nack
          handle_confirm_nack(method)
        when AMQ::Protocol::Basic::Deliver, AMQ::Protocol::Basic::Return
          # Content frames follow on this channel; take them off with the
          # method so the stream does not skew.
          @queue.pop
          @logger.debug("Channel #{@channel_id}: discarded #{method.class} while resynchronising")
        else
          @logger.warn("Channel #{@channel_id}: discarded #{method.class} while resynchronising")
        end
        return
      end

      case method
      when AMQ::Protocol::Basic::Deliver
        # Pop content directly — must not suspend dispatch_loop via wait_content.
        # After Deliver, the broker sends header+body frames immediately on this channel.
        content_msg = @queue.pop
        _, header, body = content_msg
        entry = @consumers[method.consumer_tag]
        if entry
          stamp_delivery_tag(method)
          @unsettled[method.delivery_tag.to_i] = true if entry[:manual_ack]
          # Hand off without suspending: Async::Queue wraps Thread::Queue,
          # whose push never blocks. dispatch_loop must stay free, because it
          # is also what delivers this channel's confirms, close-ok and
          # returns - a handler that publishes and waits for confirms would
          # otherwise deadlock against the loop that would have woken it. That
          # is not hypothetical: it is rabbitmq-amqp-java-client#328, where a
          # bounded work pool parked the I/O thread in LinkedBlockingQueue#put.
          # A nil queue means the channel is going away and the delivery is moot.
          if (queue = @work_queue)
            queue.push([method, header, body, entry])
            ensure_worker(queue)
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
        handle_broker_cancel(method.consumer_tag)
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
        # Resolve the registered reply slot. The promise latches, so a reply
        # that lands while the requester is still parked inside its own
        # write_frame (full write queue) is kept for it.
        promise = @reply_promise
        @reply_promise = nil
        promise&.resolve(method)
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

      error = if FrameIO::SOFT_ERROR_CODES.include?(code)
        ChannelError.new(code: code, text: text, channel_id: @channel_id, close_method: method)
      else
        ConnectionError.new(code: code, text: text)
      end

      # Wake every parked fiber with the error — the RPC caller, but equally a
      # fiber in wait_for_confirms and a publisher parked on an outstanding
      # slot: no ack will ever arrive on a closed channel, so missing them
      # here was a permanent hang for a confirm wait with no timeout (the
      # default), and a 15s stall holding the publish semaphore for the slot.
      fail_waiters!(error)
      # Stop dispatch_loop
      @queue&.push(nil)
      stop_workers(discard: true)
      wake_each_waiters
      # Last, and in a task of its own. The lines above exist to release fibers
      # this close has stranded; a hook that is slow, or that raises, must not
      # come between them and the fibers waiting on them. The natural thing to
      # write in this hook is a reopen, which is an RPC, and an RPC on the
      # dispatch fiber waits for a reply only the dispatch fiber can deliver.
      notify_app("on_error") { @on_error.call(self, method) } if @on_error
    end

    # An application callback never runs on the dispatch fiber. Two reasons,
    # both load-bearing. An RPC issued from a callback waits for a reply that
    # only this fiber delivers, so it deadlocks until rpc_timeout and then
    # drags the channel through a resync - and a re-consume is the obvious
    # thing to write in on_cancel. And an exception out of a callback escapes
    # #handle_message, whose rescue sits outside dispatch_loop's `loop`, so the
    # loop ends and the channel stops delivering anything at all: no
    # deliveries, no confirms, no RPC replies, with one line to say so.
    #
    # Bunny hands cancel notifications to its consumer work pool and the Java
    # client to its work service, for the same reason.
    def notify_app(name, &block)
      @session.spawn_background { run_callback(name, &block) }
    end

    # The body of #notify_app, for a caller that is already off the dispatch
    # fiber and has something to do after the callback.
    def run_callback(name)
      yield
    rescue *PROPAGATE_FROM_HANDLER
      raise
    rescue Exception => e # rubocop:disable Lint/RescueException
      @logger.error("Channel #{@channel_id}: #{name} callback raised #{e.class}: #{e.message}")
    end

    # The broker cancels a consumer when it can no longer serve the queue to
    # it: the queue was deleted, or the node holding a classic queue stopped.
    # On a three-node RabbitMQ 4.4.0 cluster, moving a quorum queue's
    # leadership did not do it - not a graceful stop of the leader's node, not
    # removing the member on the consuming node, not losing the majority, and
    # not cutting the leader's node off the network until the queue re-elected.
    # Other versions are reported to cancel on unavailability; it makes no
    # difference here, since the handling is the same whatever the cause. The
    # connection is untouched either way, so connection recovery never runs:
    # this frame is the only notice there is.
    #
    # The consumer goes from @consumers first, so no delivery is dispatched to
    # a block that is no longer subscribed, and everything beyond that
    # bookkeeping happens off this fiber.
    def handle_broker_cancel(tag)
      entry = @consumers.delete(tag)
      topology&.delete_consumer(tag)
      return wake_each_waiters(tag) unless entry

      # Held back from topology recovery from this moment, before anything can
      # reconnect, because one of the reasons a consumer is cancelled is the
      # queue being deleted and a reconnect re-declares the registry wholesale.
      # Settled by #reconcile_cancelled_consumer, either way.
      topology&.suspend_queue(entry[:queue_name])
      @logger.warn("Channel #{@channel_id}: broker cancelled consumer #{tag} on #{entry[:queue_name]}")
      instrument("consumer.cancelled") do
        { channel: @channel_id, consumer_tag: tag, queue: entry[:queue_name], reason: :broker }
      end
      # The callback first and the reconcile after it, in one task rather than
      # two. on_cancel is where an application re-declares a queue it wants
      # back, and a reconcile running alongside it sometimes asked the broker
      # before that declare had landed and concluded the queue was gone. Still
      # off the dispatch fiber, so an RPC in the callback can get its reply and
      # an exception in it cannot stop the channel.
      @session.spawn_background do
        run_callback("on_cancel") { @on_cancel.call(tag) } if @on_cancel
        reconcile_cancelled_consumer(tag, entry)
      end
    end

    # Why the broker cancelled is not on the wire, and the reasons want
    # opposite handling: a queue that is merely not being served should be
    # consumed from again, while a queue somebody deleted must be left deleted.
    # A passive declare is the only way to tell them apart. Spring AMQP's
    # listener container probes the same way, and for the same reason declares
    # nothing.
    #
    # Runs whether or not this channel recovers cancelled consumers, because
    # forgetting a deleted queue is not a recovery feature: a queue left in the
    # topology registry is re-declared on the next connection recovery, and
    # then the deletion never sticks.
    def reconcile_cancelled_consumer(tag, entry)
      queue = entry[:queue_name]
      return recover_cancelled_consumer(tag, entry) if recovers_cancelled_consumers?

      # Not putting the consumer back, but a queue that has been deleted must
      # still leave the registry or the next reconnect declares it again.
      # Nothing is asked of a connection that is not there: a question put to a
      # session that is down cannot be answered, and guessing here is what puts
      # a deleted queue back.
      #
      # So the queue stays suspended until the broker gives a definite answer,
      # and only :available releases it. Releasing on a probe that could not be
      # answered is how a deleted queue comes back - it is not evidence the
      # queue is there, and 46_broker_amnesia_spec is the case that proves it.
      delay = RECOVER_CONSUMER_DELAY

      loop do
        case wait_for_usable_channel
        when nil          then return release_cancelled_consumer(tag, queue)
        when :reconnected then delay = RECOVER_CONSUMER_DELAY
        end

        case queue_status(queue)
        when :deleted   then return forget_deleted_queue(tag, queue)
        when :available then return release_cancelled_consumer(tag, queue)
        end

        # :unavailable - the broker says the queue is not being served - and
        # :unreachable - no answer at all - both mean ask again, never "the
        # queue is there". Sleeping on either is also what keeps this from
        # spinning when the probe cannot be answered.
        sleep delay
        delay = [delay * 2, RECOVER_CONSUMER_MAX_DELAY].min
      end
    end

    # Wait until this channel can carry a consumer again, through however many
    # reconnects that takes. :ready if it already could, :reconnected if it had
    # to wait for one, nil if it never will.
    #
    # Giving up on a connection loss instead would lose the consumer for good:
    # it is no longer in @consumers, so the reconnect does not re-register it,
    # and this loop is the only thing that would have brought it back. That is
    # precisely the case a quorum queue produces - the re-subscribe is answered
    # with a connection-level 541, not a channel error.
    #
    # Over for good when the channel is closed, which means the application
    # closed it or recovery could not reopen it and its consumers belong to
    # whoever does that, or when the session has stopped trying.
    def wait_for_usable_channel
      waited = false
      loop do
        return waited ? :reconnected : :ready if open? && @session.open?
        return nil if closed? || @session.closed?

        waited = true
        sleep RECOVER_CONSUMER_DELAY
      end
    end

    # Let the queue be recovered again and release anyone in #each. For a
    # consumer that will not be followed up - this channel does not recover
    # cancelled consumers, or there is nothing left to recover it onto - the
    # queue must not stay held back: it was never established that it is gone,
    # and a session opened again would not re-declare it.
    def release_cancelled_consumer(tag, queue)
      topology&.resume_queue(queue)
      wake_each_waiters(tag)
    end

    # Re-consume under the same tag, for as long as the queue is there and this
    # channel can carry a consumer. Nothing is declared and nothing is bound:
    # whatever the queue is, it is the one that was there before.
    def recover_cancelled_consumer(tag, entry)
      queue    = entry[:queue_name]
      delay    = RECOVER_CONSUMER_DELAY
      failures = 0

      loop do
        # Every round starts with a connection that works and a fresh answer
        # about the queue. A queue deleted while we were retrying has to stop
        # the loop, and a basic_consume the broker answers with a 404 closes
        # this channel and takes the consumers still working on it down too -
        # so the question is asked on a channel of its own first, and only an
        # answer of "it is there" is followed by an attempt to consume.
        case wait_for_usable_channel
        when nil then return release_cancelled_consumer(tag, queue)
        when :reconnected
          # The connection was what was wrong, and it is back: start the
          # backoff again rather than sleeping out a delay earned by an outage
          # that is over. A two-second blip should not leave a healthy queue
          # unconsumed for the next thirty.
          delay = RECOVER_CONSUMER_DELAY
        end

        case queue_status(queue)
        when :deleted
          return forget_deleted_queue(tag, queue)
        when :unreachable
          # No answer rather than an answer: the connection is what is wrong,
          # not the queue, so the backoff a queue earns does not apply. Go back
          # and wait for the connection instead of sleeping out a delay that
          # doubles every time the broker cannot be reached.
          delay = RECOVER_CONSUMER_DELAY
        when :available
          begin
            basic_consume(queue, consumer_tag: tag, manual_ack: entry[:manual_ack], exclusive: entry[:exclusive],
                                 arguments: entry[:arguments] || {}, on_error: entry[:on_error], &entry[:block])
            # Consuming again, so the queue is there and recovery can have it
            # back (#handle_broker_cancel held it).
            topology&.resume_queue(queue)
            @logger.info("Channel #{@channel_id}: re-subscribed consumer #{tag} to #{queue}")
            instrument("consumer.recovered") do
              { channel: @channel_id, consumer_tag: tag, queue: queue, attempts: failures + 1 }
            end
            return
          rescue *PROPAGATE_FROM_HANDLER
            raise
          rescue ConnectionError, NotOpenError => e
            # The connection went, not the subscription. That has its own
            # reporting and its own recovery, both already running, so it is
            # not counted as a failure to re-subscribe: an application alarming
            # on consumer.recovery_failed would otherwise be told twice about
            # one event, and told it in the words of the more serious of the
            # two.
            @logger.debug("Channel #{@channel_id}: re-subscribe of #{tag} interrupted by " \
                          "#{e.class}; waiting for the connection")
          rescue ChannelError => e
            # The queue went between the question and the answer. The broker
            # has closed this channel saying so, so there is nothing left to
            # retry on.
            return forget_deleted_queue(tag, queue) if queue_missing?(e)

            failures += 1
            note_recovery_failure(tag, queue, e, failures)
          rescue => e
            failures += 1
            note_recovery_failure(tag, queue, e, failures)
          end
        end
        # :unavailable falls through to the backoff and is asked again.

        sleep delay
        delay = [delay * 2, RECOVER_CONSUMER_MAX_DELAY].min
      end
    end

    # One line per episode at warn, the rest at debug: a broker that is away
    # for an hour would otherwise write a warning every attempt for the whole
    # hour, and an application counting warnings would read one outage as
    # hundreds.
    def note_recovery_failure(tag, queue, error, failures)
      if failures == 1
        @logger.warn("Channel #{@channel_id}: could not re-subscribe #{tag} to #{queue}, " \
                     "retrying: #{error.class}: #{error.message}")
        instrument("consumer.recovery_failed") do
          { channel: @channel_id, consumer_tag: tag, queue: queue, reason: :error, error: error }
        end
      else
        @logger.debug("Channel #{@channel_id}: re-subscribe attempt #{failures} for #{tag} failed: #{error.message}")
      end
    end

    # The queue behind a cancelled consumer has gone. Drop it from the topology
    # registry, with its bindings, so a later connection recovery does not
    # re-declare a queue somebody deleted on purpose - the "deleted queue comes
    # back" bug the Java client has been fixing instances of for years.
    def forget_deleted_queue(tag, queue)
      topology&.delete_queue(queue)
      @logger.warn("Channel #{@channel_id}: queue #{queue} no longer exists; consumer #{tag} will not be recovered")
      instrument("consumer.recovery_failed") do
        { channel: @channel_id, consumer_tag: tag, queue: queue, reason: :queue_deleted }
      end
      wake_each_waiters(tag)
    end

    # Is the queue gone, there, or merely not being served right now? A passive
    # declare answers it, on a channel of its own because a passive declare
    # that fails closes the channel it ran on - which would take the queue's
    # fellow consumers down with it.
    #
    # Four answers, because conflating any two of them does harm. :deleted must
    # be forgotten and :unavailable must be waited for, and telling those apart
    # is the whole point. :unreachable - no answer at all, because the
    # connection is in trouble - is separated from :unavailable so that the
    # caller does not spend a backoff earned by a queue on a connection that is
    # simply down, and is never taken for :available, because a basic_consume
    # on a queue that is not there closes the channel.
    def queue_status(name)
      @session.with_channel { |probe| probe.queue(name, passive: true) }
      :available
    rescue ChannelError => e
      queue_missing?(e) ? :deleted : :unavailable
    rescue *PROPAGATE_FROM_HANDLER
      raise
    rescue => e
      @logger.debug("Channel #{@channel_id}: could not check whether #{name} still exists: #{e.class}: #{e.message}")
      :unreachable
    end

    # Gone, rather than merely not being served. Taking a queue whose node is
    # down for a deleted one forgets a queue that is coming back: its consumer
    # is never restored, and it is never re-declared either. Any 404 the
    # QUEUE_UNAVAILABLE wordings do not cover reads as a deletion, which is the
    # safe way round - an unmatched wording costs only what forgetting it costs
    # today.
    def queue_missing?(error)
      error.code == 404 && !QUEUE_UNAVAILABLE.match?(error.text.to_s)
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
      timed_out = false
      begin
        @rpc_sem.acquire do
          assert_open!
          send_and_wait(frame, *expected_classes)
        end
      rescue RpcTimeoutError
        timed_out = true
        raise
      ensure
        # Outside the semaphore: the reply we gave up on may still be in
        # flight, and AMQP replies carry nothing to match them to a request,
        # so the next caller on this channel would collect it instead.
        resync_after_rpc_timeout if timed_out
      end
    end

    # Close and reopen the channel after a reply never arrived, discarding
    # anything the broker sends for the abandoned request. Consumers are
    # re-registered, so a timeout costs a round trip rather than the channel's
    # deliveries. Runs outside @rpc_sem (re-registering consumers needs it).
    def resync_after_rpc_timeout
      return if @resyncing
      return unless open?

      @resyncing = true
      # Park everyone else for the duration. The channel is about to be closed:
      # a publish issued now would go out after channel.close and be dropped by
      # the broker without a word, and another request's reply would be thrown
      # away as the late one we are shedding.
      begin_recovery_wait!
      @state = :resyncing
      @logger.warn(
        "Channel #{@channel_id}: reopening after an RPC timeout. A late reply carries nothing " \
        "to match it to its request, so it would be handed to the next caller on this channel."
      )
      instrument("channel.resync") { { channel: @channel_id, reason: :rpc_timeout } }

      consumers = @consumers.dup
      # A promise: resolution is sticky, so a close-ok dispatched while this
      # fiber is still parked inside its own write (full write queue) is kept
      # rather than lost. Registered before the write for the same reason.
      promise = @resync_promise = Async::Promise.new
      @frame_io.write_frame(
        AMQ::Protocol::Channel::Close.encode(@channel_id, 200, "Resynchronising after RPC timeout", 0, 0).encode
      )

      outcome = nil
      begin
        outcome = Async::Task.current.with_timeout(@rpc_timeout || RESYNC_TIMEOUT) { promise.wait }
      rescue Async::TimeoutError
        @logger.error("Channel #{@channel_id}: no channel.close-ok while resynchronising")
        # Deliberately NOT clearing @resync_promise here: the dispatch filter
        # keys on it, and it must keep discarding frames — a very late
        # close-ok above all — until just before the reopen below. Cleared
        # there; the teardown paths (fail_waiters!) clear it themselves.
      end

      # The connection dropped while we waited: mark_recovering! moved the
      # channel to :recovering and woke us (interrupt_wait! → fail_waiters!).
      # The reopen is the recovery loop's job now — it reopens every channel on
      # the new connection and replays the topology first. Racing it from here
      # would set the channel :closed and call reopen_channel against a session
      # that is not open, which raises, drops the channel out of the session,
      # and strands its consumers (they were never cleared, so recovery can
      # still restore them). Leave it be. @consumers is untouched; the dup is
      # moot on this path.
      if recovering? || outcome.is_a?(Exception)
        @logger.info("Channel #{@channel_id}: connection lost during resync; recovery will reopen it")
        return
      end

      @state = :closed
      # The dispatch filter stays on until this point so a close-ok arriving
      # after the timeout is still discarded — otherwise it would resolve the
      # Open's reply promise below (registered before its write) and kill the
      # reopen with "expected OpenOk, got CloseOk".
      @resync_promise = nil
      # @consumers is deliberately NOT cleared here. If the connection drops
      # while reopen_channel below is parked waiting for channel.open-ok, this
      # method unwinds into the rescue with the channel now :recovering; the
      # recovery loop will reopen it and re-register whatever is in @consumers,
      # so emptying it would silently lose every consumer. (The dup is kept for
      # the same reason — see the rescue.)
      @session.reopen_channel(self, state: :resyncing)
      # reopen_on restores confirm mode but deliberately leaves the unconfirmed
      # messages alone: during connection recovery the topology they are
      # addressed to may not be back yet. Here only this channel went, so they
      # go straight out — and this is what resets @delivery_tag, which the
      # broker also restarts at 1 on the reopened channel. Without it the two
      # drift apart and every later confirm resolves the wrong publish.
      republish_unconfirmed if @confirms_enabled
      @state = :open
      @consumers.replace(consumers)
      re_register_consumers
      resume_parked!(:open)
    rescue => e
      # A connection drop during the reopen (after the early-return guard above
      # passed) lands here: the channel is now :recovering, or the session is no
      # longer open. Recovery will reopen this channel and re-register its
      # consumers, so leave it in the table with @consumers intact — restore the
      # dup in case it was mid-replace — rather than marking it closed and
      # dropping it, which would strand the consumers for good.
      if recovering? || !@session.open?
        @consumers.replace(consumers) if @consumers.empty?
        @logger.info("Channel #{@channel_id}: connection lost during resync reopen; recovery will handle it")
        return
      end
      @logger.error("Channel #{@channel_id}: could not resynchronise after an RPC timeout: #{e.class}: #{e.message}")
      mark_closed!(ChannelError.new("Channel could not be resynchronised after an RPC timeout: #{e.message}",
                                    channel_id: @channel_id))
      @session.channel_closed(@channel_id) rescue nil
    ensure
      @resyncing = false
    end

    def send_and_wait(frame, *expected_classes)
      # Register the reply slot BEFORE writing: write_frame yields when the
      # write queue is full, and the reply can be dispatched inside that gap.
      # The promise keeps it for us; a condition registered afterwards lost it,
      # and the caller timed out into a needless resync.
      promise = @reply_promise = Async::Promise.new
      @frame_io.write_frame(frame.encode)
      wait_for_any(promise, *expected_classes)
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
    def encode_publish(payload, exchange:, routing_key:, mandatory: false, persistent: true,
                       content_type: DEFAULT_CONTENT_TYPE, content_encoding: nil, headers: nil, priority: 0,
                       correlation_id: nil, reply_to: nil, expiration: nil, message_id: nil,
                       timestamp: nil, type: nil, user_id: nil, app_id: nil, properties: {})
      payload_bytes = payload.is_a?(String) ? payload.b : payload
      props = {
        delivery_mode: persistent ? 2 : 1,
        content_type:  content_type || DEFAULT_CONTENT_TYPE,
        priority:      priority || 0
      }
      props[:content_encoding] = content_encoding if content_encoding
      props[:headers]          = headers          if headers
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
    def reserve_confirm_tag(bytes, payload = nil, context = nil)
      @delivery_tag += 1
      # Snapshot the payload: the caller may reuse or mutate its string after
      # publishing (publish_context copies headers for the same reason), and
      # everything that reads this entry later — a replay re-encoded after a
      # queue rename or a frame_max change, #unconfirmed_messages handed out on
      # a node failure — must see the bytes as they were sent, not as the
      # buffer looks now. Frozen strings skip the copy.
      payload = payload.dup if payload.is_a?(String) && !payload.frozen?
      @pending_confirms[@delivery_tag] = [bytes, payload, context]
      # The encoded frames carry this channel's id, so they can only be replayed
      # on this channel. Keep the payload and where it was going alongside, so a
      # caller handed these on a node failure can republish them somewhere else
      # (see #unconfirmed_messages). A pair, not a record: this runs for every
      # confirmed publish, and building the record here cost a quarter of the
      # send throughput. #unconfirmed_messages builds them on read instead,
      # which only happens when a node has actually gone.
      @delivery_tag
    end

    # One per publish call. The caller keeps its headers hash and may go on
    # mutating it after the publish returns, so copy that too rather than only
    # the options around it.
    def publish_context(exchange, routing_key, options)
      copy = options ? options.dup : {}
      copy[:headers] = copy[:headers].dup if copy[:headers].is_a?(Hash)
      copy[:properties] = copy[:properties].dup if copy[:properties].is_a?(Hash)
      PublishContext.new(exchange, routing_key, copy).freeze
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
      pending.each do |entry|
        # Re-encode from the kept payload and context instead of replaying the
        # bytes recorded at publish time: the context may have been rewritten
        # by a server-named queue rename, and the reconnect may have negotiated
        # a different frame_max, so stale frames could be mis-addressed or
        # over-sized. This is the rare recovery path; the per-publish hot path
        # still records the pre-encoded bytes untouched.
        bytes = if (ctx = entry[2])
          encode_publish(entry[1], exchange: ctx.exchange, routing_key: ctx.routing_key, **ctx.options)
        else
          entry[0]
        end
        @frame_io.write_frame(bytes, publish: true)
        @delivery_tag += 1
        @pending_confirms[@delivery_tag] = [bytes, entry[1], entry[2]]
      end
      @logger.info("Channel #{@channel_id}: re-published #{pending.size} unconfirmed message(s) after recovery") unless pending.empty?
    end

    # Park until the current episode resolves. The promise is a latch: a waiter
    # arriving after resolution returns immediately with the same outcome, so
    # there is no window between "state says recovering" and "the wait is
    # registered" for a resume to slip through. Raises the teardown error when
    # the episode ended in mark_closed!.
    def wait_for_recovery!
      promise = (@recovery_promise ||= Async::Promise.new)
      promise.wait # :open, or raises the rejection from resume_parked!
    end

    def resume_parked!(outcome)
      promise = @recovery_promise
      return unless promise
      # Keep the resolved promise in place as the latch for late arrivals; the
      # next episode replaces it via begin_recovery_wait!.
      outcome.is_a?(Exception) ? promise.reject(outcome) : promise.resolve(outcome)
    end

    # Wait on the reply promise +send_and_wait+ registered BEFORE it wrote the
    # request. The promise latches, so a reply the dispatch fiber delivers
    # while the writer is still parked on a full write queue — a yield point
    # between send and wait under a Condition — is kept, not dropped.
    def wait_for_any(promise, *expected_classes)
      result = wait_with_timeout(promise) do
        @reply_promise = nil if @reply_promise.equal?(promise)
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

    # Re-subscribe every consumer after a reopen. A consumer whose queue is
    # gone draws a 404, and the broker closes the whole channel on it — taking
    # down every consumer already re-registered on it, which were cancelled
    # server-side by that same close. So a ChannelError cannot just be logged
    # and skipped (the old behaviour: the first bad queue left the channel
    # closed and dropped out of the session, so the rest raised NotOpenError
    # and no consumer was restored, while recovery still reported success).
    # Instead, on a ChannelError the offending consumer is dropped, the channel
    # reopened, and the survivors re-subscribed. The dropped set bounds it, so a
    # queue that stays gone is dropped once, not retried forever.
    #
    # Any OTHER error — the connection dropping again mid-recovery
    # (ConnectionError from interrupt_wait!), the session going away
    # (NotOpenError), an RPC timeout — is not this consumer's fault. It aborts
    # the pass and leaves @consumers untouched, so the next recovery cycle
    # restores every consumer; dropping them here would be silent, permanent
    # loss on a connection flap, with recovery still reporting success.
    def re_register_consumers
      return if @consumers.empty?

      dropped = {}
      # A ChannelError drops exactly one consumer (recorded in `dropped` and
      # skipped next pass) or the pass completes, so this runs at most once per
      # consumer — no spin even if the reopen itself keeps raising ChannelError.
      restart = true
      while restart
        restart = false
        aborted = catch(:abort_reregister) do
          @consumers.keys.each do |tag|
            next if dropped.key?(tag)
            entry = @consumers[tag]
            begin
              reopen_for_reregister! unless open?
              basic_consume(entry[:queue_name], consumer_tag: tag, manual_ack: entry[:manual_ack],
                            exclusive: entry[:exclusive], arguments: entry[:arguments] || {},
                            on_error: entry[:on_error], &entry[:block])
            rescue ChannelError => e
              dropped[tag] = e
              @logger.error("Channel #{@channel_id}: consumer #{tag} on #{entry[:queue_name]} rejected (#{e.message}); dropping it")
              # The broker closed the channel rejecting this one, cancelling the
              # consumers already restored on it. Restart to re-subscribe them.
              # If it is somehow still open, the earlier subscriptions stand, so
              # carry on to the next consumer.
              if !open?
                restart = true
                break
              end
            rescue => e
              @logger.warn("Channel #{@channel_id}: consumer re-registration interrupted (#{e.class}: #{e.message}); leaving consumers for the next recovery")
              throw :abort_reregister, true
            end
          end
          false
        end
        return if aborted
      end

      # Forget the ones the broker will not serve, and release anyone in #each
      # waiting on them: they are not coming back on their own.
      dropped.each_key do |tag|
        cancelled = @consumers.delete(tag)
        topology&.delete_consumer(tag)
        wake_each_waiters(tag)
        instrument("consumer.recovery_failed") do
          { channel: @channel_id, consumer_tag: tag, queue: cancelled&.dig(:queue_name), reason: :queue_deleted }
        end
      end
    end

    # Reopen the channel in place during consumer re-registration, after the
    # broker closed it rejecting a consumer. Keeps the same id; restores
    # prefetch, confirm and tx mode (reopen_on does). Leaves the channel :open.
    def reopen_for_reregister!
      @session.reopen_channel(self, state: :open)
    end

    # Raise unless the channel is open. While the connection is being recovered
    # the caller is parked instead and resumes once the channel is reopened, so
    # publishes and RPCs issued during an outage neither fail nor vanish; if
    # recovery is abandoned they raise the final error.
    def assert_open!
      wait_for_recovery! while recovering? || resyncing?
      raise NotOpenError, "Channel #{@channel_id} is not open" unless open?
    end

    def validate_pool_size!(n)
      raise ArgumentError, "pool_size must be a positive Integer (got #{n.inspect})" unless n.is_a?(Integer) && n > 0
      n
    end
  end
end
