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
    attr_reader :channel_id, :pool_size

    # +pool_size+ bounds the number of consumer-handler fibers that can run
    # concurrently on this channel (Bunny-parity: default 1 = serialized).
    # Resizable at runtime via #pool_size=; basic_qos adjusts it automatically
    # when prefetch_count > 0 so the two stay coupled.
    def initialize(channel_id, session, frame_io, frame_max:, logger:, pool_size: 1)
      @channel_id   = channel_id
      @session      = session
      @frame_io     = frame_io
      @frame_max    = frame_max
      @logger       = logger

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
      @confirm_condition = nil
      @flow_active       = true
      @on_cancel         = nil
      @on_error          = nil
      @mutex             = Async::Semaphore.new(1)
      @reply_condition   = nil
      @content_condition = nil
      @pending_content   = nil
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

    def open
      @queue = @frame_io.register_channel(@channel_id)
      # Start dispatch task BEFORE waiting so it can process the OpenOk reply.
      start_dispatch_task
      @frame_io.write_frame(AMQ::Protocol::Channel::Open.encode(@channel_id, "").encode)
      wait_for(:channel_open_ok, AMQ::Protocol::Channel::OpenOk)
      @state = :open
      self
    end

    def close
      return unless open?
      @frame_io.write_frame(
        AMQ::Protocol::Channel::Close.encode(@channel_id, 200, "Goodbye", 0, 0).encode
      )
      wait_for(:channel_close_ok, AMQ::Protocol::Channel::CloseOk)
      @state = :closed
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
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Queue::Declare.encode(
          @channel_id, name, passive, durable, exclusive, auto_delete, false, arguments
        ).encode
      )
      resp = wait_for(:queue_declare_ok, AMQ::Protocol::Queue::DeclareOk)
      Queue.new(resp.queue, resp.message_count, resp.consumer_count, self,
                durable: durable, exclusive: exclusive, auto_delete: auto_delete)
    end

    def temporary_queue(**opts)
      queue("", exclusive: true, auto_delete: true, **opts)
    end

    def quorum_queue(name, **opts)
      args = { "x-queue-type" => "quorum" }.merge(opts.delete(:arguments) || {})
      queue(name, durable: true, arguments: args, **opts)
    end

    def queue_delete(name, if_unused: false, if_empty: false)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Queue::Delete.encode(@channel_id, name, if_unused, if_empty, false).encode
      )
      wait_for(:queue_delete_ok, AMQ::Protocol::Queue::DeleteOk)
    end

    def queue_purge(name)
      assert_open!
      @frame_io.write_frame(AMQ::Protocol::Queue::Purge.encode(@channel_id, name, false).encode)
      wait_for(:queue_purge_ok, AMQ::Protocol::Queue::PurgeOk)
    end

    def queue_bind(queue_name, exchange:, routing_key: "", arguments: {})
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Queue::Bind.encode(
          @channel_id, queue_name, exchange, routing_key, false, arguments
        ).encode
      )
      wait_for(:queue_bind_ok, AMQ::Protocol::Queue::BindOk)
    end

    def queue_unbind(queue_name, exchange:, routing_key: "", arguments: {})
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Queue::Unbind.encode(
          @channel_id, queue_name, exchange, routing_key, arguments
        ).encode
      )
      wait_for(:queue_unbind_ok, AMQ::Protocol::Queue::UnbindOk)
    end

    # -------------------------------------------------------------------------
    # Exchange
    # -------------------------------------------------------------------------

    def exchange(name, type: :direct, passive: false, durable: false, auto_delete: false, internal: false, arguments: {})
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Exchange::Declare.encode(
          @channel_id, name, type.to_s, passive, durable, auto_delete, internal, false, arguments
        ).encode
      )
      wait_for(:exchange_declare_ok, AMQ::Protocol::Exchange::DeclareOk)
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
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Exchange::Delete.encode(@channel_id, name, if_unused, false).encode
      )
      wait_for(:exchange_delete_ok, AMQ::Protocol::Exchange::DeleteOk)
    end

    def exchange_bind(destination:, source:, routing_key: "", arguments: {})
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Exchange::Bind.encode(
          @channel_id, destination, source, routing_key, false, arguments
        ).encode
      )
      wait_for(:exchange_bind_ok, AMQ::Protocol::Exchange::BindOk)
    end

    def exchange_unbind(destination:, source:, routing_key: "", arguments: {})
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Exchange::Unbind.encode(
          @channel_id, destination, source, routing_key, false, arguments
        ).encode
      )
      wait_for(:exchange_unbind_ok, AMQ::Protocol::Exchange::UnbindOk)
    end

    # -------------------------------------------------------------------------
    # Basic operations
    # -------------------------------------------------------------------------

    def basic_publish(payload, exchange: "", routing_key: "", mandatory: false, persistent: false,
                      content_type: nil, content_encoding: nil, headers: nil, priority: nil,
                      correlation_id: nil, reply_to: nil, expiration: nil, message_id: nil,
                      timestamp: nil, type: nil, user_id: nil, app_id: nil, properties: {})
      assert_open!
      payload_bytes = payload.is_a?(String) ? payload.b : payload
      delivery_mode = persistent ? 2 : 1
      props         = { delivery_mode: delivery_mode }
      props[:content_type]     = content_type     if content_type
      props[:content_encoding] = content_encoding if content_encoding
      props[:headers]          = headers          if headers
      props[:priority]         = priority         if priority
      props[:correlation_id]   = correlation_id   if correlation_id
      props[:reply_to]         = reply_to         if reply_to
      props[:expiration]       = expiration        if expiration
      props[:message_id]       = message_id       if message_id
      props[:timestamp]        = timestamp        if timestamp
      props[:type]             = type             if type
      props[:user_id]          = user_id          if user_id
      props[:app_id]           = app_id           if app_id
      props.merge!(properties)

      # Basic::Publish.encode returns [MethodFrame, HeaderFrame, BodyFrame, ...],
      # splitting payload across multiple body frames when needed.
      frames = AMQ::Protocol::Basic::Publish.encode(
        @channel_id, payload_bytes, props, exchange, routing_key, mandatory, false, @frame_max
      )
      frames.each { |f| @frame_io.write_frame(f.encode, publish: true) }

      if @confirms_enabled
        @mutex.acquire do
          @delivery_tag += 1
          tag = @delivery_tag
          @pending_confirms[tag] = tag
          tag
        end
      end
    end

    # Duck-typed #write for stream composability.
    alias write basic_publish

    def basic_get(queue_name, manual_ack: false)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Get.encode(@channel_id, queue_name, !manual_ack).encode
      )
      msg = wait_for_any(:basic_get_ok, :basic_get_empty,
                         AMQ::Protocol::Basic::GetOk,
                         AMQ::Protocol::Basic::GetEmpty)
      return nil if msg.is_a?(AMQ::Protocol::Basic::GetEmpty)

      # After GetOk, next message pair is content-header + body
      content = wait_content
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
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Qos.encode(@channel_id, prefetch_size, prefetch_count, global).encode
      )
      wait_for(:basic_qos_ok, AMQ::Protocol::Basic::QosOk)
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
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Consume.encode(
          @channel_id, queue_name, consumer_tag, false, !manual_ack, exclusive, false, arguments
        ).encode
      )
      resp = wait_for(:basic_consume_ok, AMQ::Protocol::Basic::ConsumeOk)
      @consumers[resp.consumer_tag] = { queue_name: queue_name, block: block, manual_ack: manual_ack }
      resp.consumer_tag
    end

    def basic_cancel(consumer_tag)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Cancel.encode(@channel_id, consumer_tag, false).encode
      )
      wait_for(:basic_cancel_ok, AMQ::Protocol::Basic::CancelOk)
      @consumers.delete(consumer_tag)
      wake_each_waiters(consumer_tag)
    end

    # Ask the broker to redeliver all unacknowledged messages on this channel.
    # RabbitMQ only supports requeue: true; requeue: false raises a channel error.
    def basic_recover(requeue: true)
      assert_open!
      @frame_io.write_frame(
        AMQ::Protocol::Basic::Recover.encode(@channel_id, requeue).encode
      )
      wait_for(:basic_recover_ok, AMQ::Protocol::Basic::RecoverOk)
    end

    # -------------------------------------------------------------------------
    # Publisher confirms
    # -------------------------------------------------------------------------

    def confirm_select
      assert_open!
      @frame_io.write_frame(AMQ::Protocol::Confirm::Select.encode(@channel_id, false).encode)
      wait_for(:confirm_select_ok, AMQ::Protocol::Confirm::SelectOk)
      @confirms_enabled  = true
      @delivery_tag      = 0
      @pending_confirms  = {}
      @nacked_tags       = []
      @only_acks         = true
      @confirm_condition = Async::Condition.new
    end

    # Block the current fiber until every published message has been acked or
    # nacked by the broker. Returns true if all of them were acked since the
    # previous call, false if at least one was nacked (see #nacked_tags).
    # Raises ConnectionError if the session disconnects while waiting.
    def wait_for_confirms
      until @pending_confirms.empty?
        @confirm_condition.wait
        raise ConnectionError, "Session disconnected while waiting for confirms" unless open?
      end
      result     = @only_acks
      @only_acks = true
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
      assert_open!
      @frame_io.write_frame(AMQ::Protocol::Channel::Flow.encode(@channel_id, active).encode)
      wait_for(:channel_flow_ok, AMQ::Protocol::Channel::FlowOk)
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
    # Internal: called by Session when connection dies during recovery
    # to unblock any fibers waiting on reply or content conditions.
    # -------------------------------------------------------------------------

    def interrupt_wait!(error = nil)
      error ||= ConnectionError.new(code: 0, text: "Connection lost during recovery")
      @reply_condition&.signal(error)
      @reply_condition   = nil
      @content_condition&.signal(error)
      @content_condition = nil
      @confirm_condition&.signal(error)
      @confirm_condition = nil
      @queue&.push(nil) rescue nil
    rescue => e
      # ignore — best-effort unblock
    end

    # -------------------------------------------------------------------------
    # Internal: called by Session after reconnect
    # -------------------------------------------------------------------------

    def reopen_after_recovery(new_frame_io)
      @frame_io = new_frame_io
      @queue    = @frame_io.register_channel(@channel_id)
      start_dispatch_task
      @frame_io.write_frame(AMQ::Protocol::Channel::Open.encode(@channel_id, "").encode)
      wait_for(:channel_open_ok, AMQ::Protocol::Channel::OpenOk)
      @state = :open

      if @confirms_enabled
        @frame_io.write_frame(AMQ::Protocol::Confirm::Select.encode(@channel_id, false).encode)
        wait_for(:confirm_select_ok, AMQ::Protocol::Confirm::SelectOk)
        # Reset confirms state for the new connection — old pending tags are gone
        # and the old condition object is stale (any prior waiter already unblocked
        # via interrupt_wait! with a ConnectionError).
        @confirm_condition = Async::Condition.new
        @pending_confirms  = {}
        @delivery_tag      = 0
      end

      re_register_consumers
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
        @pending_content = { header: rest[0], body: rest[1] }
        @content_condition&.signal(@pending_content)
        @content_condition = nil
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
          Async { @pool_sem.acquire { entry[:block].call(method, header, body) } }
        else
          @logger.warn("Delivery on channel #{@channel_id} for unknown consumer #{method.consumer_tag}")
        end

      when AMQ::Protocol::Basic::Return
        # Same pattern: pop content directly.
        content_msg = @queue.pop
        _, header, body = content_msg
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
        if entry
          @logger.warn("Channel #{@channel_id}: broker cancelled consumer #{method.consumer_tag}")
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
        @only_acks = false
        @confirm_condition&.signal
      end
    end

    def handle_channel_close(method)
      code = method.reply_code
      text = method.reply_text
      @frame_io.write_frame(
        AMQ::Protocol::Channel::CloseOk.encode(@channel_id).encode
      )
      @state = :closed
      @session.channel_closed(@channel_id)
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

    def wait_for(_name, expected_class)
      condition = @reply_condition = Async::Condition.new
      result    = condition.wait

      if result.is_a?(ChannelError) || result.is_a?(ConnectionError)
        raise result
      end

      unless result.is_a?(expected_class)
        raise ChannelError.new("Expected #{expected_class} but got #{result.class}",
                               channel_id: @channel_id)
      end
      result
    end

    def wait_for_any(_name1, _name2, *expected_classes)
      condition = @reply_condition = Async::Condition.new
      result    = condition.wait

      if result.is_a?(ChannelError) || result.is_a?(ConnectionError)
        raise result
      end

      unless expected_classes.any? { |c| result.is_a?(c) }
        raise ChannelError.new("Unexpected method #{result.class}", channel_id: @channel_id)
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
      condition.wait
    end

    def re_register_consumers
      @consumers.each do |tag, entry|
        basic_consume(
          entry[:queue_name],
          consumer_tag: tag,
          manual_ack:   entry[:manual_ack],
          &entry[:block]
        ) rescue nil
      end
    end

    def assert_open!
      raise NotOpenError, "Channel #{@channel_id} is not open" unless open?
    end

    def validate_pool_size!(n)
      raise ArgumentError, "pool_size must be a positive Integer (got #{n.inspect})" unless n.is_a?(Integer) && n > 0
      n
    end
  end
end
