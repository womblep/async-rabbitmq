require "async"
require "async/queue"

module AsyncRabbitMQ
  # A consumer you pull from. #each hands the block one Message at a time, in
  # the fiber that called it, and the block settles each one itself:
  #
  #   consumer = queue.consumer(prefetch: 10)
  #   consumer.each do |message|
  #     process(message.body)
  #     message.ack
  #   rescue InvalidOrder
  #     message.reject(requeue: false)
  #   end
  #
  # Nothing is settled for you. A block that returns without calling #ack,
  # #nack or #reject leaves the message unacknowledged, which is how a message
  # is handed to another fiber to finish later. An exception out of the block
  # ends the loop and is raised from #each, with the message it was holding
  # still unacknowledged; the broker requeues it when the channel closes.
  #
  # The consumer is always manual-ack, and +prefetch+ is what bounds it: the
  # broker sends no more than that many unsettled messages, so that is the
  # most that can ever be waiting here. It is set with basic.qos, which is a
  # setting of the channel, so it also applies to any consumer started on the
  # same channel afterwards. Without it, and with no prefetch already set on
  # the channel, the broker sends the whole queue.
  #
  # #each returns when the consumer goes away, and says why:
  #
  # [:stopped]   #stop was called.
  # [:cancelled] the broker cancelled the consumer (the queue was deleted).
  # [:closed]    the channel was closed, by the application or the broker.
  #
  # A +break+ out of the block ends it too (and, being a break, makes #each
  # return nil).
  #
  # However it ends - a return, an exception, the calling task being stopped -
  # the consumer is cancelled and the messages that had arrived but had not
  # yet been handed to the block are put back on the queue.
  #
  # A connection loss with automatic recovery is not an ending. The consumer
  # is registered again on the new connection and #each keeps waiting.
  # Messages that were waiting here when the connection went are dropped
  # without being yielded: the broker has requeued them and sends them again.
  #
  # For handlers that run concurrently, are acked when they return and nacked
  # when they raise, see ManagedConsumer, which is built on this.
  class Consumer
    # Pushed onto the buffer to end #each. Compared by identity.
    ENDED = Object.new.freeze
    private_constant :ENDED

    # One #each call's state. Late deliveries from an earlier subscription
    # hold the earlier object, so they can never land in a later one's buffer.
    Subscription = Struct.new(:buffer, :open, :ended_because)
    private_constant :Subscription

    # What the channel signals when the consumer is gone for good.
    Watcher = Struct.new(:consumer, :subscription) do
      def signal(*)
        consumer.__send__(:consumer_ended, subscription)
      end
    end
    private_constant :Watcher

    attr_reader :channel, :queue_name

    # The tag of the running consumer; nil when #each is not running.
    attr_reader :consumer_tag

    # +prefetch+ is the most unsettled messages the broker will send at once
    # (see the class comment); nil leaves the channel's setting as it is.
    # +exclusive+, +arguments+ and +consumer_tag+ are passed to basic.consume.
    def initialize(channel, queue_name, prefetch: nil, exclusive: false, arguments: {}, consumer_tag: "")
      unless prefetch.nil? || (prefetch.is_a?(Integer) && prefetch.positive?)
        raise ArgumentError, "prefetch must be a positive Integer or nil (got #{prefetch.inspect})"
      end

      @channel      = channel
      @queue_name   = queue_name.respond_to?(:name) ? queue_name.name : queue_name
      @prefetch     = prefetch
      @exclusive    = exclusive
      @arguments    = arguments
      @requested_tag = consumer_tag
      @subscription = nil
      @consumer_tag = nil
      @stopping     = false
    end

    # True while #each is running.
    def running?
      !@subscription.nil?
    end

    # Consume, yielding one Message at a time to the block in the calling
    # fiber, until the consumer goes away. Returns :stopped, :cancelled or
    # :closed; see the class comment.
    def each
      raise ArgumentError, "Consumer#each needs a block" unless block_given?
      raise Error, "This consumer is already running on #{@queue_name}" if @subscription

      subscription = @subscription = Subscription.new(Async::Queue.new, true, nil)
      @stopping = false
      subscribe(subscription)

      loop do
        # Checked before the wait as well as after: a stop requested while the
        # block ran must not go on to hand out what is already waiting. Nor
        # is there any point handing out a message from a channel that has
        # closed, since it can no longer be settled; a consumer the broker
        # cancelled is different, and what it already delivered is still good.
        break if @stopping || subscription.ended_because == :closed

        message = subscription.buffer.dequeue
        break if message.equal?(ENDED)

        if @stopping
          requeue(message)
          break
        end
        # Delivered on a connection that has since been replaced. The broker
        # requeued it and will send it again; this copy cannot be settled.
        next if message.stale?

        yield message
      end
      subscription.ended_because || :stopped
    ensure
      finish(subscription) if subscription
    end

    # Ask a running #each to end. It returns :stopped once the block has
    # finished with the message it is on; anything waiting behind that is put
    # back on the queue. Safe to call from any fiber, and when not running.
    def stop
      return unless (subscription = @subscription)

      @stopping = true
      subscription.buffer.push(ENDED)
      nil
    end

    def inspect
      "#<#{self.class.name} queue=#{@queue_name.inspect} consumer_tag=#{@consumer_tag.inspect} " \
        "#{running? ? 'running' : 'idle'}>"
    end

    private

    def subscribe(subscription)
      @channel.basic_qos(prefetch_count: @prefetch) if @prefetch
      channel = @channel
      @consumer_tag = channel.basic_consume(@queue_name, consumer_tag: @requested_tag, manual_ack: true,
                                            exclusive: @exclusive, arguments: @arguments) do |info, header, body|
        message = Message.new(channel, info, header, body)
        # A delivery can still be on its way to this block after the consumer
        # was cancelled. Nobody will dequeue it, so it goes straight back.
        subscription.open ? subscription.buffer.push(message) : requeue(message)
      end
      @channel.watch_consumer(@consumer_tag, Watcher.new(self, subscription))
    end

    # The channel's signal that this consumer will not deliver again.
    def consumer_ended(subscription)
      return unless subscription.open

      subscription.ended_because ||= @channel.closed? ? :closed : :cancelled
      subscription.buffer.push(ENDED)
    end

    # Cancel the consumer and put back whatever arrived and was not handed
    # out. Runs on every way out of #each, a stop of the calling task
    # included, so nothing here may assume the channel is still usable.
    def finish(subscription)
      subscription.open = false
      tag = @consumer_tag
      if tag
        @channel.unwatch_consumer(tag)
        begin
          # Not only when the channel is open. While the connection is being
          # recovered the consumer is still on the channel's books, and
          # recovery would register it again with nobody left to read from
          # it: every message would be put straight back and sent straight
          # back, for ever. basic_cancel waits the recovery out and then
          # cancels, so on that path this does not return until the
          # connection is back or has been given up on.
          @channel.basic_cancel(tag) if !@channel.closed? && @channel.consumer?(tag)
        rescue StandardError => e
          @channel.logger.debug(self, "Consumer #{tag} on #{@queue_name}: cancel failed while stopping: #{e.class}: #{e.message}")
        end
      end
      until subscription.buffer.empty?
        message = subscription.buffer.dequeue
        requeue(message) unless message.equal?(ENDED)
      end
    ensure
      @subscription = nil if @subscription.equal?(subscription)
      @consumer_tag = nil
      @stopping     = false
    end

    # Put an undelivered message back. Returns true, so it reads in a
    # condition. A channel that has gone has already requeued it.
    def requeue(message)
      message.nack(requeue: true)
      true
    rescue StandardError
      true
    end
  end
end
