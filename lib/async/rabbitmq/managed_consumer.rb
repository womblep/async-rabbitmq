# frozen_string_literal: true

require "async"
require "async/condition"
require "async/promise"
require "async/queue"
require "console"

require_relative "error"
require_relative "consumer"

module Async
  module RabbitMQ
    # A consumer that runs your handler and looks after the rest: how many
    # run at once, what happens to a message whose handler returned or
    # raised, and stopping without dropping work on the floor.
    #
    #   consumer = client.managed_consumer("orders", concurrency: 10)
    #   consumer.run do |message|
    #     process(message.body)
    #   end
    #
    # #run blocks until the consumer goes away. Handlers run in fibers of
    # their own, up to +concurrency+ at a time. By default a handler that
    # returns has its message acked, and one that raises has it nacked
    # without requeue - dead-lettered if the queue has a dead-letter
    # exchange, discarded if not - and the consumer carries on with the next
    # message. A handler's exception never ends #run.
    #
    # A handler may settle the message itself; whatever it did stands, and
    # nothing further is sent for that message.
    #
    # Options, besides those of Consumer:
    #
    # [concurrency]    handlers running at once. Default 1.
    # [prefetch]       unsettled messages the broker sends at once. Defaults
    #                  to +concurrency+, and is what concurrency is really
    #                  bounded by: a prefetch below it caps the handlers at
    #                  the prefetch.
    # [ack_on_success] ack when the handler returns. Default true. With
    #                  false, a handler that returns without settling leaves
    #                  the message unacknowledged, to be settled later by
    #                  whoever holds it.
    # [exception]      what to do with the message when the handler raises:
    #                  :ack, :nack or :reject. Default :nack.
    # [requeue]        for :nack and :reject, whether the broker puts the
    #                  message back on the queue. Default false: a message
    #                  that raises once will very likely raise again, and
    #                  requeued it would come straight back.
    # [drain_timeout]  seconds #run waits, when stopping, for handlers
    #                  already running. Default 30; nil waits for as long as
    #                  they take.
    #
    # Stopping is graceful, whether by #stop, by #close, or by stopping the
    # task that is running #run (which is what a shutdown of the reactor
    # does). The consumer is cancelled so nothing new arrives, messages that
    # had arrived but not started are put back on the queue, and handlers
    # already running are given +drain_timeout+ to finish and settle
    # normally. Any still running after that are stopped and their messages
    # put back on the queue. So a stop can take up to +drain_timeout+ to
    # return, and never loses a message.
    #
    # A handler failure is logged, and passed to the #on_error block if there
    # is one.
    class ManagedConsumer
      EXCEPTION_SETTLEMENTS = %i[ack nack reject].freeze

      attr :concurrency, :prefetch, :drain_timeout

      def initialize(client, queue = nil, concurrency: 1, prefetch: nil, ack_on_success: true,
                     exception: :nack, requeue: false, drain_timeout: 30, **consumer_options)
        unless concurrency.is_a?(Integer) && concurrency.positive?
          raise ArgumentError, "concurrency must be a positive Integer (got #{concurrency.inspect})"
        end
        unless EXCEPTION_SETTLEMENTS.include?(exception)
          raise ArgumentError, "exception must be :ack, :nack or :reject (got #{exception.inspect})"
        end
        unless drain_timeout.nil? || (drain_timeout.is_a?(Numeric) && !drain_timeout.negative?)
          raise ArgumentError, "drain_timeout must be nil or a non-negative number (got #{drain_timeout.inspect})"
        end

        @concurrency    = concurrency
        @prefetch       = prefetch || concurrency
        @ack_on_success = ack_on_success ? true : false
        @exception      = exception
        @requeue        = requeue ? true : false
        @drain_timeout  = drain_timeout
        @on_error       = nil
        @workers        = nil
        @done           = nil # Promise resolved when the running #run has drained
        @fiber          = nil
        @closed         = false
        # Registered here and not by the Consumer: closing the client has to
        # drain the handlers before the channel they settle on is closed.
        @consumer = Consumer.new(client, queue, prefetch: @prefetch, link: self, **consumer_options)
      end

      # The client the consumer is attached to. In a Cluster it can change.
      def client = @consumer.client
      def on_connection_loss = @consumer.on_connection_loss

      # The underlying Consumer's: see there.
      def queue_name = @consumer.queue_name
      def exclusive? = @consumer.exclusive?
      def consumer_tag = @consumer.consumer_tag
      def running? = !@workers.nil?
      def closed? = @closed

      def bind(exchange, **options)
        @consumer.bind(exchange, **options)
        self
      end

      def unbind(exchange, **options)
        @consumer.unbind(exchange, **options)
        self
      end

      def on_attach(&block)
        consumer = self
        @consumer.on_attach { block.call(consumer) }
        self
      end

      def ack_on_success?
        @ack_on_success
      end

      # Register a block called with (error, message) when a handler raises,
      # before the message is settled. It may settle the message itself,
      # which then stands in place of the +exception:+ setting.
      def on_error(&block)
        @on_error = block
        self
      end

      # Consume, running +handler+ for each message, until the consumer goes
      # away. Returns why, as Consumer#each does. Running handlers are
      # drained before it returns; see the class comment.
      def run(&handler)
        raise ArgumentError, "ManagedConsumer#run needs a block" unless handler
        raise ClosedError, "This consumer has been closed" if @closed
        raise Error, "This consumer is already running on #{queue_name}" if @workers

        # Handlers run in a pool of fibers that is grown as it is needed, up
        # to +concurrency+, and reused. The pool pulls from a queue of its
        # own which this fiber does nothing but fill. Two cheaper-looking
        # designs were measured and lost: a fiber per message costs more to
        # start than a small message costs to handle, and handing messages
        # over one at a time, waiting for a free slot each time, switches
        # fibers for every message. Nor is the pool an Async::Barrier, which
        # keeps every task it ever started until it is waited on, and this
        # runs for the life of the process.
        #
        # The queue needs no bound of its own: the broker sends at most
        # +prefetch+ unsettled messages, and every message in it is one.
        @workers  = []
        @jobs     = Async::Queue.new
        @busy     = 0 # handlers running right now
        @idle     = Async::Condition.new
        @draining = false
        @done     = Async::Promise.new
        @fiber    = Fiber.current
        begin
          result = @consumer.each do |message|
            @jobs.push(message)
            start_worker(handler) if @workers.size < @concurrency && @jobs.size > @workers.size - @busy
          end
          # A close stops the consumer first, to drain, and closes it after.
          @closed && result == :stopped ? :closed : result
        ensure
          begin
            drain
          ensure
            @workers = nil
            @fiber   = nil
            done, @done = @done, nil
            done&.resolve(true)
          end
        end
      end

      # Ask a running #run to end gracefully. Returns at once; #run returns
      # when the handlers have drained. Safe from any fiber, and when not
      # running.
      def stop
        @consumer.stop
      end

      # Close the consumer: stop #run if it is running, wait for its handlers
      # to drain, and close the channel.
      def close
        return if @closed

        @closed = true
        stop
        done = @done
        done.wait if done && @consumer.consumer_tag && !@fiber.equal?(Fiber.current)
        @consumer.close
        nil
      end

      def inspect
        "#<#{self.class.name} queue=#{queue_name.inspect} concurrency=#{@concurrency} " \
          "#{running? ? 'running' : 'idle'}>"
      end

      private

      def start_worker(handler)
        workers = @workers
        jobs    = @jobs
        Async do |task|
          workers << task
          while (message = jobs.dequeue)
            # Taken off the queue just as the consumer began to stop: it has
            # not started, so it goes back like the rest that had not.
            next requeue(message) if @draining
            # Waiting here when the connection went. The broker took it back
            # and will send it again; handling this copy would do the work
            # twice, and the first time could not even be acknowledged.
            next if message.stale?

            @busy += 1
            begin
              handle(message, handler)
            ensure
              @busy -= 1
              @idle.signal if @busy.zero?
            end
          end
        ensure
          # Stopped, or ended by something a handler must not swallow. Off
          # the books either way, so the pool starts another if it needs one.
          workers.delete(task)
        end
      end

      def handle(message, handler)
        finished = false
        begin
          handler.call(message)
          settle(message, :ack) if @ack_on_success
        rescue StandardError, ScriptError, SystemStackError => error
          failed(error, message)
        end
        finished = true
      ensure
        # The handler was stopped part way: the drain ran out of patience, or
        # the task was stopped from outside. The work was not done.
        requeue(message) unless finished
      end

      def failed(error, message)
        Console.error(self, "Handler for #{queue_name} raised; message will be #{disposition_text}",
                      exception: error)
        begin
          @on_error&.call(error, message)
        rescue StandardError => hook_error
          Console.error(self, "on_error for #{queue_name} raised", exception: hook_error)
        end
        settle(message, @exception)
      end

      def disposition_text
        return "acked" if @exception == :ack

        "#{@exception == :nack ? 'nacked' : 'rejected'} #{@requeue ? 'and requeued' : 'without requeue'}"
      end

      # Settle unless the handler already did. A message whose channel has
      # gone settles to nothing: the broker requeued it.
      def settle(message, how)
        return if message.settled?

        case how
        when :ack    then message.ack
        when :nack   then message.nack(requeue: @requeue)
        when :reject then message.reject(requeue: @requeue)
        end
      rescue StandardError => error
        Console.debug(self, "Could not #{how} a message from #{queue_name}", exception: error)
      end

      def requeue(message)
        message.nack(requeue: true) unless message.settled?
      rescue StandardError => error
        Console.debug(self, "Could not requeue a message from #{queue_name}", exception: error)
      end

      # By now the consumer is cancelled and what it had not handed over is
      # back on the queue (Consumer#each did that on its way out). What is
      # left is what the pool holds: messages waiting for a handler, which go
      # back now, and handlers already running, which are waited for.
      def drain
        @draining = true
        requeue(@jobs.dequeue) until @jobs.empty?
        return if @busy.zero?

        begin
          if @drain_timeout
            Async::Task.current.with_timeout(@drain_timeout) { @idle.wait until @busy.zero? }
          else
            @idle.wait until @busy.zero?
          end
        rescue Async::TimeoutError
          Console.warn(self, "#{@busy} handler(s) for #{queue_name} still running after #{@drain_timeout}s; " \
                             "stopping them and requeueing their messages")
        end
      ensure
        # Stops the pool. A handler stopped part way requeues its own message
        # on the way out (see #handle); an idle worker just ends.
        @workers.dup.each { |worker| worker.respond_to?(:cancel) ? worker.cancel : worker.stop }
        # Nothing may be left in hand, whatever happened above.
        requeue(@jobs.dequeue) until @jobs.empty?
      end
    end
  end
end
