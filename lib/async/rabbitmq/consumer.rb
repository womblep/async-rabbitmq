# frozen_string_literal: true

require "async"
require "async/promise"
require "async/queue"
require "async/semaphore"
require "console"
require "amq/protocol"

require_relative "error"
require_relative "channel"
require_relative "message"

module Async
  module RabbitMQ
    # A consumer you pull from. #each hands the block one Message at a time,
    # in the fiber that called it, and the block settles each one itself:
    #
    #   consumer = client.consumer("orders", prefetch: 10)
    #   consumer.each do |message|
    #     process(message.body)
    #     message.ack
    #   rescue InvalidOrder
    #     message.reject(requeue: false)
    #   end
    #
    # Nothing is settled for you. A block that returns without calling #ack,
    # #nack or #reject leaves the message unacknowledged, which is how a
    # message is handed to another fiber to finish later. An exception out of
    # the block ends the loop and is raised from #each, with the message it
    # was holding still unacknowledged.
    #
    # +prefetch+ is what bounds the consumer: the broker sends no more than
    # that many unsettled messages, so that is the most that can ever be
    # waiting here.
    #
    # == A queue of the consumer's own
    #
    # Given no queue, or +exclusive: true+, the consumer declares a queue
    # that is its alone - exclusive to its connection, and gone when that
    # connection goes - and binds it as asked:
    #
    #   replies = client.consumer(prefetch: 50)
    #   replies.queue_name                       # the name the broker chose
    #   replies.bind("events", routing_key: "order.*")
    #
    # After a lost connection the queue is declared again, with its bindings,
    # on the next one. Whatever was in it went with the old connection, and a
    # queue the broker named has a new name; #on_attach is how to hear of it.
    #
    # A queue named by the caller is declared again under the same name. On a
    # single node that is fine. In a cluster it can be refused: the old queue
    # goes when its connection closes, but not on every node at once, and
    # until it has, #each raises a ChannelError that is resource_locked?. A
    # name the broker chooses is new each time and cannot meet the old one,
    # so in a cluster leave the naming to the broker.
    #
    # == How #each ends
    #
    # It returns when the consumer goes away, and says why:
    #
    # [:stopped]         #stop was called.
    # [:cancelled]       the broker cancelled the consumer, or the queue was
    #                    gone when a lost connection came back. See below.
    # [:closed]          the consumer, or its client, was closed.
    # [:connection_lost] the connection was lost, and the consumer was
    #                    created with +on_connection_loss: :end+.
    #
    # A +break+ out of the block ends it too. It raises ChannelError if the
    # broker closes the consumer's channel or refuses the queue, and
    # ConnectionError if the client gives up reconnecting. However it ends,
    # the consumer is cancelled and the messages that had arrived but had not
    # been handed to the block are put back on the queue. The consumer can be
    # run again; messages the block kept unsettled can still be settled until
    # the consumer is closed.
    #
    # == A lost connection
    #
    # By default it is not an ending. #each waits for the client's next
    # connection, consumes again there, and carries on. Messages delivered on
    # the old connection are stale: the broker has requeued them and sends
    # them again, so any still waiting here are dropped without being
    # yielded, and settling one already handed out does nothing.
    #
    # If the queue is not there when the connection comes back - it was
    # deleted, and the client was told to forget it or never declared it -
    # #each returns :cancelled, as it does when the broker says so itself.
    #
    # == A consumer the broker cancels
    #
    # The broker cancels a consumer when its queue is deleted, and for
    # reasons that leave the queue where it was: a quorum queue that has
    # lost its leader, say. By default either ends #each with :cancelled.
    # With +on_cancel: :resubscribe+ the consumer asks whether the queue is
    # still there, and if it is, consumes from it again and carries on,
    # trying again at intervals growing from one second to thirty for as
    # long as the queue exists and the broker refuses. Only a queue that is
    # gone ends #each with :cancelled. Nothing is declared, by the consumer
    # or, until it is consuming again, by the client on a reconnect: a queue
    # somebody deleted is not put back.
    #
    # Options:
    #
    # [prefetch]           unsettled messages the broker sends at once.
    #                      Default 100; nil for no limit, which has the broker
    #                      send the whole queue.
    # [on_connection_loss] :wait (the default) or :end; see above. A consumer
    #                      of a Cluster may also say :move; see Cluster.
    # [on_cancel]          :end (the default) or :resubscribe; see above.
    # [exclusive]          the queue is the consumer's own. Default true when
    #                      no queue is named, false otherwise.
    # [queue_arguments]    arguments for the consumer's own queue.
    # [arguments]          arguments for basic.consume.
    #
    # For handlers that run concurrently, are acked when they return and
    # nacked when they raise, see ManagedConsumer, which is built on this.
    class Consumer
      include Channel::Delegate

      P = AMQ::Protocol
      private_constant :P

      # Pushed onto the buffer to wake #each. Compared by identity.
      WAKE = Object.new.freeze
      # The broker cancelled the consumer, or closed or lost the channel.
      Cancelled = Struct.new(:channel)
      Lost      = Struct.new(:channel, :error)
      private_constant :WAKE, :Cancelled, :Lost

      LOSS_POLICIES   = %i[wait end move].freeze
      CANCEL_POLICIES = %i[end resubscribe].freeze

      attr :client, :prefetch

      # :end or :resubscribe.
      attr :on_cancel

      # The tag of the running consumer; nil when #each is not running.
      attr :consumer_tag

      def initialize(client, queue = nil, prefetch: 100, on_connection_loss: :wait, on_cancel: :end, exclusive: nil,
                     queue_arguments: {}, arguments: {}, link: nil, relocate: nil)
        unless CANCEL_POLICIES.include?(on_cancel)
          raise ArgumentError, "on_cancel must be :end or :resubscribe (got #{on_cancel.inspect})"
        end
        unless prefetch.nil? || (prefetch.is_a?(Integer) && prefetch.positive?)
          raise ArgumentError, "prefetch must be a positive Integer or nil (got #{prefetch.inspect})"
        end
        unless LOSS_POLICIES.include?(on_connection_loss)
          raise ArgumentError, "on_connection_loss must be :wait, :end or :move (got #{on_connection_loss.inspect})"
        end
        if on_connection_loss == :move && relocate.nil?
          raise ArgumentError, "on_connection_loss: :move needs somewhere to move to: it is for a consumer of a Cluster"
        end

        name = queue.respond_to?(:name) ? queue.name : queue.to_s
        exclusive = name.empty? if exclusive.nil?
        raise ArgumentError, "A consumer of a queue that is not its own needs the queue's name" if name.empty? && !exclusive

        @client          = client
        @requested_name  = name
        @queue_name      = name.empty? ? nil : name
        @exclusive       = exclusive ? true : false
        @queue_arguments = queue_arguments
        @arguments       = arguments
        @prefetch        = prefetch
        @on_loss         = on_connection_loss
        @on_cancel       = on_cancel
        @pause           = nil # Promise a wait between attempts to resubscribe ends on
        # What is registered with the client: this consumer, or the
        # ManagedConsumer built on it. And, for a consumer of a Cluster, how
        # to find it another client.
        @link            = link || self
        @relocate        = relocate
        @bindings        = []
        @on_attach       = nil
        @on_cancelled    = nil
        @on_resubscribed = nil
        @channel         = nil
        @queue_connection = nil # the connection the consumer's own queue lives on
        @attaching       = Async::Semaphore.new(1)
        @buffer          = nil # the running #each's queue of messages and signals
        @consuming       = nil # the channel the running #each is consuming on
        @consumer_tag    = nil
        @broker_cancelled = false
        @stopping        = false
        @done            = nil # Promise resolved when the running #each has ended
        @fiber           = nil
        @closed          = false
        @attached_before = false

        client.register(@link)
        begin
          channel
        rescue Exception # rubocop:disable Lint/RescueException
          client.unregister(@link)
          @closed = true
          raise
        end
      end

      # The name of the queue consumed from. For a queue the broker named, the
      # name it has now: it changes when the queue is declared again on a new
      # connection.
      attr :queue_name

      # True if the queue is the consumer's own.
      def exclusive?
        @exclusive
      end

      # :wait, :end or :move.
      def on_connection_loss
        @on_loss
      end

      # True while #each is running.
      def running?
        !@buffer.nil?
      end

      def closed?
        @closed
      end

      # Bind the consumer's own queue to +exchange+ (an Exchange or its
      # name). The binding is the consumer's: it is made again whenever the
      # queue is declared again. For a queue that is not the consumer's own,
      # bind the Queue.
      def bind(exchange, routing_key: "", arguments: {})
        raise Error, "Only a consumer's own queue is bound through it; bind #{@queue_name} with Queue#bind" unless @exclusive

        binding = [exchange.respond_to?(:name) ? exchange.name : exchange.to_s, routing_key.to_s, arguments]
        return self if @bindings.include?(binding)

        current = @channel
        @bindings << binding
        begin
          # If there is a channel the binding is made now; if there is not,
          # opening one makes it with the rest.
          current&.open? ? bind_queue(current, binding) : channel
        rescue ChannelError
          # Refused, and it would be refused again on every reconnect.
          @bindings.delete(binding)
          raise
        end
        self
      end

      # Undo a #bind.
      def unbind(exchange, routing_key: "", arguments: {})
        binding = [exchange.respond_to?(:name) ? exchange.name : exchange.to_s, routing_key.to_s, arguments]
        return self unless @bindings.delete(binding)

        # On whatever channel there is or can be had: the queue outlives its
        # channel, and the binding with it.
        current = channel
        current.call(P::Queue::Unbind.encode(current.id, @queue_name, binding[0], binding[1], arguments),
                     P::Queue::UnbindOk)
        self
      end

      # Have +block+ called, with the consumer, each time it has attached to
      # a connection after the first: its channel is open and, if the queue
      # is its own, the queue is declared and bound. It runs in whichever
      # fiber brought that about, before any message from the new connection
      # is handed out. This is where to learn the new name of a queue the
      # broker named.
      def on_attach(&block)
        @on_attach = block
        self
      end

      # Have +block+ called, with the consumer, each time the broker cancels
      # it: before it tries to consume again if it was made with
      # +on_cancel: :resubscribe+, and before #each returns :cancelled if it
      # was not. It runs in the fiber that called #each.
      def on_cancelled(&block)
        @on_cancelled = block
        self
      end

      # Have +block+ called each time the consumer is consuming again after
      # the broker cancelled it, with the number of attempts it took (1 if
      # the first was accepted) and then the consumer. Only a consumer made
      # with +on_cancel: :resubscribe+ ever is.
      #
      # With either block set, the cancel and the recovery are the
      # application's to report, and the consumer's own lines about them are
      # logged at debug only.
      def on_resubscribed(&block)
        @on_resubscribed = block
        self
      end

      # Consume, yielding one Message at a time to the block in the calling
      # fiber, until the consumer goes away. Returns why; see the class
      # comment.
      def each
        raise ArgumentError, "Consumer#each needs a block" unless block_given?
        raise ClosedError, "This consumer has been closed" if @closed
        raise Error, "This consumer is already running on #{@queue_name}" if @buffer

        buffer    = @buffer = Async::Queue.new
        @done     = Async::Promise.new
        @fiber    = Fiber.current
        @stopping = false
        result    = nil
        begin
          subscribe
          loop do
            # Checked before the wait as well as after: a stop asked for
            # while the block ran must not go on to hand out what is waiting.
            if @stopping
              result = @closed ? :closed : :stopped
              break
            end

            item = buffer.dequeue
            case item
            when Message
              if @stopping
                requeue(item)
              elsif !item.stale?
                yield item
              end
            when Cancelled
              next unless item.channel.equal?(@consuming)

              @broker_cancelled = true
              told(:on_cancelled, @on_cancelled)
              next if @on_cancel == :resubscribe && resubscribe

              result = :cancelled
              break
            when Lost
              next unless item.channel.equal?(@consuming)
              raise item.error unless item.error.is_a?(ConnectionError)

              @consuming = @consumer_tag = nil
              if @on_loss == :end
                result = :connection_lost
                break
              end
              move if @on_loss == :move
              begin
                subscribe
              rescue ChannelError => error
                raise unless error.not_found?

                # The queue went while the connection was away. The cancel
                # the broker would have sent went with the connection.
                result = :cancelled
                break
              end
            end
          end
        rescue ClosedError
          # The consumer or its client was closed while this waited for a
          # connection.
          raise unless @closed || @client.closed?

          result = :closed
        end
        result
      ensure
        finish(buffer) if buffer
      end

      # Ask a running #each to end. It returns :stopped once the block has
      # finished with the message it is on; anything waiting behind that is
      # put back on the queue. Safe from any fiber, and when not running.
      def stop
        return unless (buffer = @buffer)

        @stopping = true
        buffer.push(WAKE)
        @pause&.resolve(true)
        nil
      end

      # Close the consumer: stop #each if it is running, wait for it to
      # return, and close the channel. Messages left unsettled go back on the
      # queue, and a queue of the consumer's own goes with its connection.
      def close
        return if @closed

        @closed = true
        stop
        # Wait for #each to cancel and put back what it holds, while the
        # channel is still there to do it on. An #each that is itself waiting
        # for a connection is not waited for: it has nothing to put back, and
        # returns :closed when its wait ends.
        done = @done
        done.wait if done && @consuming && !@fiber.equal?(Fiber.current)
        @client.unregister(@link)
        channel, @channel = @channel, nil
        channel&.close
        nil
      end

      def inspect
        state = @closed ? "closed" : (running? ? "running" : "idle")
        "#<#{self.class.name} queue=#{@queue_name.inspect} #{state}>"
      end

      # -----------------------------------------------------------------------
      # Internal: Channel::Delegate. Called on the connection's reader fiber,
      # so nothing here may wait.
      # -----------------------------------------------------------------------

      def deliver(method, header, body)
        message = Message.new(@channel, method, header, body)
        if (buffer = @buffer)
          buffer.push(message)
        else
          # Nobody is reading: it was on its way when the consumer was
          # cancelled, and goes straight back.
          requeue(message)
        end
      end

      def cancelled(_method)
        @buffer&.push(Cancelled.new(@channel))
      end

      def closed(error)
        channel, @channel = @channel, nil
        @buffer&.push(Lost.new(channel, error))
      end

      private

      # The consumer's channel, opened and prepared if there is none: prefetch
      # set and, for a queue of its own, the queue declared and bound. Waits
      # for a connection if need be. Raises what the broker refuses, and
      # ClosedError or ConnectionError once the client has ended.
      def channel
        current = @channel
        return current if current&.open?

        @attaching.acquire do
          current = @channel
          return current if current&.open?

          delay = 0.05
          loop do
            raise ClosedError, "This consumer has been closed" if @closed

            fresh = @channel = @client.open_channel(self)
            if @closed
              @channel = nil
              fresh.close
              raise ClosedError, "This consumer has been closed"
            end
            begin
              if prepare(fresh) && fresh.open?
                attached if @attached_before
                @attached_before = true
                return fresh
              end
            rescue ConnectionError
              raise if @client.closed? || @client.failure
            end
            sleep(delay)
            delay = [delay * 2, 1].min
          end
        end
      end

      def prepare(channel)
        if @prefetch
          channel.call(P::Basic::Qos.encode(channel.id, 0, @prefetch, false), P::Basic::QosOk)
        end
        return true unless @exclusive

        # On the connection the queue was declared on it still exists, and is
        # asked for by the name it has; on another it is declared anew, and a
        # queue the broker names gets a new name.
        connection = channel.connection
        if connection.equal?(@queue_connection) && @requested_name.empty?
          # A name the broker chose cannot be declared, only asked after.
          begin
            channel.call(P::Queue::Declare.encode(channel.id, @queue_name, true, false, true, false, false, {}),
                         P::Queue::DeclareOk)
          rescue ChannelError => error
            raise unless error.not_found?

            # Deleted behind our back. The refusal closed the channel; the
            # next attempt declares a new queue.
            @queue_connection = nil
            return false
          end
        else
          ok = channel.call(P::Queue::Declare.encode(channel.id, @requested_name, false, false, true, false, false,
                                                     @queue_arguments), P::Queue::DeclareOk)
          @queue_name       = ok.queue
          @queue_connection = connection
        end
        @bindings.each { |binding| bind_queue(channel, binding) }
        true
      end

      def bind_queue(channel, binding)
        exchange, routing_key, arguments = binding
        channel.call(P::Queue::Bind.encode(channel.id, @queue_name, exchange, routing_key, false, arguments),
                     P::Queue::BindOk)
      end

      # Leave the client whose connection was lost for another, if the
      # cluster has a better one to offer. The channel is opened there by
      # whoever asks for it next.
      def move
        target = @relocate.call(self)
        return if target.nil? || target.equal?(@client)

        @client.logger.info(self, "Consumer of #{@queue_name} is moving", from: @client.endpoint.to_s,
                                                                    to: target.endpoint.to_s)
        target.register(@link)
        @client.unregister(@link)
        @client = target
      rescue ClosedError
        # The cluster is closing. Stay; the wait for a connection ends it.
      end

      def attached
        told(:on_attach, @on_attach)
      end

      # Call one of the consumer's blocks. What it raises is its own problem.
      def told(name, block, *arguments)
        block&.call(*arguments, @link)
      rescue StandardError => error
        @client.logger.error(self, "#{name} for #{@queue_name} raised", exception: error)
      end

      # Start consuming on the consumer's channel, opening one if need be.
      def subscribe
        stale = 0
        begin
          channel = self.channel
          ok = channel.call(P::Basic::Consume.encode(channel.id, @queue_name, "", false, false, false, false,
                                                     @arguments), P::Basic::ConsumeOk)
          @consumer_tag     = ok.consumer_tag
          @consuming        = channel
          @broker_cancelled = false
        rescue ConnectionError
          raise if @closed || @client.closed? || @client.failure

          retry
        rescue ClosedError
          # The channel closed between being handed over and being used.
          raise if @closed || @client.closed? || (stale += 1) > 3

          retry
        end
      end

      # The broker cancelled the consumer. Consume again if the queue is still
      # there, trying for as long as it is; never declare it. True if #each
      # should carry on (consuming again, or stopping), false if the queue is
      # gone.
      #
      # The client does not declare the queue either while this goes on. A
      # quorum queue part way through being deleted still answers for itself,
      # and closes the connection on whoever consumes from it; the reconnect
      # would otherwise put the queue back from the client's record.
      def resubscribe
        @consuming = @consumer_tag = nil
        @client.without_declaring(@queue_name) { consume_again }
      end

      def consume_again
        delay    = 1
        failures = 0
        loop do
          return true if @stopping
          return false unless @client.queue?(@queue_name)

          begin
            subscribe
            @client.logger.public_send(@on_resubscribed ? :debug : :info, self,
                                       "Consumer of #{@queue_name} was cancelled by the broker, and is consuming again",
                                       attempts: failures + 1)
            told(:on_resubscribed, @on_resubscribed, failures + 1)
            return true
          rescue ChannelError => error
            return false if error.not_found?

            # Said once for each cancel, however long the broker goes on
            # refusing: a long outage is one warning, not one every 30s.
            failures += 1
            level = failures == 1 && !@on_cancelled && !@on_resubscribed ? :warn : :debug
            @client.logger.public_send(level, self,
                                       "Consumer of #{@queue_name} was cancelled by the broker and cannot " \
                                       "consume again yet; it will keep trying", exception: error)
          end
          pause(delay)
          delay = [delay * 2, 30].min
        end
      end

      # Wait +seconds+, or until #stop.
      def pause(seconds)
        @pause = Async::Promise.new
        Async::Task.current.with_timeout(seconds) { @pause.wait }
      rescue Async::TimeoutError
        nil
      ensure
        @pause = nil
      end

      # Cancel the consumer and put back whatever arrived and was not handed
      # out. Runs on every way out of #each, a stop of the calling task
      # included, so nothing here may assume the channel is still usable.
      def finish(buffer)
        channel = @consuming
        tag     = @consumer_tag
        if channel&.open? && tag && !@broker_cancelled
          begin
            # Deliveries already on their way arrive before the cancel-ok,
            # and land in the buffer, which is emptied below.
            channel.call(P::Basic::Cancel.encode(channel.id, tag, false), P::Basic::CancelOk)
          rescue Error => error
            @client.logger.debug(self, "Consumer #{tag} on #{@queue_name}: cancel failed while stopping", exception: error)
          end
        end
      ensure
        @buffer = nil if @buffer.equal?(buffer)
        until buffer.empty?
          item = buffer.dequeue
          requeue(item) if item.is_a?(Message)
        end
        @consuming = @consumer_tag = nil
        @stopping  = false
        @fiber     = nil
        done, @done = @done, nil
        done&.resolve(true)
      end

      # Put an undelivered message back. A channel that has gone has already
      # requeued it.
      def requeue(message)
        message.nack(requeue: true)
      rescue StandardError
        false
      end
    end
  end
end
