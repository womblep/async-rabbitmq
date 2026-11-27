# frozen_string_literal: true

require "async"
require "async/promise"
require "async/semaphore"
require "console"

require_relative "error"
require_relative "logging"
require_relative "endpoint"
require_relative "connection"
require_relative "topology"
require_relative "queue"
require_relative "exchange"
require_relative "publisher"
require_relative "consumer"
require_relative "managed_consumer"

module Async
  module RabbitMQ
    # A client for one RabbitMQ broker.
    #
    #   Async::RabbitMQ::Client.open("amqp://localhost") do |client|
    #     orders = client.queue("orders")
    #     ...
    #   end
    #
    # It holds one connection and keeps it: when the connection is lost the
    # client opens another, declares again whatever it was asked to declare,
    # and everything built on it carries on. Nothing is repaired in place. The
    # dead connection is thrown away, and each publisher and consumer attaches
    # itself to the new one on a channel of its own.
    #
    # While there is no connection, anything that needs one waits. It raises
    # only if the client is closed, or gives up after +reconnect_attempts+.
    #
    # Given several endpoints - the nodes of one cluster, say - the client
    # connects to whichever will have it, and when that connection is lost it
    # tries them all again. It is still one connection to one node at a time;
    # see Cluster for connections to several.
    #
    #   Async::RabbitMQ::Client.open(%w[amqp://rabbit1 amqp://rabbit2 amqp://rabbit3])
    #
    # Options:
    #
    # [shuffle]                try the endpoints in a new random order each
    #                          time. Default true, which spreads a fleet of
    #                          clients across the nodes; false tries them in
    #                          the order given.
    # [reconnect_interval]     seconds before the first attempt to reconnect.
    #                          It doubles with each failure. Default 1.
    # [reconnect_max_interval] where the doubling stops. Default 30.
    # [reconnect_attempts]     how many failures in a row end the client; nil,
    #                          the default, never gives up.
    # [logger]                 where the client, and everything built on it,
    #                          logs. Console by default; a Logger-compatible
    #                          object, or nil for silence. See Logging.
    #
    # == Hearing what happens to the connection
    #
    # The client logs what happens to its connection, and an application that
    # wants to count it or report it elsewhere can be told as well:
    #
    #   client.on_connection_lost  { |error| ... }
    #   client.on_reconnect_failed { |error, attempt| ... }
    #   client.on_reconnected      { |attempts| ... }
    #   client.on_blocked          { |reason| ... }
    #   client.on_unblocked        { ... }
    #   client.on_gave_up          { |error, attempts| ... }
    #
    # Each may be given more than once, and every block is called. Each block
    # is also passed the client as a last argument, for one block shared
    # between the clients of a Cluster. They run on the fiber that noticed, in
    # the middle of its work: a block should record what it was told and
    # return, and must not wait on the client. One that raises is logged and
    # otherwise ignored.
    #
    # An event the application has a hook for is the application's to
    # report: the client's own line about it is logged at debug only.
    class Client
      P = AMQ::Protocol
      private_constant :P

      # Open a client on +endpoint+: an Endpoint, a URL, or an Array of either.
      # With a block, yields it and closes it when the block returns or raises;
      # without, returns it for the caller to close.
      def self.open(endpoint, **options)
        client = new(endpoint, **options)
        client.connect
        return client unless block_given?

        begin
          yield client
        ensure
          client.close
        end
      end

      # The endpoint of the current connection, or of the last one; before
      # there has been any, the first one given.
      attr :endpoint

      # Every endpoint the client may connect to.
      attr :endpoints

      attr :topology

      # Where the client logs: what Logging.resolve made of +logger:+.
      attr :logger

      # +topology+ is for a Cluster, whose clients share one record of what
      # has been declared.
      def initialize(endpoint, reconnect_interval: 1, reconnect_max_interval: 30, reconnect_attempts: nil,
                     shuffle: true, topology: nil, logger: ::Console)
        @endpoints = (endpoint.is_a?(Array) ? endpoint : [endpoint]).map do |one|
          one.is_a?(Endpoint) ? one : Endpoint.parse(one.to_s)
        end.freeze
        raise ArgumentError, "A client needs at least one endpoint" if @endpoints.empty?

        @endpoint = @endpoints.first
        @shuffle  = shuffle
        @reconnect_interval     = reconnect_interval
        @reconnect_max_interval = reconnect_max_interval
        @reconnect_attempts     = reconnect_attempts
        @topology   = topology || Topology.new
        @logger     = Logging.resolve(logger)
        @parent     = nil
        @connection = nil
        # Resolved with the connection once there is one and the topology is
        # declared on it; replaced by a fresh one when that connection is
        # lost; rejected when the client ends. Everything that needs a
        # connection waits on it, so nothing can miss the moment.
        @ready      = Async::Promise.new
        @supervisor = nil
        @admin      = nil
        @admin_guard = Async::Semaphore.new(1)
        @links      = []
        @closed     = false
        @failure    = nil
        @hooks      = Hash.new { |hooks, event| hooks[event] = [] }
        @connected_before = false
      end

      # Connect. Raises if the broker cannot be reached or refuses: the first
      # connection is not retried, so that a wrong address or password is
      # reported and not waited on for ever. Returns self.
      #
      # With +background: true+ it returns at once and connects from a
      # background task instead, retrying as it would after a lost
      # connection: for a Cluster, which starts with whatever nodes are up
      # and lets the rest join when they can. The client's background tasks
      # are children of +parent+.
      def connect(background: false, parent: Async::Task.current)
        raise ClosedError, "This client has been closed" if @closed
        return self if @supervisor

        @parent = parent
        if background
          @supervisor = parent.async(transient: true) do
            connection = reconnect and supervise(connection)
          end
        else
          connection = open_connection(parent)
          establish(connection)
          @connected_before = true
          @supervisor = parent.async(transient: true) { supervise(connection) }
        end
        self
      end

      # How many publishers and consumers are attached to this client.
      def link_count
        @links.size
      end

      # True while there is a usable connection.
      def connected?
        !@closed && @connection&.open? ? true : false
      end

      def closed?
        @closed
      end

      # Why the client gave up reconnecting; nil unless it has.
      attr :failure

      # Call the block with the error each time the connection is lost. The
      # client is about to reconnect; a connection the client closed itself is
      # not a loss.
      def on_connection_lost(&block) = hook(:connection_lost, block)

      # Call the block with the error, and how many attempts have now failed
      # in a row, each time an attempt to connect again fails. The last one
      # before the client gives up is reported too.
      def on_reconnect_failed(&block) = hook(:reconnect_failed, block)

      # Call the block each time there is a connection again after a loss,
      # with how many attempts it took. The topology has been declared on it
      # by then. The first connection is not a reconnection.
      def on_reconnected(&block) = hook(:reconnected, block)

      # Call the block with the broker's reason when it blocks publishing on
      # this connection (a resource alarm), and when it lifts the block. A
      # connection lost while blocked is reported as lost, not as unblocked.
      def on_blocked(&block)   = hook(:blocked, block)
      def on_unblocked(&block) = hook(:unblocked, block)

      # Call the block with the last error, and how many attempts were made,
      # when the client gives up reconnecting: it was given
      # +reconnect_attempts+ and that many have failed in a row. #failure is
      # set by then, and everything waiting on the client has been told.
      def on_gave_up(&block) = hook(:gave_up, block)

      # Close the client: everything built on it, then the connection.
      def close
        return if @closed

        @closed = true
        links, @links = @links, []
        links.each do |link|
          link.close
        rescue StandardError => error
          @logger.warn(self, "A #{link.class.name} did not close cleanly", exception: error)
        end
        stop_supervisor
        @ready.reject(ClosedError.new("This client has been closed")) unless @ready.resolved?
        connection, @connection = @connection, nil
        connection&.close
        nil
      end

      # -----------------------------------------------------------------------
      # Declaring
      # -----------------------------------------------------------------------

      # Declare a queue and return it. A queue declared here is durable, and
      # +durable: false+ is refused: it would be neither durable nor
      # exclusive, which RabbitMQ 4 no longer permits and answers by closing
      # the whole connection. +type+ is :classic, :quorum or :stream; nil
      # leaves it to the broker.
      #
      # For a queue private to one consumer - exclusive, and gone with it -
      # ask for it on the consumer, which then owns it.
      def queue(name, durable: true, auto_delete: false, type: nil, arguments: {})
        name = name.to_s
        raise ArgumentError, "A queue declared on the client needs a name" if name.empty?
        unless durable
          raise ArgumentError, "A queue declared on the client is durable: RabbitMQ 4 refuses one that is " \
                               "neither durable nor exclusive. For a queue that goes when its consumer does, " \
                               "let the consumer declare its own (client.consumer with no queue, or exclusive: true)"
        end

        arguments = arguments.merge("x-queue-type" => type.to_s) if type
        admin do |channel|
          channel.call(P::Queue::Declare.encode(channel.id, name, false, durable, false, auto_delete, false, arguments),
                       P::Queue::DeclareOk)
        end
        @topology.record_queue(name, durable: durable, auto_delete: auto_delete, arguments: arguments)
        Queue.new(self, name, durable: durable, auto_delete: auto_delete, arguments: arguments)
      end

      # Declare an exchange and return it.
      def exchange(name, type = :direct, durable: true, auto_delete: false, internal: false, arguments: {})
        name = name.to_s
        raise ArgumentError, "An exchange needs a name; see #default_exchange for the nameless one" if name.empty?

        type = type.to_s
        admin do |channel|
          channel.call(P::Exchange::Declare.encode(channel.id, name, type, false, durable, auto_delete, internal,
                                                   false, arguments), P::Exchange::DeclareOk)
        end
        @topology.record_exchange(name, type, durable: durable, auto_delete: auto_delete, internal: internal,
                                              arguments: arguments)
        Exchange.new(self, name, type.to_sym, durable: durable, auto_delete: auto_delete, internal: internal,
                                              arguments: arguments)
      end

      # The nameless exchange, which routes to the queue named by the routing key.
      def default_exchange
        @default_exchange ||= Exchange.new(self, "", :direct, durable: true, auto_delete: false, internal: false,
                                                              arguments: {})
      end

      # A Publisher to +exchange+ (an Exchange or its name; the default
      # exchange if none is given). With a block, yields it and closes it
      # afterwards. See Publisher for the options.
      def publisher(exchange = "", **options)
        publisher = Publisher.new(self, exchange, **options)
        return publisher unless block_given?

        begin
          yield publisher
        ensure
          publisher.close
        end
      end

      # A Consumer of +queue+ (a Queue or its name), to pull messages from.
      # With no queue the consumer declares one of its own. With a block,
      # yields the consumer and closes it afterwards. See Consumer.
      def consumer(queue = nil, **options)
        consumer = Consumer.new(self, queue, **options)
        return consumer unless block_given?

        begin
          yield consumer
        ensure
          consumer.close
        end
      end

      # A ManagedConsumer of +queue+, which runs a handler for each message.
      # With a block, yields it and closes it afterwards. See ManagedConsumer.
      def managed_consumer(queue = nil, **options)
        consumer = ManagedConsumer.new(self, queue, **options)
        return consumer unless block_given?

        begin
          yield consumer
        ensure
          consumer.close
        end
      end

      # Whether a queue of this name exists on the broker.
      def queue?(name)
        queue_status(name.to_s)
        true
      rescue ChannelError => error
        raise unless error.not_found?

        false
      end

      # Whether an exchange of this name exists on the broker.
      def exchange?(name)
        admin do |channel|
          channel.call(P::Exchange::Declare.encode(channel.id, name.to_s, "", true, false, false, false, false, {}),
                       P::Exchange::DeclareOk)
        end
        true
      rescue ChannelError => error
        raise unless error.not_found?

        false
      end

      # Stop declaring +entity+ (a Queue, an Exchange or a name) on reconnect,
      # without deleting it. For something the application knows has been
      # deleted by someone else.
      def forget(entity)
        name = entity.respond_to?(:name) ? entity.name : entity.to_s
        entity.is_a?(Exchange) ? @topology.delete_exchange(name) : @topology.delete_queue(name)
        nil
      end

      def inspect
        state = @closed ? "closed" : (connected? ? "connected" : "reconnecting")
        "#<#{self.class.name} #{@endpoint} #{state}>"
      end

      # -----------------------------------------------------------------------
      # Internal: what Queue and Exchange call.
      # -----------------------------------------------------------------------

      def queue_status(name)
        ok = admin do |channel|
          channel.call(P::Queue::Declare.encode(channel.id, name, true, false, false, false, false, {}),
                       P::Queue::DeclareOk)
        end
        Queue::Status.new(ok.message_count, ok.consumer_count)
      end

      def bind_queue(queue, exchange, routing_key, arguments)
        admin do |channel|
          channel.call(P::Queue::Bind.encode(channel.id, queue, exchange, routing_key, false, arguments),
                       P::Queue::BindOk)
        end
        @topology.record_queue_binding(queue, exchange, routing_key, arguments)
      end

      def unbind_queue(queue, exchange, routing_key, arguments)
        admin do |channel|
          channel.call(P::Queue::Unbind.encode(channel.id, queue, exchange, routing_key, arguments),
                       P::Queue::UnbindOk)
        end
        @topology.delete_queue_binding(queue, exchange, routing_key, arguments)
      end

      def bind_exchange(destination, source, routing_key, arguments)
        admin do |channel|
          channel.call(P::Exchange::Bind.encode(channel.id, destination, source, routing_key, false, arguments),
                       P::Exchange::BindOk)
        end
        @topology.record_exchange_binding(destination, source, routing_key, arguments)
      end

      def unbind_exchange(destination, source, routing_key, arguments)
        admin do |channel|
          channel.call(P::Exchange::Unbind.encode(channel.id, destination, source, routing_key, false, arguments),
                       P::Exchange::UnbindOk)
        end
        @topology.delete_exchange_binding(destination, source, routing_key, arguments)
      end

      def purge_queue(name)
        admin do |channel|
          channel.call(P::Queue::Purge.encode(channel.id, name, false), P::Queue::PurgeOk)
        end.message_count
      end

      def delete_queue(name, if_unused:, if_empty:)
        # Forgotten first: if the connection goes between the delete and its
        # answer, the reconnect must not put back what was just deleted.
        @topology.delete_queue(name)
        admin do |channel|
          channel.call(P::Queue::Delete.encode(channel.id, name, if_unused, if_empty, false), P::Queue::DeleteOk)
        end.message_count
      end

      def delete_exchange(name, if_unused:)
        @topology.delete_exchange(name)
        admin do |channel|
          channel.call(P::Exchange::Delete.encode(channel.id, name, if_unused, false), P::Exchange::DeleteOk)
        end
        nil
      end

      # -----------------------------------------------------------------------
      # Internal: what a Publisher or Consumer builds on.
      # -----------------------------------------------------------------------

      # The open connection, waiting for one if there is none. Raises
      # ClosedError once the client is closed, and the last connection error
      # once it has given up.
      def connection
        loop do
          connection = @ready.wait
          return connection if connection.open?
          raise ClosedError, "This client has been closed" if @closed

          # Lost, and the supervisor has not got to it yet: start the wait for
          # the next one here, so that this does not spin on a dead connection.
          lost(connection)
        end
      end

      # A channel on the current connection, waiting for a connection if need
      # be. +delegate+ hears what the broker sends on it, and when it closes.
      def open_channel(delegate = nil)
        loop do
          return connection.open_channel(delegate)
        rescue ConnectionError
          # Went between being handed over and being used. Wait for the next.
          raise if @closed || @failure
        end
      end

      # Run the block with the queue called +name+ left out of what a new
      # connection declares. For a consumer finding out whether its queue is
      # still there: a connection lost while it asks must not put the queue
      # back. A queue that does still exist loses nothing by it.
      def without_declaring(name)
        topology = @topology
        topology.hold_queue(name)
        yield
      ensure
        topology&.release_queue(name)
      end

      # Have +link+ closed when the client closes.
      def register(link)
        raise ClosedError, "This client has been closed" if @closed

        @links << link
        link
      end

      def unregister(link)
        @links.delete(link)
        nil
      end

      # Run +block+ in a background task for a link: one that does not hold
      # the reactor open, and does not belong to whichever fiber happened to
      # create the link, which may be long finished while the link lives on.
      def spawn(&block)
        parent = @supervisor
        parent = Async::Task.current if parent.nil? || parent.finished?
        parent.async(transient: true, &block)
      end

      private

      # Run a request on the client's own channel. A request interrupted by
      # the connection going is made again on the next connection: everything
      # sent this way can be repeated without harm. A ChannelError is the
      # broker's answer and is raised; it has closed the channel, and the next
      # request opens another.
      #
      # A request that loses the connection three times running is taken to
      # be what is losing it - the broker closes the connection, not just
      # the channel, over some things it refuses - and the error is raised.
      def admin
        stale = 0
        lost  = 0
        begin
          yield admin_channel
        rescue ConnectionError
          raise if @closed || @failure || (lost += 1) >= 3

          retry
        rescue ClosedError
          # Closed under us by another fiber's failed request.
          raise if @closed || (stale += 1) > 3

          retry
        end
      end

      # One at a time: two fibers finding it closed together would each open a
      # channel, and the one not kept would never be closed.
      def admin_channel
        @admin_guard.acquire do
          @admin = open_channel unless @admin&.open?
          @admin
        end
      end

      def hook(event, block)
        raise ArgumentError, "on_#{event} needs a block" unless block

        @hooks[event] << block
        self
      end

      # Tell everyone who asked to hear of +event+. What a block raises is
      # its own problem, and must not become the connection's.
      def notify(event, *arguments)
        return unless @hooks.key?(event)

        @hooks[event].each do |block|
          block.call(*arguments, self)
        rescue StandardError => error
          @logger.error(self, "on_#{event} raised", exception: error)
        end
      end

      def hooked?(event)
        @hooks.key?(event)
      end

      # The client's own line about +event+. An application that has a hook
      # for the event reports it itself, in its own words and at the level it
      # chooses, so the client's line is then only for debugging.
      def report(event, level, message, **fields)
        @logger.public_send(hooked?(event) ? :debug : level, self, message, **fields)
      end

      def establish(connection)
        declare_topology(connection)
        @connection = connection
        @endpoint   = connection.endpoint
        @admin      = nil
        # The block says whether the application is reporting this itself.
        connection.on_blocked_change do |reason|
          next false unless @connection.equal?(connection)

          reason ? notify(:blocked, reason) : notify(:unblocked)
          hooked?(reason ? :blocked : :unblocked)
        end
        @ready.resolve(connection)
        # Blocked while the topology was being declared, before anyone was
        # listening.
        notify(:blocked, connection.blocked) if connection.blocked?
      end

      # Connect to whichever endpoint will have us: each is tried in turn, in
      # a new random order each time unless the client was told not to
      # shuffle, and the first to answer is the connection. Raises the last
      # failure when none does. A refusal of the credentials ends it at once:
      # the other endpoints are the same broker and would say the same.
      def open_connection(parent = Async::Task.current)
        candidates = @shuffle ? @endpoints.shuffle : @endpoints
        failure    = nil
        candidates.each do |endpoint|
          return Connection.open(endpoint, parent: parent, logger: @logger)
        rescue AuthenticationError
          raise
        rescue Error => error
          failure = error
          @logger.debug(self, "Could not connect", endpoint: endpoint.to_s, exception: error) if candidates.size > 1
        end
        raise failure
      end

      # Start a new wait, once, for the connection after +connection+.
      def lost(connection)
        return unless @connection.equal?(connection)

        @connection = nil
        @admin      = nil
        @ready      = Async::Promise.new unless @closed
      end

      def supervise(connection)
        loop do
          error = connection.wait
          break if @closed

          lost(connection)
          report(:connection_lost, :warn, "Connection lost; reconnecting", endpoint: @endpoint.to_s, exception: error)
          notify(:connection_lost, error)
          connection = reconnect or break
        end
      end

      # Keep trying until there is a connection with the topology declared on
      # it, and return it. Nil if the client was closed or gave up.
      def reconnect
        delay    = @reconnect_interval
        attempts = 0
        loop do
          return nil if @closed

          connection = nil
          begin
            connection = open_connection
            establish(connection)
            report(:reconnected, :info, "Reconnected", endpoint: @endpoint.to_s, attempts: attempts + 1)
            notify(:reconnected, attempts + 1) if @connected_before
            @connected_before = true
            return connection
          rescue Error => error
            connection&.close(error)
            return nil if @closed

            attempts += 1
            notify(:reconnect_failed, error, attempts)
            return give_up(error, attempts) if @reconnect_attempts && attempts >= @reconnect_attempts

            @logger.debug(self, "Reconnect failed", endpoint: @endpoint.to_s, attempt: attempts, exception: error)
            # Jittered, so that many clients that lost the same broker do not
            # all come back at it in the same instant.
            sleep(delay * (0.8 + (rand * 0.4)))
            delay = [delay * 2, @reconnect_max_interval].min
          end
        end
      end

      def give_up(error, attempts)
        @failure = ConnectionError.new("Gave up reconnecting to #{@endpoint} after #{attempts} attempt(s): " \
                                       "#{error.class}: #{error.message}")
        report(:gave_up, :error, "Gave up reconnecting", endpoint: @endpoint.to_s, attempts: attempts, exception: error)
        @ready.reject(@failure) unless @ready.resolved?
        notify(:gave_up, error, attempts)
        nil
      end

      # Declare everything in the topology on +connection+, before anything
      # else is let onto it. An entity the broker refuses is reported and
      # skipped: one bad declaration must not hold the rest of the application
      # off the connection. A connection that goes meanwhile raises, and the
      # reconnect starts over. A queue that is held (see Topology) is left
      # out, with its bindings.
      def declare_topology(connection)
        return if @topology.empty?

        channel = nil
        declare = lambda do |what, frame, reply|
          channel = connection.open_channel unless channel&.open?
          channel.call(frame.call(channel.id), reply)
        rescue ChannelError => error
          @logger.warn(self, "Could not declare #{what} again", endpoint: @endpoint.to_s, exception: error)
        end

        @topology.exchanges.each do |x|
          declare.call("exchange #{x.name}", lambda { |id|
            P::Exchange::Declare.encode(id, x.name, x.type, false, x.durable, x.auto_delete, x.internal, false, x.arguments)
          }, P::Exchange::DeclareOk)
        end
        @topology.queues.each do |q|
          next if @topology.held?(q.name)

          declare.call("queue #{q.name}", lambda { |id|
            P::Queue::Declare.encode(id, q.name, false, q.durable, false, q.auto_delete, false, q.arguments)
          }, P::Queue::DeclareOk)
        end
        @topology.exchange_bindings.each do |b|
          declare.call("binding #{b.source} -> #{b.destination}", lambda { |id|
            P::Exchange::Bind.encode(id, b.destination, b.source, b.routing_key, false, b.arguments)
          }, P::Exchange::BindOk)
        end
        @topology.queue_bindings.each do |b|
          next if @topology.held?(b.destination)

          declare.call("binding #{b.source} -> #{b.destination}", lambda { |id|
            P::Queue::Bind.encode(id, b.destination, b.source, b.routing_key, false, b.arguments)
          }, P::Queue::BindOk)
        end
        channel&.close
      end

      def stop_supervisor
        supervisor, @supervisor = @supervisor, nil
        return unless supervisor && !supervisor.equal?(Async::Task.current?)

        supervisor.respond_to?(:cancel) ? supervisor.cancel : supervisor.stop
      end
    end
  end
end
