# frozen_string_literal: true

require "async"
require "console"

require_relative "error"
require_relative "endpoint"
require_relative "topology"
require_relative "client"

module Async
  module RabbitMQ
    # The nodes of one RabbitMQ cluster, with a connection to each, and
    # publishers and consumers spread across them.
    #
    #   Async::RabbitMQ::Cluster.open(%w[amqp://rabbit1 amqp://rabbit2 amqp://rabbit3]) do |cluster|
    #     replies = cluster.consumer(prefetch: 50)   # on whichever node has least to do
    #     ...
    #   end
    #
    # A Client is one connection to one node, and everything made through it
    # lives on that node. An application that makes many consumers, each with
    # a queue of its own, puts all of those queues on the one node it happens
    # to be connected to. A Cluster holds a Client for every node and gives
    # each new publisher and consumer to the node that has the fewest, so the
    # load one application brings is shared out among them.
    #
    # It is a thin thing. The Clients do the work, each reconnecting to its
    # own node by itself; the Cluster only chooses between them.
    #
    # == Where things go
    #
    # A publisher or consumer is placed when it is made, on the connected
    # node with the fewest publishers and consumers already, and at random
    # between nodes that tie. If no node is connected it goes to the one with
    # the fewest anyway, and waits there.
    #
    # Queues, exchanges and bindings declared through the cluster are sent to
    # one node and remembered by all, so whichever reconnects declares them
    # again.
    #
    # == When a node goes away
    #
    # What was placed on it stays with it, and waits for it to come back:
    # nothing is moved by default. A queue lives on a node, and a consumer
    # moved elsewhere would have a new, empty queue; the application has to
    # be ready for that.
    #
    # A consumer says what it wants with +on_connection_loss:+, as on a
    # Client:
    #
    # [:wait] the default. Consume again on the same node when it is back.
    # [:end]  #each returns :connection_lost.
    # [:move] go to another node - the least loaded one that is connected -
    #         and consume there. A queue of the consumer's own is declared
    #         and bound again on the new node, empty, and under a new name if
    #         the broker names it; #on_attach is told. If no other node is
    #         connected the consumer waits where it is.
    #
    # A publisher waits for its node. One that should publish through
    # whichever node is up belongs on a Client given all the endpoints.
    #
    # == Starting
    #
    # #connect returns once every node has answered or failed. Those that
    # failed are retried in the background and join when they can. It raises
    # only if no node could be reached, or if one refused the credentials.
    #
    # Options are those of Client, and apply to the Client of every node.
    class Cluster
      # Open a cluster on +endpoints+ (Endpoints or URLs, one per node). With
      # a block, yields it and closes it when the block returns or raises.
      def self.open(endpoints, **options)
        cluster = new(endpoints, **options)
        cluster.connect
        return cluster unless block_given?

        begin
          yield cluster
        ensure
          cluster.close
        end
      end

      # The Client of each node, in the order the endpoints were given.
      attr :clients

      attr :topology

      def initialize(endpoints, **options)
        endpoints = Array(endpoints)
        raise ArgumentError, "A cluster needs at least one endpoint" if endpoints.empty?
        if options.key?(:topology) || options.key?(:shuffle)
          raise ArgumentError, "topology and shuffle are not options of a Cluster"
        end

        @topology  = Topology.new
        @clients   = endpoints.map { |endpoint| Client.new(endpoint, topology: @topology, **options) }.freeze
        @connected = false
        @closed    = false
      end

      # Connect to every node. See the class comment. Returns self.
      def connect
        raise ClosedError, "This cluster has been closed" if @closed
        return self if @connected

        parent   = Async::Task.current
        attempts = @clients.map do |client|
          Async do
            client.connect(parent: parent)
            nil
          rescue Error => error
            error
          end
        end
        failures = attempts.map(&:wait)

        refused = failures.find { |error| error.is_a?(AuthenticationError) }
        refused ||= failures.last if failures.none?(&:nil?)
        if refused
          @clients.each(&:close)
          @closed = true
          raise refused
        end

        @clients.zip(failures).each do |client, failure|
          next unless failure

          Console.warn(self, "Node not reachable; it will join when it is", endpoint: client.endpoint.to_s,
                                                                            exception: failure)
          client.connect(background: true, parent: parent)
        end
        @connected = true
        self
      end

      # True while at least one node is connected.
      def connected?
        !@closed && @clients.any?(&:connected?)
      end

      def closed?
        @closed
      end

      # Close every node's client, and with them everything made here.
      def close
        return if @closed

        @closed = true
        @clients.each do |client|
          client.close
        rescue StandardError => error
          Console.warn(self, "A node did not close cleanly", endpoint: client.endpoint.to_s, exception: error)
        end
        nil
      end

      # -----------------------------------------------------------------------
      # Publishers and consumers: made on the node with least to do.
      # -----------------------------------------------------------------------

      # A Publisher on one of the nodes. See Client#publisher.
      def publisher(exchange = "", **options, &block)
        place.publisher(exchange, **options, &block)
      end

      # A Consumer on one of the nodes. See Client#consumer, and the class
      # comment for +on_connection_loss: :move+.
      def consumer(queue = nil, **options, &block)
        place.consumer(queue, **movable(options), &block)
      end

      # A ManagedConsumer on one of the nodes. See Client#managed_consumer.
      def managed_consumer(queue = nil, **options, &block)
        place.managed_consumer(queue, **movable(options), &block)
      end

      # -----------------------------------------------------------------------
      # Declaring: sent to one node, remembered by all.
      # -----------------------------------------------------------------------

      # Declare a queue. See Client#queue.
      def queue(name, **options)
        declared = any.queue(name, **options)
        Queue.new(self, declared.name, durable: declared.durable?, auto_delete: declared.auto_delete?,
                                       arguments: declared.arguments)
      end

      # Declare an exchange. See Client#exchange.
      def exchange(name, type = :direct, **options)
        declared = any.exchange(name, type, **options)
        Exchange.new(self, declared.name, declared.type, durable: declared.durable?,
                                                         auto_delete: declared.auto_delete?,
                                                         internal: declared.internal?, arguments: declared.arguments)
      end

      def default_exchange
        @default_exchange ||= Exchange.new(self, "", :direct, durable: true, auto_delete: false, internal: false,
                                                              arguments: {})
      end

      def queue?(name) = any.queue?(name)
      def exchange?(name) = any.exchange?(name)
      def forget(entity) = any.forget(entity)

      def inspect
        up = @clients.count(&:connected?)
        "#<#{self.class.name} #{up}/#{@clients.size} nodes connected>"
      end

      # -----------------------------------------------------------------------
      # Internal: what Queue and Exchange call.
      # -----------------------------------------------------------------------

      def queue_status(...) = any.queue_status(...)
      def bind_queue(...) = any.bind_queue(...)
      def unbind_queue(...) = any.unbind_queue(...)
      def bind_exchange(...) = any.bind_exchange(...)
      def unbind_exchange(...) = any.unbind_exchange(...)
      def purge_queue(...) = any.purge_queue(...)
      def delete_queue(...) = any.delete_queue(...)
      def delete_exchange(...) = any.delete_exchange(...)

      private

      # The client a new publisher or consumer goes to: of those connected,
      # or of them all if none is, the one with the fewest already; at random
      # between equals, so that several applications starting together do
      # not all pick the same node.
      def place(except: nil)
        raise ClosedError, "This cluster has been closed" if @closed

        candidates = @clients.reject { |client| client.closed? || client.failure || client.equal?(except) }
        connected  = candidates.select(&:connected?)
        candidates = connected unless connected.empty?
        raise ConnectionError, "No node of this cluster is usable" if candidates.empty? && except.nil?

        fewest = candidates.map(&:link_count).min
        candidates.select { |client| client.link_count == fewest }.sample
      end

      # A connected client for a request that any node can answer.
      def any
        raise ClosedError, "This cluster has been closed" if @closed

        usable = @clients.reject { |client| client.closed? || client.failure }
        raise ConnectionError, "No node of this cluster is usable" if usable.empty?

        usable.find(&:connected?) || usable.first
      end

      # Consumer options, with the means to move added for one that asks to.
      def movable(options)
        return options unless options[:on_connection_loss] == :move

        options.merge(relocate: method(:relocate))
      end

      # Where a consumer whose node has gone should go: another node that is
      # connected, or nowhere (nil) if there is none.
      def relocate(consumer)
        return nil if @closed

        target = place(except: consumer.client)
        target if target&.connected?
      end
    end
  end
end
