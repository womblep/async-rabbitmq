# frozen_string_literal: true

require_relative "session"

module AsyncRabbitMQ
  # One connection to every node of a RabbitMQ cluster, behind the Session API.
  #
  # A Session is on one node, so every channel it opens lands there, and so
  # does every exclusive queue declared through it, because the broker always
  # places those on the connecting node. Losing that node loses them all. A
  # process holding a channel per client, with many clients, wants neither.
  #
  # A Cluster pins one Session to each address it is given, opens each new
  # channel on the node with the fewest, and retries a node that is down until
  # it is back, so new channels drift to it. Nothing is ever moved: a channel
  # stays on its node for life.
  #
  #   cluster = AsyncRabbitMQ::Cluster.new(addresses: %w[rabbit1:5672 rabbit2:5672 rabbit3:5672],
  #                                        username: "app", password: secret, on_node_down: :drop)
  #   cluster.connect                    # every node; the ones that are down are retried
  #   channel = cluster.open_channel     # on the node with the fewest channels
  #
  # Same constructor options and public methods as Session, plus two:
  #
  # * +on_node_down:+ what happens to the channels on a node whose connection
  #   is lost. +:park+ (the default, and what a Session does) keeps them: they
  #   wait, and resume on the same node when it returns. +:drop+ closes them at
  #   once, so the fibers using them see the loss immediately and can start
  #   again on another node through #open_channel. A callable receives
  #   (session, channels, error) while the channels are parked and closes the
  #   ones it wants dropped; the rest wait.
  # * +clear_topology_on_drop:+ with +:drop+, forget everything the lost
  #   connection had declared (default true), so nothing is re-declared when
  #   the node returns. false keeps the Session's registry rules, for
  #   connections that also carry durable topology.
  #
  # Ordinary publishing and shared topology are expected to live on a plain
  # Session alongside; this object is for the mass of per-client channels.
  class Cluster
    # Every node's registry, read-only, behind the TopologyRegistry readers.
    class TopologyView
      def initialize(registries)
        @registries = registries
      end

      def exchanges         = @registries.flat_map(&:exchanges)
      def queues            = @registries.flat_map(&:queues)
      def queue_bindings    = @registries.flat_map(&:queue_bindings)
      def exchange_bindings = @registries.flat_map(&:exchange_bindings)
      def consumers         = @registries.map(&:consumers).reduce({}, :merge)
      def empty?            = @registries.all?(&:empty?)
    end

    # [host, port] pairs, one Session each, in the order given.
    attr_reader :addresses
    attr_reader :vhost, :username

    # Structured events from every node and channel; see Session#on_event.
    attr_reader :notifier

    # Build a Cluster from one AMQP URI per node; see Session.from_uri.
    def self.from_uri(*uri_strings, **kwargs)
      new(**Session.options_from_uris(*uri_strings, **kwargs))
    end

    def initialize(on_node_down: :park, clear_topology_on_drop: true, **session_opts)
      unless %i[park drop].include?(on_node_down) || on_node_down.respond_to?(:call)
        raise ArgumentError, "on_node_down must be :park, :drop or a callable (got #{on_node_down.inspect})"
      end
      if session_opts[:auto_recover] == false
        raise ArgumentError, "a Cluster needs auto_recover: a node that is down is retried until it is back"
      end

      @on_node_down_policy    = on_node_down
      @clear_topology_on_drop = clear_topology_on_drop
      @logger   = session_opts[:logger] || Log.new
      @notifier = session_opts[:notifier] || Notifier.new(logger: @logger)
      if (instrumenter = session_opts[:instrumenter])
        @notifier.subscribe { |name, payload| instrumenter.call(name, payload) }
      end
      @addresses = Session.address_list(host: session_opts.fetch(:host, "localhost"),
                                        port: session_opts.fetch(:port, 5672),
                                        hosts: session_opts[:hosts], addresses: session_opts[:addresses])
      @vhost    = session_opts.fetch(:vhost, "/")
      @username = session_opts.fetch(:username, "guest")
      @retry_interval     = session_opts.fetch(:recovery_interval, Session::RECOVERY_INITIAL)
      @retry_max_interval = session_opts.fetch(:recovery_max_interval, Session::RECOVERY_MAX)

      node_opts = session_opts.except(:host, :port, :hosts, :addresses, :hosts_shuffle_strategy,
                                      :instrumenter, :logger, :notifier)
      @sessions = @addresses.map do |host, port|
        Session.new(**node_opts, host: host, port: port, logger: @logger, notifier: @notifier).tap { |s| watch(s) }
      end

      @closed      = true
      @supervisors = {}   # session => the task connecting it, until it is up
      @wakeups     = {}   # session => Async::Condition, to cut a retry sleep short on close
      @on_node_down = @on_node_up = @on_connection_lost = nil
      @on_blocked = @on_unblocked = @on_recovery_attempt = @on_recovery = @on_recovery_exhausted = nil
    end

    # The Session on each node, in address order.
    def sessions
      @sessions.dup
    end

    # A Session reports the address it is on; a Cluster is on all of them, so
    # these are the first address. See #addresses.
    def host = @addresses.first[0]
    def port = @addresses.first[1]

    # Connect every node, concurrently. Returns once each one is either
    # connected or has failed a first attempt, so the channels opened next are
    # spread over every node that is reachable rather than piling onto whichever
    # answered first. A node that did not answer keeps being retried in the
    # background with the recovery backoff and takes channels once it is up, so
    # the wait is bounded by one connect_timeout however many nodes are down.
    # Raises the last error if none can be reached, and AuthenticationError as
    # soon as a node reports it, since the others would say the same.
    def connect
      raise NotOpenError, "Cluster is already connected" unless @closed
      @closed  = false
      pending  = @sessions.size
      errors   = {}
      reported = Async::Condition.new
      # Parent the per-node supervisors at the reactor, not at whichever task
      # called connect. A caller that finishes, or is stopped, would otherwise
      # take every node's supervisor down with it — and with them the retry
      # loop that brings a node back — without raising anything. Transient so
      # they never hold the reactor open on their own; #close stops them.
      root = Async::Task.current.root
      @sessions.each do |session|
        @supervisors[session] = root.async(transient: true) do
          supervise(session) do |error|
            errors[session] = error
            pending -= 1
            reported.signal
          end
        end
      end

      loop do
        if (refused = errors.values.find { |e| e.is_a?(AuthenticationError) })
          close
          raise refused
        end
        break if pending.zero?
        reported.wait
      end
      return if open?

      close
      raise errors.values.compact.last || ConnectionTimeoutError.new("Could not connect to any node")
    end

    # True while at least one node is connected.
    def open?
      !@closed && @sessions.any?(&:open?)
    end

    # True before connect and after close. A connected cluster whose nodes are
    # all down is neither open nor closed, like a recovering Session.
    def closed?
      @closed
    end

    # Close every node's connection and stop retrying the ones that are down.
    def close
      return if @closed
      @closed = true
      @sessions.each { |s| s.close rescue nil }
      @wakeups.values.each { |c| c.signal rescue nil }
      # The supervisors are parented at the reactor, so dropping the references
      # would not stop them; a node still in its retry backoff would keep
      # reconnecting after the cluster was closed.
      @supervisors.each_value { |task| task.stop rescue nil }
      @supervisors.clear
    end

    # Open a channel on the connected node with the fewest channels.
    def open_channel(pool_size: 1)
      least_loaded_node.open_channel(pool_size: pool_size)
    end

    def with_channel(pool_size: 1)
      ch = open_channel(pool_size: pool_size)
      begin
        yield ch
      ensure
        ch.close rescue nil
      end
    end

    # Every channel on every node.
    def channels
      @sessions.flat_map(&:channels)
    end

    def channel_count
      @sessions.sum(&:channel_count)
    end

    # Rotate the secret on every node: sent to the connected ones, stored for
    # the next connect on the ones that are down.
    # Rotate the credential on every node. The new secret is stored on all of
    # them first, so a node that refuses the live update — or is down — still
    # reconnects with the new secret rather than the old one. Every node is
    # attempted; the failures are collected and raised together, so one bad
    # node cannot leave the ones after it on the previous secret.
    def update_secret(new_secret, reason = "secret update")
      raise NotOpenError, "No cluster node is connected" unless open?

      @sessions.each { |s| s.store_secret(new_secret) }

      errors = {}
      @sessions.each do |s|
        next unless s.open?

        begin
          s.update_secret(new_secret, reason)
        rescue => e
          errors[s] = e
        end
      end

      unless errors.empty?
        detail = errors.map { |s, e| "#{s.host}:#{s.port} (#{e.class}: #{e.message})" }.join(", ")
        raise ClusterError, "update_secret failed on #{errors.size} of #{@sessions.size} node(s): #{detail}"
      end

      true
    end

    def store_secret(new_secret)
      @sessions.each { |s| s.store_secret(new_secret) }
    end

    def queue_exists?(name)
      least_loaded_node.queue_exists?(name)
    end

    def exchange_exists?(name)
      least_loaded_node.exchange_exists?(name)
    end

    def topology
      TopologyView.new(@sessions.map(&:topology))
    end

    def frame_max
      (@sessions.find(&:open?) || @sessions.first).frame_max
    end

    # --- callbacks, fanned in from every node -------------------------------

    def on_blocked(&block)
      @on_blocked = block
    end

    def on_unblocked(&block)
      @on_unblocked = block
    end

    def on_recovery_attempt(&block)
      @on_recovery_attempt = block
    end

    # The block receives the node's Session.
    def on_recovery(&block)
      @on_recovery = block
    end

    def on_recovery_exhausted(&block)
      @on_recovery_exhausted = block
    end

    # The block receives the node's Session and the error.
    def on_connection_lost(&block)
      @on_connection_lost = block
    end

    def on_event(pattern = nil, &block)
      @notifier.subscribe(pattern, &block)
    end

    # Register a callback invoked when a node's connection is lost, after the
    # +on_node_down:+ policy has been applied. The block receives the node's
    # Session, the channels that were on it and the error; declare a fourth
    # parameter to also receive the UnconfirmedMessage records the broker never
    # acked, so they can be republished on another node. Under +:drop+ that is
    # the only chance to see them.
    def on_node_down(&block)
      @on_node_down = block
    end

    # Register a callback invoked when a node that was down is connected
    # again, by recovery or by the background retry. The block receives the
    # node's Session.
    def on_node_up(&block)
      @on_node_up = block
    end

    # --- Async::Pool resource interface ---

    def reusable?
      @sessions.any?(&:reusable?)
    end

    def viable?
      open?
    end

    # Channels across every node.
    def concurrency
      @sessions.sum(&:concurrency)
    end

    private

    def least_loaded_node
      @sessions.select(&:open?).min_by(&:channel_count) or
        raise NotOpenError, "No cluster node is connected"
    end

    def watch(session)
      session.on_connection_lost    { |s, error| node_down(s, error) }
      session.on_recovery           { |s| node_up(s); @on_recovery&.call(s) }
      session.on_recovery_attempt   { |n| @on_recovery_attempt&.call(n) }
      session.on_recovery_exhausted { |s| @on_recovery_exhausted&.call(s) }
      session.on_blocked            { |reason| @on_blocked&.call(reason) }
      session.on_unblocked          { @on_unblocked&.call }
    end

    # One per node: connects it, retrying with the recovery backoff until it is
    # up, and reports the first attempt's outcome to #connect. Once the node is
    # connected its Session looks after itself, so the task ends. A plain
    # Session, like Bunny's start, tries its address list once and leaves
    # retrying to the caller; here a node that is down at startup is nothing
    # special.
    def supervise(session)
      delay   = @retry_interval
      attempt = 0
      until @closed
        attempt += 1
        error = try_connect(session)
        yield error if attempt == 1
        if @closed                   # closed while this connect was in flight
          session.close rescue nil
          return
        end
        if error.nil?
          node_up(session) if attempt > 1
          return
        end
        return if error.is_a?(AuthenticationError)

        @logger.warn("Node #{session.host}:#{session.port} unreachable (#{error.class}: #{error.message}); " \
                     "next attempt in #{delay.round(1)}s")
        wait_for(session, delay)
        delay = [delay * 2, @retry_max_interval].min
      end
    end

    def try_connect(session)
      session.connect
      nil
    rescue AuthenticationError => e
      @logger.error("Node #{session.host}:#{session.port} refused the credentials: #{e.message}")
      e
    rescue Error, SystemCallError, IOError => e
      e
    end

    # Sleep for +seconds+, give or take the recovery jitter, cut short by #close.
    def wait_for(session, seconds)
      condition = @wakeups[session] = Async::Condition.new
      jitter = seconds * Session::RECOVERY_JITTER * (rand * 2 - 1)
      Async::Task.current.with_timeout(seconds + jitter) { condition.wait }
    rescue Async::TimeoutError
      nil
    ensure
      @wakeups.delete(session) if @wakeups[session].equal?(condition)
    end

    def node_down(session, error)
      channels = session.channels
      # Collected before the policy runs: :drop discards the channels, and
      # with them any record of what the broker never confirmed.
      unconfirmed = channels.flat_map { |ch| ch.unconfirmed_messages rescue [] }

      begin
        case @on_node_down_policy
        when :park then nil
        when :drop then drop_channels(session, channels, error)
        else @on_node_down_policy.call(session, channels, error)
        end
      rescue => e
        @logger.error("on_node_down failed for #{session.host}:#{session.port}: #{e.class}: #{e.message}")
      end
      @on_connection_lost&.call(session, error)
      notify_node_down(session, channels, error, unconfirmed)
    end

    # on_node_down took three arguments before unconfirmed messages were passed
    # to it, so a block that declares three still works.
    def notify_node_down(session, channels, error, unconfirmed)
      return unless @on_node_down

      if @on_node_down.arity >= 0 && @on_node_down.arity <= 3
        @on_node_down.call(session, channels, error)
      else
        @on_node_down.call(session, channels, error, unconfirmed)
      end
    end

    def drop_channels(session, channels, error)
      lost = ConnectionError.new(code: 0, text: "Connection to #{session.host}:#{session.port} lost: #{error.message}")
      channels.each { |ch| ch.drop!(lost) }
      # Only what was tied to this connection. Durable exchanges, queues and
      # bindings stay recorded: if this node comes back from an empty data
      # directory they still need re-declaring.
      session.topology.clear_transient if @clear_topology_on_drop
      @logger.info("Dropped #{channels.size} channel(s) on #{session.host}:#{session.port}")
    end

    def node_up(session)
      @logger.info("Node #{session.host}:#{session.port} connected")
      @on_node_up&.call(session)
    rescue => e
      @logger.error("on_node_up failed for #{session.host}:#{session.port}: #{e.class}: #{e.message}")
    end
  end
end
