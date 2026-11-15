require "spec_helper"

# Fixes that need no broker: what survives a node drop, what a notifier does
# while its subscriber list changes under it, and that a cluster-wide secret
# rotation is all-or-reported.
RSpec.describe "reliability fixes" do

  describe AsyncRabbitMQ::TopologyRegistry do
    def populated
      reg = described_class.new
      reg.record_exchange(1, "durable.x", :direct, durable: true, auto_delete: false, internal: false, arguments: {})
      reg.record_exchange(1, "temp.x", :fanout, durable: false, auto_delete: true, internal: false, arguments: {})
      reg.record_queue(1, "durable.q", durable: true, exclusive: false, auto_delete: false,
                          arguments: {}, server_named: false)
      reg.record_queue(1, "excl.q", durable: false, exclusive: true, auto_delete: false,
                          arguments: {}, server_named: false)
      reg.record_queue(1, "auto.q", durable: false, exclusive: false, auto_delete: true,
                          arguments: {}, server_named: false)
      reg.record_queue(1, "amq.gen-abc", durable: false, exclusive: false, auto_delete: false,
                          arguments: {}, server_named: true)
      reg.record_queue_binding(1, queue: "durable.q", exchange: "durable.x", routing_key: "k", arguments: {})
      reg.record_queue_binding(1, queue: "excl.q",    exchange: "durable.x", routing_key: "k", arguments: {})
      reg.record_queue_binding(1, queue: "durable.q", exchange: "temp.x",    routing_key: "k", arguments: {})
      reg
    end

    it "clear_transient keeps durable topology and forgets connection-scoped topology" do
      reg = populated
      reg.clear_transient

      expect(reg.exchanges.map(&:name)).to eq(["durable.x"])
      expect(reg.queues.map(&:name)).to eq(["durable.q"])
    end

    it "clear_transient drops bindings that referred to what it forgot" do
      reg = populated
      reg.clear_transient

      remaining = reg.queue_bindings.map { |b| [b.queue, b.exchange] }
      expect(remaining).to eq([["durable.q", "durable.x"]])
    end

    it "clear still forgets everything" do
      reg = populated
      reg.clear
      expect(reg).to be_empty
    end

    # A queue whose consumer the broker cancelled is held back from recovery
    # until the client has established whether it is still there. Recovering it
    # meanwhile puts a deleted queue back, and a queue that comes back with a
    # consumer on it cannot be deleted at all.
    describe "a suspended queue" do
      it "is left out of recovery, with its bindings" do
        reg = populated
        reg.suspend_queue("durable.q")

        expect(reg.queues.map(&:name)).not_to include("durable.q")
        expect(reg.queue_bindings.map(&:queue)).not_to include("durable.q")
      end

      it "comes back when it is resumed" do
        reg = populated
        reg.suspend_queue("durable.q")
        reg.resume_queue("durable.q")

        expect(reg.queues.map(&:name)).to include("durable.q")
        expect(reg.queue_bindings.map(&:queue)).to include("durable.q")
      end

      it "is settled by the application declaring it again" do
        reg = populated
        reg.suspend_queue("durable.q")
        reg.record_queue(1, "durable.q", durable: true, exclusive: false, auto_delete: false,
                            arguments: {}, server_named: false)

        expect(reg.suspended_queues).to be_empty
        expect(reg.queues.map(&:name)).to include("durable.q")
      end

      it "leaves nothing behind when it turns out to be deleted" do
        reg = populated
        reg.suspend_queue("durable.q")
        reg.delete_queue("durable.q")

        expect(reg.suspended_queues).to be_empty
        expect(reg.queues.map(&:name)).not_to include("durable.q")
      end

      it "follows a server-named queue to its new name" do
        reg = populated
        reg.suspend_queue("amq.gen-abc")
        reg.rename_queue("amq.gen-abc", "amq.gen-xyz")

        expect(reg.suspended_queues.to_a).to eq(["amq.gen-xyz"])
      end

      it "is not carried over a clear" do
        reg = populated
        reg.suspend_queue("durable.q")
        reg.clear

        expect(reg.suspended_queues).to be_empty
      end
    end
  end

  # Both answers are NOT_FOUND and only the text separates them: one queue is
  # gone, the other is on a node that has stopped and comes back with it.
  # Producing the second needs more than one node, so the predicate that tells
  # them apart is tested on its own.
  describe "a 404 on a passive declare" do
    def missing?(text)
      AsyncRabbitMQ::Channel.allocate.send(:queue_missing?,
                                           AsyncRabbitMQ::ChannelError.new(code: 404, text: text))
    end

    it "is a deletion when the broker says there is no such queue" do
      expect(missing?("NOT_FOUND - no queue 'q' in vhost '/'")).to be(true)
    end

    it "is not a deletion when the queue's process is stopped" do
      expect(missing?("NOT_FOUND - queue 'q' in vhost '/' process is stopped by supervisor")).to be(false)
    end

    it "is not a deletion when the queue's home node is down" do
      expect(missing?("NOT_FOUND - home node 'rabbit@two' of durable queue 'q' in vhost '/' is down or inaccessible"))
        .to be(false)
    end

    it "is not a deletion for any other code" do
      error = AsyncRabbitMQ::ChannelError.new(code: 405, text: "RESOURCE_LOCKED - cannot obtain exclusive access")
      expect(AsyncRabbitMQ::Channel.allocate.send(:queue_missing?, error)).to be(false)
    end
  end

  describe AsyncRabbitMQ::Notifier do
    it "does not skip a subscriber when another unsubscribes mid-publish" do
      notifier = described_class.new
      seen = []

      first = notifier.subscribe { |name, _| seen << [:first, name]; notifier.unsubscribe(first) }
      notifier.subscribe { |name, _| seen << [:second, name] }
      notifier.subscribe { |name, _| seen << [:third, name] }

      notifier.publish("event.one", {})

      # Without a snapshot, removing :first shifts the array under the iterator
      # and :second is stepped over.
      expect(seen).to eq([[:first, "event.one"], [:second, "event.one"], [:third, "event.one"]])
    end

    it "honours an unsubscribe on the next publish" do
      notifier = described_class.new
      seen = []
      handle = notifier.subscribe { |name, _| seen << name }
      notifier.publish("a", {})
      notifier.unsubscribe(handle)
      notifier.publish("b", {})
      expect(seen).to eq(["a"])
    end
  end

  describe "background task parent" do
    it "uses the reactor it is called in, not the one it first saw" do
      session = AsyncRabbitMQ::Session.new(host: "localhost")
      roots = []

      # Two separate reactors, as a CLI-style script or a test suite gives it.
      2.times do
        Sync do
          task = session.spawn_background { sleep 0 }
          roots << task.parent
          task.stop
        end
      end

      expect(roots.size).to eq(2)
      # Memoising the first reactor's root would keep spawning onto a reactor
      # that has already finished.
      expect(roots[0]).not_to equal(roots[1])
    end
  end

  describe AsyncRabbitMQ::Cluster do
    it "stores the new secret on every node before updating any of them" do
      cluster = described_class.new(addresses: %w[a b c])
      order   = []

      cluster.sessions.each_with_index do |s, i|
        s.define_singleton_method(:open?) { true }
        s.define_singleton_method(:store_secret) { |secret| order << [:store, i]; @password = secret }
        s.define_singleton_method(:update_secret) do |_secret, _reason = nil|
          order << [:update, i]
          raise AsyncRabbitMQ::ConnectionError.new(code: 0, text: "no") if i == 1
          true
        end
      end
      cluster.instance_variable_set(:@closed, false)

      expect { cluster.update_secret("rotated") }
        .to raise_error(AsyncRabbitMQ::ClusterError, /1 of 3 node/)

      # Every node stored the secret first, and the node after the failure was
      # still attempted rather than left on the old credential.
      expect(order.select { |(kind, _)| kind == :store }.size).to eq(3)
      expect(order.select { |(kind, _)| kind == :update }.map(&:last)).to eq([0, 1, 2])
      expect(cluster.sessions.map { |s| s.instance_variable_get(:@password) }).to eq(%w[rotated rotated rotated])
    end
  end
end
