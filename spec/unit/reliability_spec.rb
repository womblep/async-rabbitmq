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
