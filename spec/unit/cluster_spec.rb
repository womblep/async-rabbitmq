require "spec_helper"

# What a Cluster settles without a broker: its options, the addresses it pins a
# Session to, and that it presents the whole Session API.
RSpec.describe AsyncRabbitMQ::Cluster do
  it "pins one Session to each address, in the order given" do
    cluster = described_class.new(addresses: %w[a:5672 b:5673 c])
    expect(cluster.addresses).to eq([["a", 5672], ["b", 5673], ["c", 5672]])
    expect(cluster.sessions.map { |s| [s.host, s.port] }).to eq(cluster.addresses)
    expect(cluster.sessions.map(&:addresses)).to all(have_attributes(size: 1))   # no failover inside a node
    expect([cluster.host, cluster.port]).to eq(["a", 5672])
  end

  it "accepts hosts: with one port, and host:/port: as a cluster of one" do
    expect(described_class.new(hosts: %w[a b], port: 5673).addresses).to eq([["a", 5673], ["b", 5673]])
    expect(described_class.new(host: "solo").addresses).to eq([["solo", 5672]])
    expect(described_class.new.addresses).to eq([["localhost", 5672]])
  end

  it "builds one node per URI with from_uri, connection settings from the first" do
    cluster = described_class.from_uri("amqp://u:p@a:5672/vh", "amqp://b:5673/other")
    expect(cluster.addresses).to eq([["a", 5672], ["b", 5673]])
    expect(cluster.vhost).to eq("vh")
    expect(cluster.username).to eq("u")
    expect(cluster.sessions.map(&:vhost)).to eq(%w[vh vh])
  end

  it "rejects an unknown on_node_down policy and auto_recover: false" do
    expect { described_class.new(on_node_down: :panic) }.to raise_error(ArgumentError, /on_node_down/)
    expect { described_class.new(auto_recover: false) }.to raise_error(ArgumentError, /auto_recover/)
    expect(described_class.new(on_node_down: :drop)).to be_a(described_class)
    expect(described_class.new(on_node_down: ->(_session, _channels, _error) {})).to be_a(described_class)
  end

  it "shares one notifier with every node, so one subscription sees them all" do
    cluster = described_class.new(addresses: %w[a b])
    expect(cluster.sessions.map(&:notifier)).to all(equal(cluster.notifier))
    cluster.on_event("connection.") { |_name, _payload| }
    expect(cluster.notifier.subscriber_count).to eq(1)

    seen = []
    with_instrumenter = described_class.new(addresses: %w[a b], instrumenter: ->(name, payload) { seen << [name, payload] })
    expect(with_instrumenter.notifier.subscriber_count).to eq(1)
  end

  it "is closed, not open, and has nothing to hand out before connect" do
    cluster = described_class.new(addresses: %w[a b])
    expect(cluster.closed?).to be true
    expect(cluster.open?).to be false
    expect { cluster.open_channel }.to raise_error(AsyncRabbitMQ::NotOpenError, /No cluster node/)
    expect { cluster.update_secret("x") }.to raise_error(AsyncRabbitMQ::NotOpenError)
    expect(cluster.channels).to eq([])
    expect(cluster.channel_count).to eq(0)
    expect(cluster.topology).to be_empty
    expect(cluster.topology.queues).to eq([])
    expect(cluster.topology.consumers).to eq({})
    expect(cluster.viable?).to be false
    expect(cluster.reusable?).to be false
    expect(cluster.concurrency).to eq(2 * 2047)
    expect(cluster.frame_max).to eq(131_072)
    cluster.close                                        # a no-op before connect
    expect(cluster.closed?).to be true
  end

  it "stores a rotated secret on every node" do
    cluster = described_class.new(addresses: %w[a b])
    cluster.store_secret("rotated")
    expect(cluster.sessions.map { |s| s.instance_variable_get(:@password) }).to eq(%w[rotated rotated])
  end

  it "presents every public Session method" do
    internal = %i[channel_closed reopen_channel queue_renamed trigger_recovery spawn_background]
    expected = AsyncRabbitMQ::Session.public_instance_methods(false) - internal
    missing  = expected.reject { |m| described_class.method_defined?(m) }
    expect(missing).to eq([])
  end
end
