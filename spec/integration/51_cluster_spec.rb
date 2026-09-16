require "spec_helper"

# A Cluster pins one Session to each node address, opens each new channel on
# the node with the fewest, and treats a lost node by its on_node_down policy:
# park (wait for it, as a Session does), drop (close its channels at once so
# their fibers move on) or a callable that decides per channel. A node that is
# down, at startup or later, is retried until it is back and then takes new
# channels until the counts are level. The single test broker stands in for
# three nodes: two addresses straight to it and one through toxiproxy, which
# is the node that goes down.
RSpec.describe AsyncRabbitMQ::Cluster, :integration do

  def addresses
    ["#{RABBITMQ_HOST}:#{RABBITMQ_PORT}", "#{RABBITMQ_HOST}:#{RABBITMQ_PORT}", "#{RABBITMQ_HOST}:#{TOXIPROXY_PORT}"]
  end

  def cluster_for(vhost, **opts)
    described_class.new(addresses: addresses, vhost: vhost, recovery_interval: 0.2, recovery_max_interval: 0.5, **opts)
  end

  # An isolated vhost and a connected cluster, closed and deleted afterwards.
  def with_cluster(**opts)
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    cluster = cluster_for(vhost, **opts)
    cluster.connect
    yield cluster, vhost
  ensure
    cluster&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  def proxied(cluster) = cluster.sessions.find { |s| s.port == TOXIPROXY_PORT }
  def direct(cluster)  = cluster.sessions.reject { |s| s.port == TOXIPROXY_PORT }

  def wait_until(what, timeout: 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.05
    end
  end

  def queue_names(vhost)
    http_get("/api/queues/#{URI.encode_www_form_component(vhost)}").map { |q| q["name"] }
  end

  it "connects to every address and gives each new channel to the node with the fewest" do
    with_cluster do |cluster, _|
      expect(cluster.open?).to be true
      expect(cluster.closed?).to be false
      expect(cluster.sessions.map(&:open?)).to eq([true, true, true])

      channels = 6.times.map { cluster.open_channel }
      expect(cluster.sessions.map(&:channel_count)).to eq([2, 2, 2])
      expect(cluster.channel_count).to eq(6)
      expect(cluster.channels).to match_array(channels)

      # A closed channel frees its node, which is where the next ones go.
      cluster.sessions.first.channels.each(&:close)
      expect(cluster.sessions.map(&:channel_count)).to eq([0, 2, 2])
      2.times { cluster.open_channel }
      expect(cluster.sessions.map(&:channel_count)).to eq([2, 2, 2])

      cluster.with_channel { |ch| ch.queue("test.cluster.durable", durable: true) }
      expect(cluster.topology.queues.map(&:name)).to eq(["test.cluster.durable"])
      expect(cluster.queue_exists?("test.cluster.durable")).to be true
      expect(cluster.queue_exists?("test.cluster.missing")).to be false
      expect(cluster.exchange_exists?("amq.direct")).to be true

      expect(cluster.viable?).to be true
      expect(cluster.reusable?).to be true
      expect(cluster.concurrency).to eq(cluster.sessions.sum(&:concurrency))
      expect(cluster.frame_max).to eq(cluster.sessions.first.frame_max)

      cluster.close
      expect(cluster.closed?).to be true
      expect(cluster.open?).to be false
      expect(cluster.sessions).to all(be_closed)
      expect { cluster.open_channel }.to raise_error(AsyncRabbitMQ::NotOpenError)

      cluster.connect                                    # a closed cluster can connect again
      expect(cluster.sessions.map(&:open?)).to eq([true, true, true])
    end
  end

  it "builds one node per URI" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    cluster = described_class.from_uri(*addresses.map { |a| "amqp://guest:guest@#{a}/#{vhost}" })
    cluster.connect
    expect(cluster.addresses).to eq(addresses.map { |a| h, p = a.split(":"); [h, p.to_i] })
    expect(cluster.vhost).to eq(vhost)
    expect(cluster.sessions.map(&:open?)).to eq([true, true, true])
  ensure
    cluster&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "drop: closes the channels on a lost node at once, leaves the others alone, and fills the node again when it is back" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    with_cluster(on_node_down: :drop) do |cluster, vhost|
      downs = []
      ups   = []
      cluster.on_node_down { |session, channels, error| downs << [session, channels, error] }
      cluster.on_node_up   { |session| ups << session }

      channels  = 6.times.map { cluster.open_channel }
      node      = proxied(cluster)
      doomed    = node.channels
      survivors = channels - doomed
      expect(doomed.size).to eq(2)

      # Each doomed channel consumes its own temporary queue, as a per-client
      # worker does; its fiber returns from #each when the channel is dropped.
      finished = []
      doomed.each do |ch|
        q = ch.temporary_queue
        Async::Task.current.async do
          ch.each(q.name) { |*| }
          finished << ch
        end
      end
      survivors.each(&:temporary_queue)
      sleep 0.2
      expect(node.topology.queues.size).to eq(2)
      expect(queue_names(vhost).size).to eq(6)

      toxiproxy_rabbitmq.down do
        wait_until("the doomed channels to close") { doomed.all?(&:closed?) && finished.size == 2 }
        expect(finished).to match_array(doomed)
        expect(downs.size).to eq(1)
        session, dropped, error = downs.first
        expect(session).to equal(node)
        expect(dropped).to match_array(doomed)
        expect(error).to be_a(StandardError)
        expect(node.channel_count).to eq(0)
        expect(node.topology).to be_empty
        expect(node.topology.consumers).to be_empty
        expect { doomed.first.temporary_queue }.to raise_error(AsyncRabbitMQ::NotOpenError)

        # The other nodes never noticed.
        expect(survivors).to all(be_open)
        survivors.each { |ch| ch.basic_publish("still here", routing_key: "nowhere") }
        expect(cluster.open?).to be true

        # New channels avoid the node that is down.
        2.times { cluster.open_channel }
        expect(node.channel_count).to eq(0)
        expect(direct(cluster).map(&:channel_count)).to eq([3, 3])
      end

      # Back: the node reconnects with nothing recorded, so nothing is re-declared.
      wait_until("the node to reconnect") { node.open? && ups == [node] }
      wait_until("the dead temporary queues to be gone") { queue_names(vhost).size == 4 }
      expect(node.topology).to be_empty

      # It takes every new channel until the counts are level again.
      3.times { cluster.open_channel }
      expect(cluster.sessions.map(&:channel_count)).to eq([3, 3, 3])
    end
  end

  it "drop with clear_topology_on_drop: false keeps the Session's registry rules" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    with_cluster(on_node_down: :drop, clear_topology_on_drop: false) do |cluster, _|
      3.times { cluster.open_channel }
      node = proxied(cluster)
      ch   = node.channels.first
      ch.queue("test.cluster.kept", durable: true)
      q = ch.temporary_queue
      ch.basic_consume(q.name) { |*| }

      toxiproxy_rabbitmq.down do
        wait_until("the channel to be dropped") { ch.closed? }
        # The temporary queue went with its consumer; the durable one is kept.
        expect(node.topology.queues.map(&:name)).to eq(["test.cluster.kept"])
      end
      wait_until("the node to reconnect") { node.open? }
      expect(node.topology.queues.map(&:name)).to eq(["test.cluster.kept"])
      expect(cluster.queue_exists?("test.cluster.kept")).to be true
    end
  end

  it "park: the channels on a lost node wait, then resume on it with what they had parked" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    with_cluster do |cluster, _|                                # :park is the default
      3.times { cluster.open_channel }
      node = proxied(cluster)
      ch   = node.channels.first
      q    = ch.queue("test.cluster.park", durable: true)
      received = []
      q.subscribe(manual_ack: false) { |_di, _h, body| received << body }
      downs = []
      cluster.on_node_down { |session, channels, _error| downs << [session, channels] }

      published = false
      toxiproxy_rabbitmq.down do
        wait_until("the channel to start recovering") { ch.recovering? }
        expect(downs).to eq([[node, [ch]]])
        expect(node.channel_count).to eq(1)
        Async::Task.current.async do
          ch.basic_publish("after the outage", routing_key: q.name)   # parks until the node is back
          published = true
        end
        sleep 0.3
        expect(published).to be false
        cluster.open_channel                                          # goes to a node that is up
        expect(node.channel_count).to eq(1)
        expect(direct(cluster).map(&:channel_count)).to eq([2, 1])
      end

      wait_until("the node to recover") { ch.open? }
      wait_until("the parked publish to be delivered") { received == ["after the outage"] }
      expect(published).to be true
      expect(node.topology.queues.map(&:name)).to eq(["test.cluster.park"])
    end
  end

  it "callable: drops the channels it closes and parks the rest" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    seen = []
    policy = lambda do |session, channels, error|
      seen << [session, channels, error]
      channels.first.close                                 # this one is dropped; the other waits
    end
    with_cluster(on_node_down: policy) do |cluster, _|
      6.times { cluster.open_channel }
      node = proxied(cluster)
      dropped, kept = node.channels

      toxiproxy_rabbitmq.down do
        wait_until("the policy to run") { seen.size == 1 }
        expect(seen.first[0]).to equal(node)
        expect(seen.first[1]).to eq([dropped, kept])
        expect(dropped.closed?).to be true
        expect(kept.recovering?).to be true
        expect(node.channels).to eq([kept])
      end

      wait_until("the kept channel to recover") { kept.open? }
      kept.temporary_queue
      expect(dropped.closed?).to be true
    end
  end

  it "connects with a node down at startup and brings it in when it comes back" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    ups = []
    cluster = cluster_for(vhost)
    cluster.on_node_up { |session| ups << session }

    toxiproxy_rabbitmq.down do
      cluster.connect
      expect(cluster.open?).to be true
      expect(cluster.sessions.map(&:open?)).to eq([true, true, false])
      cluster.open_channel
      expect(cluster.sessions.map(&:channel_count)).to eq([1, 0, 0])
    end

    wait_until("the late node to connect") { proxied(cluster).open? }
    expect(ups).to eq([proxied(cluster)])
    2.times { cluster.open_channel }
    expect(cluster.sessions.map(&:channel_count)).to eq([1, 1, 1])
  ensure
    cluster&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "raises when no node can be reached, and at once when the credentials are refused" do
    unreachable = described_class.new(addresses: ["#{RABBITMQ_HOST}:1", "#{RABBITMQ_HOST}:2"], connect_timeout: 3)
    expect { unreachable.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError)
    expect(unreachable.closed?).to be true

    refused = described_class.new(addresses: addresses, password: "not-the-password", recovery_interval: 0.2)
    expect { refused.connect }.to raise_error(AsyncRabbitMQ::AuthenticationError)
    expect(refused.closed?).to be true
    expect(refused.sessions).to all(be_closed)
  end

  it "fans callbacks in from every node and streams every node's events through one subscription" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    events = []
    with_cluster(instrumenter: ->(name, payload) { events << [name, payload] }) do |cluster, _|
      opens = events.select { |name, _| name == "connection.open" }
      expect(opens.map { |_, p| p[:port] }).to match_array([RABBITMQ_PORT, RABBITMQ_PORT, TOXIPROXY_PORT])
      expect(cluster.sessions.map(&:notifier)).to all(equal(cluster.notifier))

      lost      = []
      attempts  = []
      recovered = []
      cluster.on_connection_lost  { |session, _error| lost << session }
      cluster.on_recovery_attempt { |n| attempts << n }
      cluster.on_recovery         { |session| recovered << session }
      node = proxied(cluster)

      toxiproxy_rabbitmq.down { wait_until("the loss to be reported") { lost == [node] } }
      wait_until("the node to recover") { recovered == [node] }
      expect(attempts.first).to eq(1)
      expect(events.map(&:first)).to include("connection.lost", "recovery.attempt", "recovery.succeeded")
      expect(events.select { |n, _| n == "connection.lost" }.map { |_, p| p[:port] }).to eq([TOXIPROXY_PORT])
    end
  end

  it "rotates the secret on every node, storing it for the ones that are down" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    with_cluster do |cluster, _|
      node = proxied(cluster)
      toxiproxy_rabbitmq.down do
        wait_until("the node to go down") { !node.open? }
        expect(cluster.update_secret("guest", "rotation")).to be true
      end
      expect(cluster.sessions.map { |s| s.instance_variable_get(:@password) }).to eq(%w[guest guest guest])
      wait_until("the node to recover") { node.open? }
    end
  end
end
