# frozen_string_literal: true

require_relative "helper"

# There is one broker to test against, so the "nodes" here are two ways in to
# it: directly, and through Toxiproxy, which can be taken down to stand for a
# node that has gone. Queues are therefore visible from both, which a real
# cluster also arranges; what differs is which connection a thing lives on.
RSpec.describe Async::RabbitMQ::Cluster, :integration do
  before { skip "toxiproxy not available" unless toxiproxy_available? }

  def with_cluster(**options)
    with_vhost do |vhost|
      endpoints = [endpoint(vhost: vhost), endpoint(vhost: vhost, port: TOXIPROXY_PORT, timeout: 1)]
      Async::RabbitMQ::Cluster.open(endpoints, reconnect_interval: 0.1, **options) do |cluster|
        yield cluster, cluster.clients[0], cluster.clients[1], vhost
      end
    end
  end

  # Something made by the block that landed on +client+. Placement is by
  # fewest links, at random between equals, so the second try is certain.
  def placed_on(client)
    spare = []
    loop do
      made = yield
      return made if made.client.equal?(client)

      spare << made
      raise "never placed on #{client.endpoint}" if spare.size > 3
    end
  ensure
    spare.each(&:close)
  end

  describe ".open" do
    it "connects to every node, and closes them all afterwards" do
      held = nil
      with_cluster do |cluster, direct, proxied, _|
        held = cluster
        expect(cluster).to be_connected
        expect(cluster.clients).to eq([direct, proxied])
        expect(cluster.clients).to all(be_connected)
        expect(direct.endpoint.port).to eq(RABBITMQ_PORT)
        expect(proxied.endpoint.port).to eq(TOXIPROXY_PORT)
      end
      expect(held).to be_closed
      expect(held.clients).to all(be_closed)
      expect { held.consumer("q") }.to raise_error(Async::RabbitMQ::ClosedError)
    end

    it "starts with the nodes that are up, and the rest join when they can" do
      with_vhost do |vhost|
        endpoints = [endpoint(vhost: vhost), endpoint(vhost: vhost, port: TOXIPROXY_PORT, timeout: 1)]
        cluster   = nil
        toxiproxy_rabbitmq.down do
          cluster = Async::RabbitMQ::Cluster.open(endpoints, reconnect_interval: 0.1)
          expect(cluster).to be_connected
          expect(cluster.clients.map(&:connected?)).to eq([true, false])
          # Everything goes to the node that is there.
          consumers = Array.new(3) { cluster.consumer }
          expect(consumers.map(&:client)).to all(equal(cluster.clients[0]))
        end

        wait_for { cluster.clients.all?(&:connected?) }
        # And the node that joined, having nothing, gets what comes next.
        expect(cluster.consumer.client).to equal(cluster.clients[1])
      ensure
        cluster&.close
      end
    end

    it "raises when no node can be reached, and when one refuses the credentials" do
      dead = [endpoint(port: 1, timeout: 2), endpoint(port: 2, timeout: 2)]
      expect { Async::RabbitMQ::Cluster.open(dead) }.to raise_error(Async::RabbitMQ::ConnectionError)
      expect { Async::RabbitMQ::Cluster.open([endpoint, endpoint(password: "wrong")]) }
        .to raise_error(Async::RabbitMQ::AuthenticationError)
      expect { Async::RabbitMQ::Cluster.new([]) }.to raise_error(ArgumentError, /at least one/)
    end
  end

  describe "placing" do
    it "shares consumers and publishers out among the nodes" do
      with_cluster do |cluster, direct, proxied, _|
        consumers  = Array.new(6) { cluster.consumer }
        expect(consumers.count { |consumer| consumer.client.equal?(direct) }).to eq(3)
        expect(consumers.count { |consumer| consumer.client.equal?(proxied) }).to eq(3)

        publishers = Array.new(4) { cluster.publisher }
        expect([direct, proxied].map(&:link_count)).to eq([5, 5])
        managed = cluster.managed_consumer
        expect(managed.client.link_count).to eq(6)

        # Closing one makes room where it was.
        gone = consumers.find { |consumer| !consumer.client.equal?(managed.client) }
        home = gone.client
        gone.close
        expect(home.link_count).to eq(4)
        expect(cluster.consumer.client).to equal(home)
        publishers.each(&:close)
      end
    end

    it "does not always start with the same node" do
      with_vhost do |vhost|
        endpoints = [endpoint(vhost: vhost), endpoint(vhost: vhost, port: TOXIPROXY_PORT)]
        ports = Array.new(12) do
          Async::RabbitMQ::Cluster.open(endpoints) { |cluster| cluster.consumer.client.endpoint.port }
        end
        expect(ports.uniq).to match_array([RABBITMQ_PORT, TOXIPROXY_PORT])
      end
    end
  end

  describe "declaring" do
    it "declares through one node and remembers on all" do
      with_cluster do |cluster, direct, proxied, _|
        events = cluster.exchange("events", :topic)
        work   = cluster.queue("work").bind(events, routing_key: "order.*")
        expect(direct.topology).to equal(proxied.topology)
        expect(cluster.topology.queues.map(&:name)).to eq(%w[work])
        expect(cluster.queue?("work")).to be true
        expect(cluster.exchange?("nope")).to be false

        events.publisher { |publisher| publisher.publish("hello", routing_key: "order.created") }
        expect(work.message_count).to eq(1)
        seen = nil
        work.consumer do |consumer|
          consumer.each do |message|
            seen = message.body
            message.ack
            consumer.stop
          end
        end
        expect(seen).to eq("hello")

        work.delete
        expect(cluster.topology.queues).to be_empty
        expect(cluster.default_exchange).to be_default
      end
    end

    it "goes on declaring through a node that is up when another is down" do
      with_cluster do |cluster, _direct, proxied, _|
        toxiproxy_rabbitmq.down do
          wait_for { !proxied.connected? }
          expect(cluster.queue("made-meanwhile").exists?).to be true
        end
      end
    end
  end

  it "sets a connection hook on every node, and says which node it was" do
    with_cluster do |cluster, direct, proxied, _|
      lost = []
      back = []
      expect(cluster.on_connection_lost { |_error, client| lost << client }).to equal(cluster)
      cluster.on_reconnected { |_attempts, client| back << client }
      sever!(proxied.instance_variable_get(:@connection))
      wait_for { back == [proxied] }

      expect(lost).to eq([proxied])
      expect(direct).to be_connected
    end
  end

  describe "when a node goes away" do
    it "keeps a consumer with its node by default, and it consumes again when the node is back" do
      with_cluster do |cluster, direct, proxied, _|
        consumer = placed_on(proxied) { cluster.consumer }
        seen     = []
        running  = Async do
          consumer.each do |message|
            seen << message.body
            message.ack
          end
        end
        wait_for { consumer.consumer_tag }

        toxiproxy_rabbitmq.down do
          wait_for { !proxied.connected? }
          sleep 0.3
          expect(consumer.client).to equal(proxied)
          expect(running).to be_running
          expect(direct.link_count).to eq(0)
        end
        wait_for { proxied.connected? && consumer.consumer_tag }
        direct.publisher { |publisher| publisher.publish("back", routing_key: consumer.queue_name) }
        wait_for { seen == %w[back] }
        consumer.stop
        expect(running.wait).to eq(:stopped)
      end
    end

    it "moves a consumer that asked to, declares its queue again there, and says so" do
      with_cluster do |cluster, direct, proxied, _|
        events   = cluster.exchange("events", :fanout)
        consumer = placed_on(proxied) { cluster.consumer(on_connection_loss: :move) }
        consumer.bind(events)
        original = consumer.queue_name
        attached = []
        consumer.on_attach { |c| attached << [c.client, c.queue_name] }
        seen    = []
        running = Async do
          consumer.each do |message|
            seen << message.body
            message.ack
          end
        end
        wait_for { consumer.consumer_tag }

        toxiproxy_rabbitmq.down do
          wait_for { consumer.client.equal?(direct) && consumer.consumer_tag }
          expect(consumer.queue_name).not_to eq(original)
          expect(attached).to eq([[direct, consumer.queue_name]])
          expect([direct, proxied].map(&:link_count)).to eq([1, 0])

          events.publisher { |publisher| publisher.publish("on the new node") }
          wait_for { seen == ["on the new node"] }
        end

        # It does not go back when the first node returns.
        wait_for { proxied.connected? }
        sleep 0.2
        expect(consumer.client).to equal(direct)
        consumer.stop
        expect(running.wait).to eq(:stopped)
      end
    end

    it "moves a managed consumer too" do
      with_cluster do |cluster, direct, proxied, _|
        work     = cluster.queue("work")
        consumer = placed_on(proxied) { cluster.managed_consumer(work, on_connection_loss: :move) }
        seen     = []
        running  = Async { consumer.run { |message| seen << message.body } }
        wait_for { consumer.consumer_tag }

        toxiproxy_rabbitmq.down do
          wait_for { consumer.client.equal?(direct) && consumer.consumer_tag }
          expect([direct, proxied].map(&:link_count)).to eq([1, 0])
          direct.publisher { |publisher| publisher.publish("still served", routing_key: "work") }
          wait_for { seen == ["still served"] }
        end
        consumer.stop
        expect(running.wait).to eq(:stopped)
      end
    end

    it "leaves a consumer that asked to move where it is when there is nowhere to go" do
      with_vhost do |vhost|
        only = [endpoint(vhost: vhost, port: TOXIPROXY_PORT, timeout: 1)]
        Async::RabbitMQ::Cluster.open(only, reconnect_interval: 0.1) do |cluster|
          consumer = cluster.consumer("work", exclusive: true, on_connection_loss: :move)
          seen     = []
          running  = Async do
            consumer.each do |message|
              seen << message.body
              message.ack
              consumer.stop
            end
          end
          wait_for { consumer.consumer_tag }
          toxiproxy_rabbitmq.down do
            wait_for { !cluster.connected? }
            sleep 0.3
            expect(running).to be_running
          end
          wait_for { consumer.consumer_tag }
          cluster.publisher { |publisher| publisher.publish("waited", routing_key: "work") }
          expect(running.wait).to eq(:stopped)
          expect(seen).to eq(%w[waited])
        end
      end
    end

    it "ends a consumer that asked for that" do
      with_cluster do |cluster, _direct, proxied, _|
        work     = cluster.queue("work")
        consumer = placed_on(proxied) { cluster.consumer(work, on_connection_loss: :end) }
        running  = Async { consumer.each(&:ack) }
        wait_for { consumer.consumer_tag }
        toxiproxy_rabbitmq.down do
          expect(running.wait).to eq(:connection_lost)
        end
      end
    end

    it "keeps a publisher with its node" do
      with_cluster do |cluster, _direct, proxied, _|
        work      = cluster.queue("work")
        publisher = placed_on(proxied) { cluster.publisher }
        waiting   = nil
        toxiproxy_rabbitmq.down do
          wait_for { !publisher.connected? }
          waiting = Async { publisher.publish("held", routing_key: "work") }
          sleep 0.3
          expect(waiting).to be_running
          expect(publisher.client).to equal(proxied)
        end
        expect(waiting.wait).to be true
        expect(work.message_count).to eq(1)
      end
    end
  end

  it "only lets a consumer of a cluster ask to move" do
    with_vhost do |vhost|
      Async::RabbitMQ::Client.open(endpoint(vhost: vhost)) do |client|
        expect { client.consumer(on_connection_loss: :move) }.to raise_error(ArgumentError, /Cluster/)
      end
    end
  end

  it "closes what was made through it" do
    consumer = publisher = nil
    with_cluster do |cluster, _, _, _|
      consumer  = cluster.consumer
      publisher = cluster.publisher
    end
    expect(consumer).to be_closed
    expect(publisher).to be_closed
  end
end
