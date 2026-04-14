require "spec_helper"

# Tests for multi-host failover: hosts:, addresses:, from_uri with multiple
# URIs, and hosts_shuffle_strategy.
RSpec.describe "Multi-host failover", :integration do

  # ---------------------------------------------------------------------------
  # Address list construction
  # ---------------------------------------------------------------------------

  describe "address list construction" do
    it "builds a single-entry list from host: + port: (default)" do
      session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT)
      expect(session.addresses).to eq([[RABBITMQ_HOST, RABBITMQ_PORT]])
      session.connect
      expect(session.open?).to be true
      session.close
    ensure
      session&.close rescue nil
    end

    it "builds list from hosts: array using the default port" do
      session = AsyncRabbitMQ::Session.new(
        hosts: [RABBITMQ_HOST],
        port: RABBITMQ_PORT
      )
      expect(session.addresses).to eq([[RABBITMQ_HOST, RABBITMQ_PORT]])
      session.connect
      expect(session.open?).to be true
      session.close
    ensure
      session&.close rescue nil
    end

    it "builds list from addresses: array with explicit ports" do
      session = AsyncRabbitMQ::Session.new(
        addresses: ["#{RABBITMQ_HOST}:#{RABBITMQ_PORT}"]
      )
      expect(session.addresses).to eq([[RABBITMQ_HOST, RABBITMQ_PORT]])
      session.connect
      expect(session.open?).to be true
      session.close
    ensure
      session&.close rescue nil
    end

    it "addresses: entries without a port fall back to the port: default" do
      session = AsyncRabbitMQ::Session.new(
        addresses: [RABBITMQ_HOST],
        port: RABBITMQ_PORT
      )
      expect(session.addresses).to eq([[RABBITMQ_HOST, RABBITMQ_PORT]])
      session.connect
      expect(session.open?).to be true
      session.close
    ensure
      session&.close rescue nil
    end

    it "addresses: takes precedence over hosts:" do
      session = AsyncRabbitMQ::Session.new(
        addresses: ["#{RABBITMQ_HOST}:#{RABBITMQ_PORT}"],
        hosts: ["should-be-ignored"],
        port: 9999
      )
      expect(session.addresses).to eq([[RABBITMQ_HOST, RABBITMQ_PORT]])
      session.connect
      expect(session.open?).to be true
      session.close
    ensure
      session&.close rescue nil
    end
  end

  # ---------------------------------------------------------------------------
  # from_uri with multiple URI strings
  # ---------------------------------------------------------------------------

  describe "Session.from_uri with multiple URIs" do
    it "builds an address list from multiple URI strings" do
      session = AsyncRabbitMQ::Session.from_uri(
        "amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
        "amqp://guest:guest@otherhost:5673"
      )
      expect(session.addresses.length).to eq(2)
      expect(session.addresses[0]).to eq([RABBITMQ_HOST, RABBITMQ_PORT])
      expect(session.addresses[1]).to eq(["otherhost", 5673])
      # Credentials come from first URI
      expect(session.username).to eq("guest")
    end

    it "connects successfully when the reachable host is in the list" do
      session = AsyncRabbitMQ::Session.from_uri(
        "amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
        "amqp://guest:guest@unreachable-host:5672",
        hosts_shuffle_strategy: :none
      )
      session.connect
      expect(session.open?).to be true
      expect(session.host).to eq(RABBITMQ_HOST)
      session.close
    ensure
      session&.close rescue nil
    end

    it "raises ArgumentError when no URIs are given" do
      expect {
        AsyncRabbitMQ::Session.from_uri
      }.to raise_error(ArgumentError, /at least one URI/)
    end
  end

  # ---------------------------------------------------------------------------
  # Connect failover
  # ---------------------------------------------------------------------------

  describe "connect failover" do
    it "skips unreachable hosts and connects to the first reachable one" do
      session = AsyncRabbitMQ::Session.new(
        addresses: [
          "127.0.0.1:19999",          # closed port — ECONNREFUSED
          "#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
        ],
        hosts_shuffle_strategy: :none,
        logger: Logger.new(IO::NULL)
      )
      session.connect
      expect(session.open?).to be true
      expect(session.host).to eq(RABBITMQ_HOST)
      expect(session.port).to eq(RABBITMQ_PORT)
      session.close
    ensure
      session&.close rescue nil
    end

    it "raises ConnectionTimeoutError when all hosts are unreachable" do
      session = AsyncRabbitMQ::Session.new(
        addresses: ["127.0.0.1:19999", "127.0.0.1:19998"],
        hosts_shuffle_strategy: :none,
        logger: Logger.new(IO::NULL)
      )
      expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError, /tried/)
    end

    it "does not retry after AuthenticationError even with multiple hosts" do
      session = AsyncRabbitMQ::Session.new(
        addresses: [
          "#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
          "#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
        ],
        auth_mechanism: "NONEXISTENT-MECH",
        hosts_shuffle_strategy: :none,
        logger: Logger.new(IO::NULL)
      )
      expect { session.connect }.to raise_error(AsyncRabbitMQ::AuthenticationError)
    end
  end

  # ---------------------------------------------------------------------------
  # hosts_shuffle_strategy
  # ---------------------------------------------------------------------------

  describe "hosts_shuffle_strategy" do
    it ":none preserves address order" do
      addrs = ["a:1", "b:2", "c:3"]
      session = AsyncRabbitMQ::Session.new(
        addresses: addrs,
        hosts_shuffle_strategy: :none
      )
      # Access the private method to verify ordering
      order = session.send(:shuffled_addresses)
      expect(order).to eq([["a", 1], ["b", 2], ["c", 3]])
    end

    it ":shuffle randomizes address order" do
      addrs = (1..20).map { |i| "host#{i}:#{5670 + i}" }
      session = AsyncRabbitMQ::Session.new(
        addresses: addrs,
        hosts_shuffle_strategy: :shuffle
      )
      orders = 5.times.map { session.send(:shuffled_addresses) }
      # With 20 hosts, getting the exact same order 5 times is astronomically unlikely
      expect(orders.uniq.length).to be > 1
    end

    it "accepts a custom Proc strategy" do
      # Reverse order
      strategy = ->(list) { list.reverse }
      session = AsyncRabbitMQ::Session.new(
        addresses: ["a:1", "b:2", "c:3"],
        hosts_shuffle_strategy: strategy
      )
      order = session.send(:shuffled_addresses)
      expect(order).to eq([["c", 3], ["b", 2], ["a", 1]])
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery failover
  # ---------------------------------------------------------------------------

  describe "recovery with multiple hosts", :toxiproxy do
    it "tries all addresses during recovery" do
      skip "Toxiproxy not available" unless toxiproxy_available?

      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)

      recovery_fired = false
      session = AsyncRabbitMQ::Session.new(
        addresses: [
          "127.0.0.1:#{TOXIPROXY_PORT}",
          "#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
        ],
        vhost: vhost,
        hosts_shuffle_strategy: :none,
        recovery_interval: 0.3,
        recovery_max_interval: 1.0
      )
      session.on_recovery { |_s| recovery_fired = true }
      session.connect
      expect(session.open?).to be true

      # Cut toxiproxy — recovery should fail over to the direct host
      toxiproxy_rabbitmq.down do
        sleep 0.5
      end

      15.times do
        break if recovery_fired
        sleep 0.5
      end

      expect(session.open?).to be true
      expect(recovery_fired).to be true
      # After recovery, the session should be connected to the direct host
      expect(session.host).to eq(RABBITMQ_HOST)
      expect(session.port).to eq(RABBITMQ_PORT)
      session.close
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end

    it "recovery_attempts counts full cycles through all addresses, not individual hosts", :toxiproxy do
      skip "Toxiproxy not available" unless toxiproxy_available?

      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)

      attempt_numbers = []
      exhausted_fired = false

      session = AsyncRabbitMQ::Session.new(
        addresses: [
          "127.0.0.1:#{TOXIPROXY_PORT}",
          "127.0.0.1:19999",           # closed port
          "127.0.0.1:19998",           # closed port
        ],
        vhost: vhost,
        hosts_shuffle_strategy: :none,
        recovery_attempts: 2,
        recovery_interval: 0.2,
        recovery_max_interval: 0.5
      )
      session.on_recovery_attempt { |n| attempt_numbers << n }
      session.on_recovery_exhausted { |_s| exhausted_fired = true }
      session.connect
      expect(session.open?).to be true

      # Keep all hosts down so every address in every cycle fails
      toxiproxy_rabbitmq.down do
        sleep 5
      end

      expect(exhausted_fired).to be true
      # Exactly 2 attempts — each attempt tried all 3 addresses
      expect(attempt_numbers).to eq([1, 2])
      expect(session.closed?).to be true
      session.close rescue nil
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end
  end
end
