# frozen_string_literal: true

require_relative "helper"

RSpec.describe Async::RabbitMQ::Client, :integration do
  def with_client(port: RABBITMQ_PORT, endpoint_options: {}, **options)
    with_vhost do |vhost|
      Async::RabbitMQ::Client.open(endpoint(vhost: vhost, port: port, **endpoint_options),
                                   reconnect_interval: 0.1, **options) do |client|
        yield client, vhost
      end
    end
  end

  def current_connection(client)
    client.instance_variable_get(:@connection)
  end

  def mgmt_vhost(vhost)
    URI.encode_www_form_component(vhost)
  end

  describe ".open" do
    it "yields a connected client and closes it afterwards" do
      held = nil
      with_client do |client, _|
        held = client
        expect(client).to be_connected
        expect(client).not_to be_closed
      end
      expect(held).to be_closed
      expect(held).not_to be_connected
    end

    it "closes the client when the block raises" do
      held = nil
      expect do
        with_client do |client, _|
          held = client
          raise ArgumentError, "application error"
        end
      end.to raise_error(ArgumentError)
      expect(held).to be_closed
    end

    it "returns the client when there is no block, and accepts a URL" do
      with_vhost do |vhost|
        client = Async::RabbitMQ::Client.open("amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}/#{vhost}")
        expect(client).to be_connected
        expect(client.endpoint.vhost).to eq(vhost)
        client.close
        client.close # twice is harmless
      end
    end

    it "raises on the first connection, without retrying, when the password is wrong" do
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect { Async::RabbitMQ::Client.open(endpoint(password: "wrong")) }
        .to raise_error(Async::RabbitMQ::AuthenticationError)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 3
    end
  end

  describe "declaring" do
    # RabbitMQ 4 refuses a queue that is neither durable nor exclusive, and
    # closes the whole connection to say so. The client will not ask for one.
    it "refuses a queue that is not durable before the broker can" do
      with_client do |client, _|
        connection = client.connection
        expect { client.queue("transient", durable: false) }.to raise_error(ArgumentError, /neither durable nor/)
        expect { client.queue("transient", durable: false, auto_delete: true) }.to raise_error(ArgumentError)
        expect(client.connection).to equal(connection)
        expect(client.topology).to be_empty
      end
    end

    # The guard above covers the case that is known. This is for the rest:
    # the same refusal, sent past the guard.
    it "raises, and does not retry for ever, when a request costs the connection each time" do
      with_client do |client, _|
        declare = lambda do |channel|
          channel.call(AMQ::Protocol::Queue::Declare.encode(channel.id, "transient", false, false, false, false,
                                                            false, {}), AMQ::Protocol::Queue::DeclareOk)
        end
        expect { client.send(:admin, &declare) }
          .to raise_error(Async::RabbitMQ::ConnectionError, /transient_nonexcl_queues/)
        wait_for { client.connected? }
        expect(client.queue("fine").exists?).to be true
        expect(client.topology.queues.map(&:name)).to eq(%w[fine])
      end
    end

    it "declares a durable queue by default and reports its status" do
      with_client do |client, _|
        queue = client.queue("orders")
        expect(queue.name).to eq("orders")
        expect(queue).to be_durable
        expect(queue.exists?).to be true
        expect(queue.status.to_a).to eq([0, 0])
        expect(client.queue?("orders")).to be true
        expect(client.queue?("nothing-here")).to be false
        expect(client.queue?("orders")).to be true # the failed check cost nothing lasting
      end
    end

    it "declares a quorum queue when asked" do
      with_client do |client, vhost|
        client.queue("ledger", type: :quorum)
        wait_for { http_get("/api/queues/#{mgmt_vhost(vhost)}").any? { |q| q["name"] == "ledger" } }
        info = http_get("/api/queues/#{mgmt_vhost(vhost)}").find { |q| q["name"] == "ledger" }
        expect(info["type"]).to eq("quorum")
      end
    end

    it "declares exchanges, binds queues and exchanges, and routes through them" do
      with_client do |client, _|
        events = client.exchange("events", :topic)
        audit  = client.exchange("audit", :fanout).bind(events, routing_key: "#")
        orders = client.queue("orders").bind(events, routing_key: "order.*")
        log    = client.queue("log").bind(audit)
        expect(events.type).to eq(:topic)
        expect(events.exists?).to be true
        expect(client.exchange?("nope")).to be false

        channel = client.open_channel
        frames  = AMQ::Protocol::Basic::Publish.encode(channel.id, "x", { delivery_mode: 2 }, "events",
                                                       "order.created", false, false, 131_072)
        channel.write_content(frames.map(&:encode).join)

        wait_for { orders.message_count == 1 && log.message_count == 1 }
        expect(orders.purge).to eq(1)
        orders.unbind(events, routing_key: "order.*")
        expect(client.topology.queue_bindings.map(&:destination)).to eq(%w[log])
      end
    end

    it "deletes a queue and an exchange, and forgets them" do
      with_client do |client, _|
        events = client.exchange("events", :topic)
        orders = client.queue("orders").bind(events, routing_key: "#")
        orders.delete
        events.delete

        expect(client.queue?("orders")).to be false
        expect(client.exchange?("events")).to be false
        expect(client.topology).to be_empty
      end
    end

    it "raises the broker's refusal, and carries on working afterwards" do
      with_client do |client, _|
        client.queue("orders")
        expect { client.queue("orders", arguments: { "x-message-ttl" => 1000 }) }
          .to raise_error(Async::RabbitMQ::ChannelError) { |error| expect(error).to be_precondition_failed }
        expect(client.queue("other").exists?).to be true
        expect(client).to be_connected
      end
    end

    it "needs a name for a queue declared on the client" do
      with_client do |client, _|
        expect { client.queue("") }.to raise_error(ArgumentError, /name/)
        expect { client.exchange("") }.to raise_error(ArgumentError, /default_exchange/)
        expect(client.default_exchange).to be_default
      end
    end
  end

  describe "when the connection is lost" do
    it "reconnects, and what was waiting for a connection gets the new one" do
      with_client do |client, _|
        first = current_connection(client)
        sever!(first)
        channel = client.open_channel # waits out the reconnect

        expect(channel).to be_open
        expect(client).to be_connected
        expect(current_connection(client)).not_to equal(first)
        expect(first).to be_closed
      end
    end

    it "carries a request across the reconnect" do
      with_client do |client, _|
        client.queue("before")
        sever!(current_connection(client))
        expect(client.queue("after").exists?).to be true
        expect(client.queue?("before")).to be true
      end
    end

    it "declares again what the broker has lost" do
      with_client do |client, vhost|
        events = client.exchange("events", :topic)
        orders = client.queue("orders").bind(events, routing_key: "order.*")
        first  = current_connection(client)

        # Stand-in for a broker that came back without them.
        http_delete("/api/queues/#{mgmt_vhost(vhost)}/orders")
        http_delete("/api/exchanges/#{mgmt_vhost(vhost)}/events")
        wait_for { !client.queue?("orders") }
        sever!(first)
        wait_for { client.connected? && !current_connection(client).equal?(first) }

        expect(orders.exists?).to be true
        expect(events.exists?).to be true
        bindings = http_get("/api/queues/#{mgmt_vhost(vhost)}/orders/bindings")
        expect(bindings.map { |b| [b["source"], b["routing_key"]] }).to include(["events", "order.*"])
      end
    end

    it "does not declare again what was deleted through it, or what it was told to forget" do
      with_client do |client, vhost|
        client.queue("deleted").delete
        gone = client.queue("forgotten")
        http_delete("/api/queues/#{mgmt_vhost(vhost)}/forgotten")
        client.forget(gone)
        first = current_connection(client)

        sever!(first)
        wait_for { client.connected? && !current_connection(client).equal?(first) }

        expect(client.queue?("deleted")).to be false
        expect(client.queue?("forgotten")).to be false
      end
    end

    it "skips a declaration the broker now refuses, and still reconnects" do
      with_client do |client, vhost|
        client.queue("changed")
        client.queue("fine")
        http_delete("/api/queues/#{mgmt_vhost(vhost)}/changed")
        http_put("/api/queues/#{mgmt_vhost(vhost)}/changed", { durable: true, arguments: { "x-max-length" => 5 } }.to_json)
        first = current_connection(client)

        sever!(first)
        wait_for { client.connected? && !current_connection(client).equal?(first) }
        expect(client.queue?("fine")).to be true
      end
    end

    it "keeps trying while the broker is unreachable, and reconnects when it returns" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        first   = current_connection(client)
        waiting = nil
        toxiproxy_rabbitmq.down do
          wait_for { !client.connected? }
          waiting = Async { client.open_channel }
          sleep 0.6
          expect(waiting).to be_running
          expect(client).not_to be_closed
        end

        expect(waiting.wait).to be_open
        expect(current_connection(client)).not_to equal(first)
      end
    end

    it "gives up after reconnect_attempts, and tells everything that was waiting" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }, reconnect_attempts: 2) do |client, _|
        toxiproxy_rabbitmq.down do
          wait_for { !client.connected? }
          expect { client.open_channel }.to raise_error(Async::RabbitMQ::ConnectionError, /Gave up/)
          expect(client.failure).to be_a(Async::RabbitMQ::ConnectionError)
          expect { client.queue("x") }.to raise_error(Async::RabbitMQ::ConnectionError, /Gave up/)
        end
      end
    end
  end

  describe "telling the application what happens to the connection" do
    it "says when the connection is lost and when it is back, and not for the first connection" do
      with_client do |client, _|
        events = []
        client.on_connection_lost { |error, from| events << [:lost, error.class, from] }
        client.on_reconnected { |attempts, from| events << [:reconnected, attempts, from] }
        first = current_connection(client)
        sever!(first)
        client.open_channel

        expect(events).to eq([[:lost, Async::RabbitMQ::ConnectionError, client], [:reconnected, 1, client]])
      end
    end

    it "says each time an attempt to reconnect fails, the last one before giving up included" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }, reconnect_attempts: 2) do |client, _|
        failed = []
        back   = []
        client.on_reconnect_failed { |error, attempt| failed << [error.class.ancestors.include?(Async::RabbitMQ::Error), attempt] }
        client.on_reconnected { |attempts| back << attempts }
        gave_up = []
        client.on_gave_up { |error, attempts, from| gave_up << [error.is_a?(Async::RabbitMQ::Error), attempts, from.failure.class] }
        toxiproxy_rabbitmq.down do
          expect { client.open_channel }.to raise_error(Async::RabbitMQ::ConnectionError, /Gave up/)
        end

        expect(failed).to eq([[true, 1], [true, 2]])
        expect(gave_up).to eq([[true, 2, Async::RabbitMQ::ConnectionError]])
        expect(back).to be_empty
      end
    end

    it "says when the broker blocks publishing and when it stops" do
      with_client do |client, _|
        events = []
        client.on_blocked { |reason| events << [:blocked, reason] }
        client.on_unblocked { |from| events << [:unblocked, from] }
        connection = current_connection(client)
        # A resource alarm, as the broker would announce it.
        connection.send(:receive_connection_method, AMQ::Protocol::Connection::Blocked.new("low on memory"))
        connection.send(:receive_connection_method, AMQ::Protocol::Connection::Unblocked.new)

        expect(events).to eq([[:blocked, "low on memory"], [:unblocked, client]])
      end
    end

    it "calls every block given, and carries on when one raises" do
      with_client do |client, _|
        heard = []
        client.on_connection_lost { raise "a hook with a bug" }
        client.on_connection_lost { heard << :second }
        first = current_connection(client)
        sever!(first)

        expect(client.open_channel).to be_open
        expect(heard).to eq([:second])
        expect { client.on_reconnected }.to raise_error(ArgumentError, /needs a block/)
      end
    end
  end

  describe "#close" do
    it "releases a fiber that was waiting for a connection" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        toxiproxy_rabbitmq.down do
          wait_for { !client.connected? }
          waiting = Async do
            client.open_channel
          rescue Async::RabbitMQ::Error => error
            error
          end
          sleep 0.2
          client.close
          expect(waiting.wait).to be_a(Async::RabbitMQ::ClosedError)
        end
      end
    end

    it "closes what was registered with it, and refuses more" do
      with_client do |client, _|
        link = Class.new do
          attr_reader :closed

          def close = @closed = true
        end.new
        client.register(link)
        client.close

        expect(link.closed).to be true
        expect { client.register(link) }.to raise_error(Async::RabbitMQ::ClosedError)
        expect { client.open_channel }.to raise_error(Async::RabbitMQ::ClosedError)
        expect { client.connect }.to raise_error(Async::RabbitMQ::ClosedError)
      end
    end

    it "does not reconnect after it" do
      with_client do |client, _|
        connection = current_connection(client)
        client.close
        sleep 0.4
        expect(connection).to be_closed
        expect(client).not_to be_connected
      end
    end
  end
end
