# frozen_string_literal: true

require_relative "helper"

RSpec.describe Async::RabbitMQ::Publisher, :integration do
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

  # Everything in +queue+, once there are +count+ of them: [body, properties].
  def messages_in(client, queue, count)
    delegate = V1Helpers::RecordingDelegate.new
    channel  = client.open_channel(delegate)
    channel.call(AMQ::Protocol::Basic::Consume.encode(channel.id, queue.to_s, "", false, true, false, false, {}),
                 AMQ::Protocol::Basic::ConsumeOk)
    wait_for { delegate.of(:deliver).size >= count }
    delegate.of(:deliver).map { |_, _method, header, body| [body, header.properties] }
  ensure
    channel&.close
  end

  describe "#publish" do
    it "returns once the broker has the message" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher

        expect(publisher.publish("hello", routing_key: orders)).to be true
        expect(orders.message_count).to eq(1)
        expect(publisher.unconfirmed_count).to eq(0)
        expect(publisher).to be_connected
      end
    end

    it "sends the body and the properties given" do
      with_client do |client, _|
        orders = client.queue("orders")
        client.publisher.publish("payload", routing_key: "orders", content_type: "application/json",
                                            headers: { "tenant" => "acme" }, correlation_id: "c-1",
                                            reply_to: "replies", message_id: "m-1", type: "order.created",
                                            app_id: "spec", priority: 3, expiration: 60_000)

        body, properties = messages_in(client, orders, 1).first
        expect(body).to eq("payload")
        expect(properties).to include(content_type: "application/json", correlation_id: "c-1", reply_to: "replies",
                                      message_id: "m-1", type: "order.created", app_id: "spec", priority: 3,
                                      expiration: "60000", delivery_mode: 2)
        expect(properties[:headers]).to eq("tenant" => "acme")
      end
    end

    it "publishes through a named exchange, and from the exchange itself" do
      with_client do |client, _|
        events = client.exchange("events", :topic)
        orders = client.queue("orders").bind(events, routing_key: "order.*")

        client.publisher(events).publish("one", routing_key: "order.created")
        events.publisher { |publisher| publisher.publish("two", routing_key: "order.paid", persistent: false) }

        received = messages_in(client, orders, 2)
        expect(received.map(&:first)).to eq(%w[one two])
        expect(received.map { |_, properties| properties[:delivery_mode] }).to eq([2, 1])
      end
    end

    it "lets many fibers publish at once, each waiting for its own message" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        tasks     = Array.new(200) { |i| Async { publisher.publish("m#{i}", routing_key: "orders") } }

        expect(tasks.map(&:wait)).to all(be true)
        expect(orders.message_count).to eq(200)
      end
    end

    it "raises NackedError when the broker refuses the message" do
      with_client do |client, _|
        client.queue("small", arguments: { "x-max-length" => 1, "x-overflow" => "reject-publish" })
        publisher = client.publisher

        publisher.publish("fits", routing_key: "small")
        expect { publisher.publish("does not", routing_key: "small") }
          .to raise_error(Async::RabbitMQ::NackedError)
        expect(publisher).to be_connected
      end
    end

    it "raises ReturnedError for a mandatory message no queue takes" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher(mandatory: true)

        expect { publisher.publish("lost", routing_key: "nowhere") }
          .to raise_error(Async::RabbitMQ::ReturnedError) { |error|
            expect(error.code).to eq(312)
            expect(error.routing_key).to eq("nowhere")
          }
        expect(publisher.publish("found", routing_key: "orders", headers: { "a" => 1 })).to be true
        expect(publisher.publish("unchecked", routing_key: "nowhere", mandatory: false)).to be true

        _, properties = messages_in(client, orders, 1).first
        expect(properties[:headers]).to include("a" => 1)
        expect(properties[:headers]).to have_key(described_class::PUBLISH_ID_HEADER)
        expect(publisher.unconfirmed_count).to eq(0)
      end
    end

    it "tells returned and delivered messages apart when they are in flight together" do
      with_client do |client, _|
        client.queue("orders")
        publisher = client.publisher(mandatory: true)
        outcomes  = Array.new(60) { |i| publisher.submit("m#{i}", routing_key: i.even? ? "orders" : "nowhere") }
        outcomes.each { |outcome| outcome.wait rescue nil }

        expect(outcomes.each_slice(2).map { |pair| pair.map(&:status) }).to all(eq(%i[acked returned]))
      end
    end

    it "raises ChannelError for an exchange that does not exist, and stays usable" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher

        expect { publisher.publish("x", exchange: "no-such-exchange", routing_key: "k") }
          .to raise_error(Async::RabbitMQ::ChannelError) { |error| expect(error).to be_not_found }
        expect(publisher.publish("after", routing_key: "orders")).to be true
        expect(orders.message_count).to eq(1)
      end
    end

    it "wants a String" do
      with_client do |client, _|
        expect { client.publisher.publish(42, routing_key: "x") }.to raise_error(ArgumentError, /String/)
      end
    end
  end

  describe "#submit" do
    it "returns an outcome that settles when the broker answers" do
      with_client do |client, _|
        client.queue("orders")
        outcome = client.publisher.submit("hello", routing_key: "orders")

        expect(outcome.wait).to equal(outcome)
        expect(outcome).to be_acked
        expect(outcome.error).to be_nil
        expect(outcome.wait(timeout: 1)).to equal(outcome) # again is harmless
      end
    end

    it "keeps the order in which messages were submitted" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        outcomes  = Array.new(500) { |i| publisher.submit(i.to_s, routing_key: "orders") }
        outcomes.each(&:wait)

        expect(messages_in(client, orders, 500).map(&:first)).to eq((0...500).map(&:to_s))
      end
    end

    it "sends what it was given, whatever the caller does to it afterwards" do
      with_client do |client, _|
        orders  = client.queue("orders")
        body    = +"original"
        headers = { "k" => "v" }
        outcome = client.publisher.submit(body, routing_key: "orders", headers: headers)
        body << " changed"
        headers["k"] = "changed"
        outcome.wait

        received, properties = messages_in(client, orders, 1).first
        expect(received).to eq("original")
        expect(properties[:headers]).to eq("k" => "v")
      end
    end

    it "waits for room once max_unconfirmed messages are unanswered" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher(max_unconfirmed: 5)
        highest   = 0
        outcomes  = Array.new(100) do |i|
          publisher.submit(i.to_s, routing_key: "orders").tap do
            highest = [highest, publisher.unconfirmed_count].max
          end
        end
        outcomes.each(&:wait)

        expect(highest).to be <= 5
        expect(orders.message_count).to eq(100)
      end
    end

    it "raises TimeoutError from wait when no answer comes in time, and can be waited on again" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        client.queue("orders")
        publisher = client.publisher(resend: true)
        outcome   = nil
        blackhole(toxiproxy_rabbitmq, direction: :upstream) do
          outcome = publisher.submit("late", routing_key: "orders")
          expect { outcome.wait(timeout: 0.2) }.to raise_error(Async::RabbitMQ::TimeoutError)
          expect(outcome).to be_pending
          sever!(current_connection(client))
        end
        expect(outcome.wait(timeout: 10)).to be_acked
      end
    end
  end

  describe "when the connection is lost" do
    it "publishes on the next connection" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        publisher.publish("before", routing_key: "orders")

        sever!(current_connection(client))
        wait_for { !publisher.connected? }
        expect(publisher.publish("after", routing_key: "orders")).to be true
        expect(orders.message_count).to eq(2)
        expect(publisher).not_to be_resend
      end
    end

    it "fails what the broker had not answered for with UnconfirmedError, and sends none of it again" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        outcomes  = nil
        waiting   = nil
        blackhole(toxiproxy_rabbitmq, direction: :upstream) do
          outcomes = Array.new(5) { |i| publisher.submit("m#{i}", routing_key: "orders") }
          waiting  = Async do
            publisher.publish("blocking", routing_key: "orders")
          rescue Async::RabbitMQ::Error => error
            error
          end
          sleep 0.3
          sever!(current_connection(client))
        end

        outcomes.each do |outcome|
          expect { outcome.wait(timeout: 10) }.to raise_error(Async::RabbitMQ::UnconfirmedError) { |error|
            expect(error.reason).to be_a(Async::RabbitMQ::ConnectionError)
          }
        end
        expect(outcomes).to all(be_failed)
        expect(waiting.wait).to be_a(Async::RabbitMQ::UnconfirmedError)
        expect(publisher.unconfirmed_count).to eq(0)

        # The publisher carries on, and nothing came back on its own.
        expect(publisher.publish("after", routing_key: "orders")).to be true
        expect(orders.message_count).to eq(1)
      end
    end

    it "sends a message again when its outcome is resubmitted" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        outcomes  = nil
        blackhole(toxiproxy_rabbitmq, direction: :upstream) do
          outcomes = Array.new(3) do |i|
            publisher.submit("m#{i}", routing_key: "orders", headers: { "n" => i }, message_id: "id-#{i}")
          end
          sleep 0.3
          sever!(current_connection(client))
        end
        # The loop an application would write.
        outcomes.each do |outcome|
          outcome.wait(timeout: 10)
        rescue Async::RabbitMQ::UnconfirmedError
          expect(outcome.resubmit).to equal(outcome)
          expect(outcome.error).to be_nil
          retry
        end

        expect(outcomes).to all(be_acked)
        received = messages_in(client, orders, 3)
        expect(received.map(&:first)).to eq(%w[m0 m1 m2])
        expect(received.map { |_, properties| [properties[:headers]["n"], properties[:message_id]] })
          .to eq([[0, "id-0"], [1, "id-1"], [2, "id-2"]])
      end
    end

    it "keeps a message that had not gone out, and sends it on the next connection" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        waiting   = nil
        toxiproxy_rabbitmq.down do
          wait_for { !publisher.connected? }
          waiting = Async { publisher.submit("held", routing_key: "orders") }
          sleep 0.3
        end

        expect(waiting.wait.wait(timeout: 10)).to be_acked
        expect(orders.message_count).to eq(1)
      end
    end
  end

  describe "#resubmit" do
    it "sends a returned message again, and is refused for one that was acked" do
      with_client do |client, _|
        publisher = client.publisher(mandatory: true)
        returned  = publisher.submit("lost", routing_key: "orders", headers: { "a" => 1 })
        expect { returned.wait }.to raise_error(Async::RabbitMQ::ReturnedError)

        orders = client.queue("orders")
        expect(returned.resubmit).to equal(returned)
        expect(returned.wait).to be_acked
        _, properties = messages_in(client, orders, 1).first
        expect(properties[:headers]).to include("a" => 1)

        expect { returned.resubmit }.to raise_error(Async::RabbitMQ::Error, /confirmed/)
      end
    end

    it "can be returned again, and resubmitted again" do
      with_client do |client, _|
        publisher = client.publisher(mandatory: true)
        outcome   = publisher.submit("lost", routing_key: "orders")
        2.times do
          expect { outcome.wait }.to raise_error(Async::RabbitMQ::ReturnedError)
          outcome.resubmit
          expect(outcome).to be_pending
        end
        expect { outcome.wait }.to raise_error(Async::RabbitMQ::ReturnedError)
        expect(publisher.unconfirmed_count).to eq(0)
      end
    end

    it "is refused while the outcome is still pending" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        client.queue("orders")
        publisher = client.publisher
        blackhole(toxiproxy_rabbitmq, direction: :upstream) do
          outcome = publisher.submit("m", routing_key: "orders")
          expect { outcome.resubmit }.to raise_error(Async::RabbitMQ::Error, /still waiting/)
        end
      end
    end

    it "is refused once the publisher is closed" do
      with_client do |client, _|
        publisher = client.publisher(mandatory: true)
        returned  = publisher.submit("lost", routing_key: "nowhere")
        returned.wait rescue nil
        publisher.close
        expect { returned.resubmit }.to raise_error(Async::RabbitMQ::ClosedError)
        expect(returned).to be_returned # left as it was
      end
    end
  end

  describe "with resend: true" do
    it "sends again what the broker had not answered for, and the waiting fibers return" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher(resend: true)
        first     = current_connection(client)
        outcomes  = Array.new(300) { |i| publisher.submit(i.to_s, routing_key: "orders") }
        sever!(first)

        outcomes.each { |outcome| outcome.wait(timeout: 10) }
        expect(outcomes).to all(be_acked)
        expect(current_connection(client)).not_to equal(first)
        # At least once: a message taken before the cut may be there twice.
        bodies = messages_in(client, orders, 300).map(&:first)
        expect(bodies.uniq.size).to eq(300)
        expect(bodies.uniq).to eq((0...300).map(&:to_s)) # first appearances are in order
      end
    end

    it "sends again a message that never reached the broker" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher(resend: true)
        outcomes  = nil
        # Nothing reaches the broker, and then the link drops.
        blackhole(toxiproxy_rabbitmq, direction: :upstream) do
          outcomes = Array.new(5) { |i| publisher.submit("m#{i}", routing_key: "orders") }
          sleep 0.3
          expect(outcomes).to all(be_pending)
          sever!(current_connection(client))
        end

        outcomes.each { |outcome| outcome.wait(timeout: 10) }
        expect(orders.message_count).to eq(5) # once each: the first copies never arrived
        expect(messages_in(client, orders, 5).map(&:first)).to eq(%w[m0 m1 m2 m3 m4])
      end
    end

  end

  describe "during an outage" do
    it "keeps publishers waiting through an outage" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        waiting   = nil
        toxiproxy_rabbitmq.down do
          wait_for { !publisher.connected? }
          waiting = Async { publisher.publish("held", routing_key: "orders") }
          sleep 0.5
          expect(waiting).to be_running
        end

        expect(waiting.wait).to be true
        expect(orders.message_count).to eq(1)
      end
    end

    it "fails what was waiting when the client gives up" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }, reconnect_attempts: 2) do |client, _|
        client.queue("orders")
        publisher = client.publisher
        toxiproxy_rabbitmq.down do
          wait_for { !publisher.connected? }
          expect { publisher.publish("never", routing_key: "orders") }
            .to raise_error(Async::RabbitMQ::ConnectionError, /Gave up/)
          expect(publisher).to be_closed
        end
      end
    end
  end

  describe "#close" do
    it "waits for the broker to answer for what was sent" do
      with_client do |client, _|
        orders    = client.queue("orders")
        publisher = client.publisher
        outcomes  = Array.new(200) { |i| publisher.submit(i.to_s, routing_key: "orders") }
        publisher.close

        expect(outcomes).to all(be_acked)
        expect(orders.message_count).to eq(200)
        expect(publisher).to be_closed
        publisher.close # twice is harmless
      end
    end

    it "refuses anything more" do
      with_client do |client, _|
        publisher = client.publisher
        publisher.close
        expect { publisher.publish("x", routing_key: "y") }.to raise_error(Async::RabbitMQ::ClosedError)
        expect { publisher.submit("x", routing_key: "y") }.to raise_error(Async::RabbitMQ::ClosedError)
      end
    end

    it "gives its channel back" do
      with_client do |client, _|
        before    = current_connection(client).channel_count
        publisher = client.publisher
        expect(current_connection(client).channel_count).to eq(before + 1)
        publisher.close
        expect(current_connection(client).channel_count).to eq(before)
      end
    end

    it "is closed with its client, and by the block form" do
      held = nil
      with_client do |client, _|
        client.queue("orders")
        inner = client.publisher { |publisher| publisher.tap { publisher.publish("x", routing_key: "orders") } }
        expect(inner).to be_closed
        held = client.publisher
      end
      expect(held).to be_closed
    end

    it "fails a fiber waiting for a connection, without waiting itself" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
        publisher = client.publisher
        toxiproxy_rabbitmq.down do
          wait_for { !publisher.connected? }
          waiting = Async do
            publisher.publish("x", routing_key: "y")
          rescue Async::RabbitMQ::Error => error
            error
          end
          sleep 0.2
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          publisher.close
          expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
          expect(waiting.wait).to be_a(Async::RabbitMQ::ClosedError)
        end
      end
    end

    it "refuses a publisher on a closed client" do
      with_client do |client, _|
        client.close
        expect { client.publisher }.to raise_error(Async::RabbitMQ::ClosedError)
      end
    end
  end
end
