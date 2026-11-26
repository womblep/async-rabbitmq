# frozen_string_literal: true

require_relative "helper"

RSpec.describe Async::RabbitMQ::Connection, :integration do
  P = AMQ::Protocol

  def declare(channel, name, **flags)
    channel.call(P::Queue::Declare.encode(channel.id, name, false, flags.fetch(:durable, false),
                                          flags.fetch(:exclusive, true), false, false, {}), P::Queue::DeclareOk)
  end

  def publish(channel, routing_key, body, exchange: "", mandatory: false, frame_max: 131_072)
    frames = [P::Basic::Publish.encode(channel.id, body, { delivery_mode: 1 }, exchange, routing_key, mandatory, false, frame_max)]
    channel.write_content(frames.flatten.map(&:encode).join)
  end

  describe ".open" do
    it "connects, negotiates the tune values and says what the broker is" do
      with_connection(heartbeat: 30, connection_name: "v1-spec") do |connection, _|
        expect(connection).to be_open
        expect(connection.heartbeat).to eq(30)
        expect(connection.frame_max).to eq(131_072)
        expect(connection.channel_max).to be_between(1, 2047)
        expect(connection.server_properties["product"]).to eq("RabbitMQ")
        expect(connection.error).to be_nil
      end
    end

    it "raises AuthenticationError for a wrong password, and leaves nothing open" do
      expect { Async::RabbitMQ::Connection.open(endpoint(password: "wrong")) }
        .to raise_error(Async::RabbitMQ::AuthenticationError) { |error| expect(error.code).to eq(403) }
    end

    it "raises ConnectionError for a virtual host that does not exist" do
      expect { Async::RabbitMQ::Connection.open(endpoint(vhost: "no-such-vhost-#{SecureRandom.hex(4)}")) }
        .to raise_error(Async::RabbitMQ::ConnectionError)
    end

    it "raises when nothing is listening" do
      expect { Async::RabbitMQ::Connection.open(endpoint(port: 1, timeout: 2)) }
        .to raise_error(StandardError) { |error|
          expect(error).to be_a(SystemCallError).or be_a(Async::RabbitMQ::ConnectionError)
        }
    end

    it "gives up after the endpoint's timeout when the peer never speaks AMQP" do
      server = TCPServer.new("127.0.0.1", 0)
      accepted = []
      acceptor = Async { loop { accepted << server.accept } }
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      expect { Async::RabbitMQ::Connection.open(endpoint(port: server.addr[1], timeout: 0.3)) }
        .to raise_error(Async::RabbitMQ::ConnectTimeoutError)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 2
    ensure
      acceptor&.stop
      accepted&.each(&:close)
      server&.close
    end

    it "does not keep its task alive: the reader and heartbeat are transient" do
      with_vhost do |vhost|
        connection = nil
        task = Async { connection = Async::RabbitMQ::Connection.open(endpoint(vhost: vhost)) }
        task.wait
        expect(task).to be_finished
        expect(connection).to be_open
        connection.close
      end
    end
  end

  describe "#close" do
    it "closes in an orderly way, and reports no error" do
      with_connection do |connection, _|
        connection.close
        expect(connection).to be_closed
        expect(connection.error).to be_nil
        expect(connection.wait).to be_nil
        connection.close # and again is harmless
      end
    end

    it "closes its channels and tells their delegates" do
      with_connection do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        connection.close

        expect(channel).to be_closed
        expect(delegate.of(:closed).size).to eq(1)
        expect { channel.write("x") }.to raise_error(Async::RabbitMQ::ConnectionError)
        expect { connection.open_channel }.to raise_error(Async::RabbitMQ::ConnectionError)
      end
    end
  end

  describe "when the connection is lost" do
    it "closes with the reason, releases a caller waiting for a reply, and wakes #wait" do
      with_connection do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        waiter   = Async { connection.wait }

        sever!(connection)

        expect(waiter.wait).to be_a(Async::RabbitMQ::ConnectionError)
        expect(connection).to be_closed
        expect(connection.error).to be_a(Async::RabbitMQ::ConnectionError)
        expect(channel).to be_closed
        expect(delegate.of(:closed).first.last).to be_a(Async::RabbitMQ::ConnectionError)
        expect { declare(channel, "q") }.to raise_error(Async::RabbitMQ::ConnectionError)
      end
    end

    it "reports the broker's own reason when the broker closes it" do
      name = "v1-force-#{SecureRandom.hex(4)}"
      with_connection(connection_name: name) do |connection, _|
        force_close_connection!(name)
        error = connection.wait

        expect(error).to be_a(Async::RabbitMQ::ConnectionError)
        expect(error.code).to eq(320)
      end
    end

    it "declares the peer dead when it goes silent, within twice the heartbeat" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_vhost do |vhost|
        connection = Async::RabbitMQ::Connection.open(endpoint(vhost: vhost, port: TOXIPROXY_PORT, heartbeat: 1))
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        error = nil
        blackhole(toxiproxy_rabbitmq) { error = connection.wait }

        expect(error).to be_a(Async::RabbitMQ::HeartbeatTimeoutError)
        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 6
      end
    end

    it "stays up through a quiet period when heartbeats are flowing" do
      with_connection(heartbeat: 1) do |connection, _|
        sleep 3.5
        expect(connection).to be_open
      end
    end
  end

  describe "channels" do
    it "answers a request with the broker's reply" do
      with_connection do |connection, _|
        channel = connection.open_channel
        ok = declare(channel, "v1.q")
        expect(ok.queue).to eq("v1.q")
        expect(ok.message_count).to eq(0)
      end
    end

    it "closes only the channel the broker closed, with the broker's reason" do
      with_connection do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        bad  = connection.open_channel(delegate)
        good = connection.open_channel

        expect do
          bad.call(P::Queue::Declare.encode(bad.id, "missing", true, false, false, false, false, {}), P::Queue::DeclareOk)
        end.to raise_error(Async::RabbitMQ::ChannelError) { |error|
          expect(error).to be_not_found
          expect(error.text).to include("missing")
        }

        expect(bad).to be_closed
        expect(bad.error).to be_a(Async::RabbitMQ::ChannelError)
        expect(delegate.of(:closed).size).to eq(1)
        expect { bad.write("x") }.to raise_error(Async::RabbitMQ::ClosedError)
        expect(good).to be_open
        expect(declare(good, "still-fine").queue).to eq("still-fine")
        expect(connection).to be_open
      end
    end

    it "gives a closed channel's id back for the next one" do
      with_connection do |connection, _|
        first = connection.open_channel
        id    = first.id
        first.close
        expect(first).to be_closed
        expect(connection.channel_count).to eq(0)
        expect(connection.open_channel.id).to eq(id)
      end
    end

    it "refuses to open more channels than the broker allows, without losing the connection" do
      with_connection(channel_max: 3) do |connection, _|
        3.times { connection.open_channel }
        expect { connection.open_channel }.to raise_error(Async::RabbitMQ::ChannelLimitError)
        expect(connection).to be_open
      end
    end

    it "serialises requests from several fibers on one channel" do
      with_connection do |connection, _|
        channel = connection.open_channel
        names   = Array.new(20) { |i| "v1.serial.#{i}" }
        replies = names.map { |name| Async { declare(channel, name).queue } }.map(&:wait)
        expect(replies).to eq(names)
      end
    end

    it "closes the channel when a reply does not come in time, and keeps the id back until the broker agrees" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_vhost do |vhost|
        connection = Async::RabbitMQ::Connection.open(endpoint(vhost: vhost, port: TOXIPROXY_PORT, heartbeat: 0))
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        id       = channel.id

        toxiproxy_rabbitmq.downstream(:latency, latency: 1500).apply do
          expect do
            channel.call(P::Queue::Declare.encode(id, "slow", false, false, true, false, false, {}),
                         P::Queue::DeclareOk, timeout: 0.3)
          end.to raise_error(Async::RabbitMQ::TimeoutError, /queue.declare-ok.*0.3s/)

          expect(channel).to be_closed
          expect(delegate.of(:closed).first.last).to be_a(Async::RabbitMQ::TimeoutError)
          # The late declare-ok is still on its way: the id is not free yet.
          expect(connection.channel_count).to eq(1)
        end

        wait_for { connection.channel_count.zero? } # the close-ok arrived
        expect(connection).to be_open
        replacement = connection.open_channel
        expect(replacement.id).to eq(id)
        expect(declare(replacement, "after").queue).to eq("after") # and got its own answer
        connection.close
      end
    end
  end

  describe "what the broker sends unasked" do
    it "hands a delivery to the delegate with its header and body" do
      with_connection do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        declare(channel, "v1.deliver")
        channel.call(P::Basic::Consume.encode(channel.id, "v1.deliver", "", false, true, false, false, {}),
                     P::Basic::ConsumeOk)
        publish(channel, "v1.deliver", "hello")

        wait_for { delegate.of(:deliver).any? }
        _, method, header, body = delegate.of(:deliver).first
        expect(method.routing_key).to eq("v1.deliver")
        expect(header.body_size).to eq(5)
        expect(body).to eq("hello")
      end
    end

    it "reassembles a body that spans several frames, and delivers an empty one" do
      with_connection(frame_max: 8192) do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        declare(channel, "v1.big")
        channel.call(P::Basic::Consume.encode(channel.id, "v1.big", "", false, true, false, false, {}),
                     P::Basic::ConsumeOk)
        big = Random.bytes(30_000)
        publish(channel, "v1.big", big, frame_max: connection.frame_max)
        publish(channel, "v1.big", "", frame_max: connection.frame_max)

        wait_for { delegate.of(:deliver).size == 2 }
        bodies = delegate.of(:deliver).map(&:last)
        expect(bodies.first).to eq(big)
        expect(bodies.first.encoding).to eq(Encoding::BINARY)
        expect(bodies.last).to eq("")
      end
    end

    it "hands a returned message and a confirm to the delegate, the return first" do
      with_connection do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        channel.call(P::Confirm::Select.encode(channel.id, false), P::Confirm::SelectOk)
        publish(channel, "v1.nowhere.#{SecureRandom.hex(4)}", "lost", mandatory: true)

        wait_for { delegate.of(:confirmed).any? }
        kinds = delegate.events.map(&:first)
        expect(kinds).to eq(%i[returned confirmed])
        expect(delegate.of(:returned).first[1].reply_code).to eq(312)
        expect(delegate.of(:returned).first.last).to eq("lost")
        expect(delegate.of(:confirmed).first.last.delivery_tag).to eq(1)
      end
    end

    it "tells the delegate when the broker cancels a consumer" do
      with_connection do |connection, _|
        delegate = V1Helpers::RecordingDelegate.new
        channel  = connection.open_channel(delegate)
        declare(channel, "v1.cancel", exclusive: false, durable: true)
        ok = channel.call(P::Basic::Consume.encode(channel.id, "v1.cancel", "", false, true, false, false, {}),
                          P::Basic::ConsumeOk)
        other = connection.open_channel
        other.call(P::Queue::Delete.encode(other.id, "v1.cancel", false, false, false), P::Queue::DeleteOk)

        wait_for { delegate.of(:cancelled).any? }
        expect(delegate.of(:cancelled).first.last.consumer_tag).to eq(ok.consumer_tag)
        expect(channel).to be_open
      end
    end

    it "survives a delegate that raises" do
      with_connection do |connection, _|
        delegate = Class.new do
          include Async::RabbitMQ::Channel::Delegate
          attr_reader :seen

          def deliver(_method, _header, body)
            (@seen ||= []) << body
            raise "delegate bug"
          end
        end.new
        channel = connection.open_channel(delegate)
        declare(channel, "v1.raise")
        channel.call(P::Basic::Consume.encode(channel.id, "v1.raise", "", false, true, false, false, {}),
                     P::Basic::ConsumeOk)
        2.times { |i| publish(channel, "v1.raise", "m#{i}") }

        wait_for { delegate.seen&.size == 2 }
        expect(connection).to be_open
      end
    end
  end
end
