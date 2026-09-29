require "spec_helper"

# AMQP replies carry nothing to match them to their request. A reply that
# arrives after the caller gave up would be collected by whoever asks next, so
# the channel is closed and reopened instead, discarding the late reply and
# re-registering consumers.
RSpec.describe "channel resync after an RPC timeout", :integration do

  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  # Swallow one expected reply so the next RPC on the channel times out with a
  # reply still in flight — the situation the resync exists for.
  def swallow_next_reply(ch)
    ch.define_singleton_method(:__swallowed, -> { @__swallowed })
    original = ch.method(:handle_method)
    ch.define_singleton_method(:handle_method) do |method|
      if !@__swallowed && method.is_a?(AMQ::Protocol::Queue::DeclareOk)
        @__swallowed = true
        next nil
      end
      original.call(method)
    end
  end

  it "reopens the channel and keeps it usable after a reply never arrives" do
    isolated_session(rpc_timeout: 1) do |session, _|
      ch = session.open_channel
      id = ch.channel_id
      swallow_next_reply(ch)

      expect { ch.queue("test.resync.#{SecureRandom.hex(4)}", durable: true) }
        .to raise_error(AsyncRabbitMQ::RpcTimeoutError)

      wait_until { ch.open? }
      expect(ch.channel_id).to eq(id)

      # The late DeclareOk must not be handed to this next request.
      q = ch.queue("test.resync.after.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("after resync", routing_key: q.name)
      sleep 0.3
      expect(ch.basic_get(q.name, manual_ack: false)[2]).to eq("after resync")
      ch.close
    end
  end

  it "bumps the delivery generation so tags from before the timeout are stale" do
    isolated_session(rpc_timeout: 1) do |session, _|
      ch = session.open_channel
      qname = "test.resync.gen.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)
      ch.basic_publish("x", routing_key: q.name)
      sleep 0.3
      di, = ch.basic_get(qname, manual_ack: true)
      stale = di.delivery_tag

      swallow_next_reply(ch)
      expect { ch.queue("test.resync.gen2.#{SecureRandom.hex(4)}", durable: true) }
        .to raise_error(AsyncRabbitMQ::RpcTimeoutError)
      wait_until { ch.open? }

      expect(ch.basic_ack(stale)).to be false
      expect(ch.open?).to be true
      ch.close
    end
  end

  it "re-registers consumers across the reopen" do
    isolated_session(rpc_timeout: 1) do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 5)
      qname = "test.resync.consumer.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)

      got = []
      ch.basic_consume(qname, manual_ack: false) { |_di, _h, b| got << b }

      swallow_next_reply(ch)
      expect { ch.queue("test.resync.other.#{SecureRandom.hex(4)}", durable: true) }
        .to raise_error(AsyncRabbitMQ::RpcTimeoutError)
      wait_until { ch.open? }

      # The consumer came back with the channel, so deliveries resume.
      ch.basic_publish("after", routing_key: qname)
      wait_until { got.include?("after") }
      expect(got).to include("after")
      ch.close
    end
  end
end
