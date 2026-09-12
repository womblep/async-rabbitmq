require "spec_helper"

# basic_get defaults to manual acknowledgement (Bunny parity): a fetched
# message that is never acked goes back to the queue instead of being lost.
RSpec.describe "basic_get acknowledgement default and Queue#pop", :integration do

  def ready_count(session, name)
    session.with_channel { |ch| ch.queue(name, passive: true).message_count }
  end

  it "requeues an unacked message when the channel closes (default manual_ack: true)" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.default.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("keep me", routing_key: q.name)
      sleep 0.1

      di, _h, body = ch.basic_get(q.name)
      expect(body).to eq("keep me")
      expect(di.delivery_tag).to be_a(Integer)
      expect(ready_count(session, q.name)).to eq(0)   # unacked, not ready

      ch.close
      sleep 0.2
      expect(ready_count(session, q.name)).to eq(1)   # requeued
    end
  end

  it "removes the message once it is acked" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.ack.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("ack me", routing_key: q.name)
      sleep 0.1

      di, _h, _body = ch.basic_get(q.name)
      ch.basic_ack(di.delivery_tag)
      ch.close
      sleep 0.2
      expect(ready_count(session, q.name)).to eq(0)
    end
  end

  it "discards the message on fetch with manual_ack: false" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.noack.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("gone", routing_key: q.name)
      sleep 0.1

      _di, _h, body = ch.basic_get(q.name, manual_ack: false)
      expect(body).to eq("gone")
      ch.close
      sleep 0.2
      expect(ready_count(session, q.name)).to eq(0)
    end
  end

  it "Queue#pop (alias get) fetches through the channel and returns nil when empty" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.pop.#{SecureRandom.hex(4)}", durable: true)
      q.publish("popped")
      sleep 0.1

      _di, _h, body = q.pop(manual_ack: false)
      expect(body).to eq("popped")
      expect(q.pop).to be_nil
      expect(q.get).to be_nil
      ch.close
    end
  end
end
