require "spec_helper"

# Channel#reopen: after a server-initiated channel.close the channel can be
# reopened on the same connection with the same id, with prefetch, confirm
# and transactional settings restored (Bunny 3.0 parity).
RSpec.describe "Channel#reopen", :integration do

  def close_by_broker(ch)
    ch.basic_ack(4242)   # unknown delivery tag: 406, broker closes the channel
    20.times { break if ch.closed?; sleep 0.05 }
    expect(ch.closed?).to be true
  end

  it "reopens a broker-closed channel with the same id and makes it usable again" do
    isolated_session do |session, _|
      ch = session.open_channel
      id = ch.channel_id
      close_by_broker(ch)
      expect(session.instance_variable_get(:@channels)).not_to have_key(id)

      expect(ch.reopen).to equal(ch)
      expect(ch.open?).to be true
      expect(ch.channel_id).to eq(id)
      expect(session.instance_variable_get(:@channels)[id]).to equal(ch)

      q = ch.queue("test.reopen.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("after reopen", routing_key: q.name)
      sleep 0.1
      expect(ch.basic_get(q.name, manual_ack: false)[2]).to eq("after reopen")
      ch.close
    end
  end

  it "restores confirm mode and prefetch" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.reopen.settings.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select
      ch.basic_qos(prefetch_count: 2)
      close_by_broker(ch)
      ch.reopen

      # Confirm mode: wait_for_confirms only resolves if the broker acks.
      ch.basic_publish("confirmed after reopen", routing_key: q.name)
      expect(ch.wait_for_confirms).to be true

      # Prefetch 2: with manual acks and no acking, only 2 of 5 are delivered.
      4.times { |i| ch.basic_publish("m#{i}", routing_key: q.name) }
      expect(ch.wait_for_confirms).to be true
      delivered = []
      ch.basic_consume(q.name, manual_ack: true) { |di, _h, body| delivered << body }
      sleep 0.5
      expect(delivered.size).to eq(2)
      ch.close
    end
  end

  it "restores transactional mode" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.reopen.tx.#{SecureRandom.hex(4)}", durable: true)
      ch.tx_select
      close_by_broker(ch)
      ch.reopen
      ch.basic_publish("in tx", routing_key: q.name)
      expect { ch.tx_commit }.not_to raise_error   # 406 if the channel were not transactional
      expect(ch.using_tx?).to be true
      ch.close
    end
  end

  it "drops the old consumers by default and re-registers them on request" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.reopen.consumers.#{SecureRandom.hex(4)}", durable: true)
      received = []
      ch.basic_consume(q.name, manual_ack: false) { |_di, _h, body| received << body }
      close_by_broker(ch)

      ch.reopen
      expect(ch.instance_variable_get(:@consumers)).to be_empty
      ch.basic_publish("nobody listening", routing_key: q.name)
      sleep 0.3
      expect(received).to be_empty
      close_by_broker(ch)

      # Register again, close again, reopen with consumers recovered.
      ch.reopen
      ch.basic_consume(q.name, manual_ack: false) { |_di, _h, body| received << body }
      sleep 0.3   # drains "nobody listening"
      close_by_broker(ch)
      ch.reopen(recover_consumers: true)
      ch.basic_publish("heard", routing_key: q.name)
      sleep 0.3
      expect(received).to include("heard")
      ch.close
    end
  end

  it "refuses to reopen an open channel" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect { ch.reopen }.to raise_error(AsyncRabbitMQ::NotOpenError, /only a closed channel/)
      ch.close
    end
  end
end
