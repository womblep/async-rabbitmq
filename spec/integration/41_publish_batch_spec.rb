require "spec_helper"

# Channel#basic_publish_batch: many messages encoded into one buffer and
# written once, with confirm tags reserved as a block (Bunny 3.0 parity).
RSpec.describe "Channel#basic_publish_batch", :integration do

  def drain(ch, name)
    bodies = []
    while (msg = ch.basic_get(name, manual_ack: false))
      bodies << msg[2]
    end
    bodies
  end

  it "publishes every payload in order" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.batch.#{SecureRandom.hex(4)}", durable: true)
      payloads = (1..100).map { |i| "batch-#{i}" }

      expect(ch.basic_publish_batch(payloads, routing_key: q.name)).to be_nil   # no confirms => no tags
      sleep 0.3
      expect(drain(ch, q.name)).to eq(payloads)
      ch.close
    end
  end

  it "reserves consecutive confirm tags and confirms the whole batch" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.batch.confirm.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select
      ch.basic_publish("single", routing_key: q.name)   # tag 1

      tags = ch.basic_publish_batch(%w[a b c d], routing_key: q.name)
      expect(tags).to eq([2, 3, 4, 5])
      expect(ch.wait_for_confirms).to be true
      expect(ch.unconfirmed_tags).to be_empty
      expect(drain(ch, q.name)).to eq(%w[single a b c d])
      ch.close
    end
  end

  it "applies the publish options to every message and handles large bodies" do
    isolated_session(frame_max: 8192) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.batch.opts.#{SecureRandom.hex(4)}", durable: true)
      big = "x" * 20_000   # several body frames per message
      ch.basic_publish_batch([big, big], routing_key: q.name, persistent: true, content_type: "text/plain", headers: { "n" => 1 })
      sleep 0.3

      2.times do
        _di, header, body = ch.basic_get(q.name, manual_ack: false)
        expect(body.bytesize).to eq(20_000)
        expect(header.properties[:delivery_mode]).to eq(2)
        expect(header.properties[:content_type]).to eq("text/plain")
        expect(header.properties[:headers]["n"]).to eq(1)
      end
      ch.close
    end
  end

  it "is a no-op for an empty batch and rejects non-arrays" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect(ch.basic_publish_batch([], routing_key: "anything")).to be_nil
      expect { ch.basic_publish_batch("not an array", routing_key: "x") }.to raise_error(ArgumentError)
      ch.close
    end
  end

  it "is not interleaved with a concurrent single publish on the same channel" do
    isolated_session(frame_max: 8192) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.batch.concurrent.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select
      big_batch = 6.times.map { |i| ("b#{i}" * 60_000)[0, 100_000] }

      t1 = Async::Task.current.async { ch.basic_publish_batch(big_batch, routing_key: q.name) }
      t2 = Async::Task.current.async { ch.basic_publish("s" * 100_000, routing_key: q.name) }
      t1.wait; t2.wait
      expect(ch.wait_for_confirms).to be true
      expect(session.open?).to be true
      bodies = drain(ch, q.name)
      expect(bodies.size).to eq(7)
      expect(bodies.map(&:bytesize).uniq).to eq([100_000])
      ch.close
    end
  end
end
