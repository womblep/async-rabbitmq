require "spec_helper"

# Step 1: Frame demultiplexer + single-writer pattern.
# Test: basic.publish + basic.get round-trip proves the demux routes frames correctly.
RSpec.describe "Frame demultiplexer — publish/get round-trip", :integration do

  it "publishes a message and retrieves it via basic.get" do
    isolated_session do |session, _vhost|
      ch      = session.open_channel
      q       = ch.queue("test.demux", durable: true)
      payload = "hello from demux #{SecureRandom.hex(4)}"

      ch.basic_publish(payload, routing_key: q.name)

      delivery_info, _headers, body = ch.basic_get(q.name, manual_ack: false)
      expect(delivery_info).not_to be_nil
      expect(body).to eq(payload.b)

      ch.close
    end
  end

  it "routes frames for two channels independently" do
    isolated_session do |session, _vhost|
      ch1 = session.open_channel
      ch2 = session.open_channel

      q1 = ch1.queue("test.ch1", durable: true)
      q2 = ch2.queue("test.ch2", durable: true)

      ch1.basic_publish("msg-ch1", routing_key: q1.name)
      ch2.basic_publish("msg-ch2", routing_key: q2.name)

      _di1, _h1, body1 = ch1.basic_get(q1.name, manual_ack: false)
      _di2, _h2, body2 = ch2.basic_get(q2.name, manual_ack: false)

      expect(body1).to eq("msg-ch1".b)
      expect(body2).to eq("msg-ch2".b)

      ch1.close
      ch2.close
    end
  end
end
