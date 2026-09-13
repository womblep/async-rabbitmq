require "spec_helper"

# Consecutive basic_get calls must each return their own message. A stale
# content buffer used to shift every result after the first by one message.
RSpec.describe "basic_get returns each message exactly once and in order", :integration do

  it "drains a queue in publish order with distinct bodies" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.order.#{SecureRandom.hex(4)}", durable: true)
      expected = (1..8).map { |i| "message-#{i}" }
      expected.each { |body| ch.basic_publish(body, routing_key: q.name) }
      sleep 0.2

      bodies = []
      while (msg = ch.basic_get(q.name, manual_ack: false))
        bodies << msg[2]
      end
      expect(bodies).to eq(expected)
      ch.close
    end
  end

  it "does not carry content across a GetEmpty into the next successful get" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.empty.#{SecureRandom.hex(4)}", durable: true)

      ch.basic_publish("first", routing_key: q.name)
      sleep 0.1
      expect(ch.basic_get(q.name, manual_ack: false)[2]).to eq("first")
      expect(ch.basic_get(q.name, manual_ack: false)).to be_nil

      ch.basic_publish("second", routing_key: q.name)
      sleep 0.1
      expect(ch.basic_get(q.name, manual_ack: false)[2]).to eq("second")
      ch.close
    end
  end

  it "returns matching delivery info, properties and body for each message" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.get.props.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("a", routing_key: q.name, message_id: "id-a")
      ch.basic_publish("b", routing_key: q.name, message_id: "id-b")
      sleep 0.2

      _di, header_a, body_a = ch.basic_get(q.name, manual_ack: false)
      _di, header_b, body_b = ch.basic_get(q.name, manual_ack: false)
      expect([body_a, header_a.properties[:message_id]]).to eq(["a", "id-a"])
      expect([body_b, header_b.properties[:message_id]]).to eq(["b", "id-b"])
      ch.close
    end
  end
end
