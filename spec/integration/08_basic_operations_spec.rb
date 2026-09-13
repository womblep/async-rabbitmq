require "spec_helper"

# Step 8: Basic publish, get, ack, nack, reject.
RSpec.describe "Basic AMQP operations", :integration do

  it "publishes and retrieves a message (basic.get)" do
    isolated_session do |session, _|
      ch      = session.open_channel
      q       = ch.queue("test.basic", durable: true)
      payload = "hello-#{SecureRandom.hex(4)}"

      ch.basic_publish(payload, routing_key: q.name)
      di, _headers, body = ch.basic_get(q.name, manual_ack: true)

      expect(body).to eq(payload.b)
      ch.basic_ack(di.delivery_tag)
      ch.close
    end
  end

  it "returns nil from basic.get when the queue is empty" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.empty", durable: true)

      result = ch.basic_get(q.name, manual_ack: false)
      expect(result).to be_nil
      ch.close
    end
  end

  it "nacks and requeues a message" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.nack", durable: true)

      ch.basic_publish("nack-me", routing_key: q.name)
      di, _headers, _body = ch.basic_get(q.name, manual_ack: true)
      ch.basic_nack(di.delivery_tag, requeue: true)

      # Message should be back in the queue
      di2, _h, body2 = ch.basic_get(q.name, manual_ack: true)
      expect(body2).to eq("nack-me".b)
      ch.basic_ack(di2.delivery_tag)
      ch.close
    end
  end

  it "rejects and requeues a message" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.reject", durable: true)

      ch.basic_publish("reject-me", routing_key: q.name)
      di, _h, _body = ch.basic_get(q.name, manual_ack: true)
      ch.basic_reject(di.delivery_tag, requeue: true)

      di2, _h, body2 = ch.basic_get(q.name, manual_ack: true)
      expect(body2).to eq("reject-me".b)
      ch.basic_ack(di2.delivery_tag)
      ch.close
    end
  end

  it "discards a message when nacked with requeue: false" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.nack.discard", durable: true)

      ch.basic_publish("discard-me", routing_key: q.name)
      di, _h, _body = ch.basic_get(q.name, manual_ack: true)
      ch.basic_nack(di.delivery_tag, requeue: false)

      result = ch.basic_get(q.name, manual_ack: false)
      expect(result).to be_nil
      ch.close
    end
  end
end
