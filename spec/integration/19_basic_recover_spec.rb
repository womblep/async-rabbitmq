require "spec_helper"

# Step 19: basic.recover — redeliver unacknowledged messages.
RSpec.describe "basic.recover", :integration do

  it "redelivers unacknowledged messages with requeue: true" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.recover", durable: false)

      ch.basic_publish("recover-me", routing_key: q.name)

      ch.basic_qos(prefetch_count: 10)

      deliveries = []
      tag = q.subscribe(manual_ack: true) do |di, _h, body|
        deliveries << { body: body.to_s, redelivered: di.redelivered }
        # Do NOT ack — leave unacknowledged so recover can redeliver
      end

      sleep 0.2
      expect(deliveries.size).to eq(1)
      expect(deliveries.first[:redelivered]).to eq(false)

      # Ask the broker to redeliver all unacked messages
      ch.basic_recover(requeue: true)

      sleep 0.3

      expect(deliveries.size).to be >= 2
      expect(deliveries.last[:redelivered]).to eq(true)
      expect(deliveries.last[:body]).to eq("recover-me")

      ch.basic_cancel(tag)
      ch.close
    end
  end

  it "returns RecoverOk" do
    isolated_session do |session, _|
      ch = session.open_channel
      result = ch.basic_recover(requeue: true)
      expect(result).to be_a(AMQ::Protocol::Basic::RecoverOk)
      ch.close
    end
  end
end
