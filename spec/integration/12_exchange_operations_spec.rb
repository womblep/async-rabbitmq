require "spec_helper"

# Step 12: Exchange declare, bind, unbind, delete — all four types.
RSpec.describe "Exchange operations", :integration do

  %i[direct topic fanout headers].each do |ex_type|
    it "declares a #{ex_type} exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.exchange("test.#{ex_type}", type: ex_type, durable: false)
        expect(ex.name).to eq("test.#{ex_type}")
        ch.close
      end
    end
  end

  it "routes a message via a direct exchange to a bound queue" do
    isolated_session do |session, _|
      ch = session.open_channel
      ex = ch.exchange("test.routing", type: :direct, durable: false)
      q  = ch.queue("test.routing.q", durable: true)
      q.bind(exchange: ex.name, routing_key: "rk")

      ex.publish("payload", routing_key: "rk")

      _di, _h, body = ch.basic_get(q.name)
      expect(body).to eq("payload".b)
      ch.close
    end
  end

  it "routes a message via a topic exchange" do
    isolated_session do |session, _|
      ch = session.open_channel
      ex = ch.exchange("test.topic", type: :topic, durable: false)
      q  = ch.queue("test.topic.q", durable: true)
      q.bind(exchange: ex.name, routing_key: "orders.#")

      ex.publish("order", routing_key: "orders.new")

      _di, _h, body = ch.basic_get(q.name)
      expect(body).to eq("order".b)
      ch.close
    end
  end

  it "fanout exchange delivers to all bound queues" do
    isolated_session do |session, _|
      ch = session.open_channel
      ex = ch.exchange("test.fanout", type: :fanout, durable: false)
      q1 = ch.queue("test.fanout.q1", durable: true)
      q2 = ch.queue("test.fanout.q2", durable: true)
      q1.bind(exchange: ex.name)
      q2.bind(exchange: ex.name)

      ex.publish("broadcast")

      _di1, _h, b1 = ch.basic_get(q1.name)
      _di2, _h, b2 = ch.basic_get(q2.name)
      expect(b1).to eq("broadcast".b)
      expect(b2).to eq("broadcast".b)
      ch.close
    end
  end

  it "deletes an exchange" do
    isolated_session do |session, _|
      ch = session.open_channel
      ex = ch.exchange("test.delete.ex", type: :direct, durable: false)
      expect { ex.delete }.not_to raise_error
      ch.close
    end
  end

  it "binds two exchanges together" do
    isolated_session do |session, _|
      ch   = session.open_channel
      src  = ch.exchange("test.src", type: :fanout, durable: false)
      dst  = ch.exchange("test.dst", type: :fanout, durable: false)
      expect { src.bind(destination: dst.name) }.not_to raise_error
      ch.close
    end
  end
end
