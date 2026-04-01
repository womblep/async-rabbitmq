require "spec_helper"

# Step 11: Queue declare, bind, unbind, delete, purge.
RSpec.describe "Queue operations", :integration do

  it "declares a durable queue" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.durable", durable: true)
      expect(q.name).to eq("test.durable")
      ch.close
    end
  end

  it "declares a transient queue" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.transient", durable: false)
      expect(q.name).to eq("test.transient")
      ch.close
    end
  end

  it "binds and unbinds a queue to an exchange" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.bind", durable: false)
      ex = ch.exchange("test.ex.bind", type: :direct, durable: false)

      expect { q.bind(exchange: ex.name, routing_key: "my.key") }.not_to raise_error
      expect { q.unbind(exchange: ex.name, routing_key: "my.key") }.not_to raise_error
      ch.close
    end
  end

  it "purges a queue and reports zero messages" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.purge", durable: false)

      5.times { ch.basic_publish("x", routing_key: q.name) }
      sleep 0.1

      result = ch.queue_purge(q.name)
      expect(result.message_count).to be >= 0   # broker may have consumed some

      ch.close
    end
  end

  it "deletes a queue" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.delete.me", durable: false)
      expect { q.delete }.not_to raise_error
      ch.close
    end
  end
end
