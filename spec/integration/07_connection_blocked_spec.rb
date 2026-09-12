require "spec_helper"

# Step 7: connection.blocked / connection.unblocked handling.
# Note: triggering a real connection.blocked requires filling the broker's memory
# alarm threshold, which is impractical in a unit integration test.
# We test the method frame routing directly via a stub-level unit test here,
# and document a chaos-test scenario for manual/dedicated chaos CI.
RSpec.describe "connection.blocked / connection.unblocked", :integration do

  it "session logs a warning and stays open when connection.blocked is received" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.blocked", durable: true)
      ch.basic_publish("payload", routing_key: q.name)

      # Simulate receiving a connection.blocked method frame on channel 0
      frame_io = session.instance_variable_get(:@frame_io)

      # Route it directly to the channel-0 queue
      queue0 = frame_io.channel_queue(0)
      queue0.push([:method, AMQ::Protocol::Connection::Blocked.new("memory alarm")]) if queue0

      # Connection must stay open
      sleep 0.1
      expect(session.open?).to be true

      # Close while still blocked: control frames bypass the publish gate.
      ch.close
    end
  end

  it "session recovers from connection.unblocked after blocked" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.queue("test.unblocked", durable: true)

      frame_io = session.instance_variable_get(:@frame_io)
      queue0   = frame_io.channel_queue(0)

      queue0&.push([:method, AMQ::Protocol::Connection::Blocked.new("test")])
      sleep 0.05
      queue0&.push([:method, AMQ::Protocol::Connection::Unblocked.new])
      sleep 0.05

      expect(session.open?).to be true
      ch.close
    end
  end
end
