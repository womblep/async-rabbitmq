require "spec_helper"

# Step 15: Publisher confirms + recovery.
RSpec.describe "Publisher confirms", :integration do

  it "confirm_select activates confirms mode" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect { ch.confirm_select }.not_to raise_error
      ch.close
    end
  end

  it "wait_for_confirms returns true for a single published message" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.confirm.single", durable: false)
      ch.confirm_select

      ch.basic_publish("confirmed!", routing_key: q.name)
      result = ch.wait_for_confirms

      expect(result).to be true
      ch.close
    end
  end

  it "wait_for_confirms handles a batch of messages" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.confirm.batch", durable: false)
      ch.confirm_select

      20.times { |i| ch.basic_publish("msg-#{i}", routing_key: q.name) }
      result = ch.wait_for_confirms

      expect(result).to be true
      ch.close
    end
  end

  it "Friday 2am test: 10,000 messages over 100 fibers, no message loss" do
    skip "Set FRIDAY_2AM=1 to run" unless ENV["FRIDAY_2AM"]

    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.friday2am", durable: true)
      ch.confirm_select

      tasks = 100.times.map do |fiber_idx|
        Async do
          100.times do |msg_idx|
            ch.basic_publish("#{fiber_idx}-#{msg_idx}", routing_key: q.name, persistent: true)
          end
          ch.wait_for_confirms
        end
      end

      tasks.each(&:wait)

      # Drain and count
      received = 0
      while (msg = ch.basic_get(q.name, manual_ack: true))
        received += 1
        ch.basic_ack(msg[0].delivery_tag)
      end

      expect(received).to eq(10_000)
      ch.close
    end
  end
end
