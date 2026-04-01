require "spec_helper"

# Step 5: Channel open / close lifecycle.
RSpec.describe "Channel lifecycle", :integration do

  it "opens a channel" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect(ch.open?).to be true
      ch.close
    end
  end

  it "closes a channel" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.close
      expect(ch.closed?).to be true
    end
  end

  it "opens multiple channels on the same session" do
    isolated_session do |session, _|
      channels = 10.times.map { session.open_channel }
      channels.each { |ch| expect(ch.open?).to be true }
      channels.each(&:close)
    end
  end

  it "reopens a channel after closing" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.close
      ch2 = session.open_channel
      expect(ch2.open?).to be true
      ch2.close
    end
  end

  it "raises NotOpenError when publishing on a closed channel" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.close
      expect { ch.basic_publish("x", routing_key: "foo") }.to raise_error(AsyncRabbitMQ::NotOpenError)
    end
  end
end
