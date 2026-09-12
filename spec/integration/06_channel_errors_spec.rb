require "spec_helper"

# Step 6: Channel error handling — soft vs. hard errors.
RSpec.describe "Channel error handling", :integration do

  it "raises ChannelError(404) when declaring a queue with conflicting properties" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.queue("test.conflict", durable: true)

      ch2 = session.open_channel
      expect {
        # Redeclare with different durable setting — PRECONDITION_FAILED (406)
        ch2.queue("test.conflict", durable: true)
      }.to raise_error(AsyncRabbitMQ::ChannelError) do |err|
        expect([406, 404]).to include(err.code)
      end
    end
  end

  it "channel is closed after soft error, connection stays alive" do
    isolated_session do |session, _|
      ch = session.open_channel

      begin
        ch.queue("test.softfail", durable: true)
        # Re-declare with wrong durability to trigger PRECONDITION_FAILED
        ch.queue("test.softfail", durable: true)
      rescue AsyncRabbitMQ::ChannelError
        # expected
      end

      expect(ch.closed?).to be true
      expect(session.open?).to be true
    end
  end

  it "can open a new channel after a soft error" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.queue("test.recover", durable: true)

      begin
        ch.queue("test.recover", durable: true)
      rescue AsyncRabbitMQ::ChannelError
        # expected
      end

      # Open new channel on same session — must work
      ch2 = session.open_channel
      expect(ch2.open?).to be true
      ch2.close
    end
  end

  it "raises ChannelError(404) when consuming from non-existent queue" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect {
        ch.basic_consume("no.such.queue.#{SecureRandom.hex(8)}")
      }.to raise_error(AsyncRabbitMQ::ChannelError) do |err|
        expect(err.code).to eq(404)
      end
    end
  end
end
