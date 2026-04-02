require "spec_helper"

# Step 9: channel.flow — client-side publish throttling.
# NOTE: RabbitMQ 3.9+ deprecated channel.flow and closes the connection
# with 540 NOT_IMPLEMENTED when active=false. active=true is accepted as
# a no-op since flow is already active by default.
RSpec.describe "Channel flow control", :integration do

  it "sends channel.flow(false) and broker rejects with NOT_IMPLEMENTED" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect {
        ch.flow(false)
      }.to raise_error(AsyncRabbitMQ::ConnectionError, /NOT_IMPLEMENTED/)
    end
  end

  it "sends channel.flow(true) which succeeds as a no-op" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect { ch.flow(true) }.not_to raise_error
      ch.close
    end
  end
end
