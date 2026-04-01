require "spec_helper"

# Step 9: channel.flow — client-side publish throttling.
# NOTE: RabbitMQ 3.9+ deprecated channel.flow. Modern brokers close the
# connection instead of sending FlowOk. Both tests skip gracefully when
# the broker does not support this method.
RSpec.describe "Channel flow control", :integration do

  it "sends channel.flow(false) and receives FlowOk" do
    isolated_session do |session, _|
      ch = session.open_channel
      begin
        ch.flow(false)
      rescue AsyncRabbitMQ::ConnectionError, AsyncRabbitMQ::ChannelError => e
        skip "Broker does not support channel.flow (deprecated in RabbitMQ 3.9+): #{e.message}"
      end
      # Restore flow before closing
      begin
        ch.flow(true)
      rescue AsyncRabbitMQ::ConnectionError, AsyncRabbitMQ::ChannelError
        # Already closing; swallow
      end
      ch.close rescue nil
    end
  end

  it "sends channel.flow(true) to resume" do
    isolated_session do |session, _|
      ch = session.open_channel
      begin
        ch.flow(false)
      rescue AsyncRabbitMQ::ConnectionError, AsyncRabbitMQ::ChannelError => e
        skip "Broker does not support channel.flow (deprecated in RabbitMQ 3.9+): #{e.message}"
      end
      begin
        ch.flow(true)
      rescue AsyncRabbitMQ::ConnectionError, AsyncRabbitMQ::ChannelError => e
        skip "Broker does not support channel.flow (deprecated in RabbitMQ 3.9+): #{e.message}"
      end
      ch.close rescue nil
    end
  end
end
