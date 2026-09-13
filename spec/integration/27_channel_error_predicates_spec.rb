require "spec_helper"

# A broker-initiated channel.close is surfaced as a ChannelError that carries
# the decoded Channel::Close, so callers can use the amq-protocol predicates
# instead of matching on reply_text.
RSpec.describe "ChannelError predicates from a real channel.close", :integration do

  it "flags a double ack as unknown_delivery_tag? on the error raised to the waiting RPC" do
    isolated_session do |session, _|
      ch = session.open_channel
      captured = nil
      ch.on_error { |_c, method| captured = method }

      # Acking a tag the broker never issued closes the channel with 406.
      ch.basic_ack(999)

      # The declare is in flight when channel.close arrives, so it is the RPC
      # that receives the ChannelError.
      expect {
        ch.queue("test.pred.#{SecureRandom.hex(4)}", durable: true)
      }.to raise_error(AsyncRabbitMQ::ChannelError) { |e|
        expect(e.code).to eq(406)
        expect(e.unknown_delivery_tag?).to be true
        expect(e.delivery_ack_timeout?).to be false
        expect(e.close_method).to be_a(AMQ::Protocol::Channel::Close)
      }

      expect(captured).to be_a(AMQ::Protocol::Channel::Close)
      expect(captured.unknown_delivery_tag?).to be true
      expect(ch.open?).to be false
      expect(session.open?).to be true
    end
  end
end
