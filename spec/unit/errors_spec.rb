require "spec_helper"

# ChannelError carries the broker's channel.close and exposes the amq-protocol
# (>= 2.7) predicates for the common closure reasons. No broker needed.
RSpec.describe AsyncRabbitMQ::ChannelError do
  def close_method(code, text)
    AMQ::Protocol::Channel::Close.new(code, text, 60, 80)
  end

  def error_for(code, text)
    described_class.new(code: code, text: text, channel_id: 1, close_method: close_method(code, text))
  end

  it "recognises a consumer delivery acknowledgement timeout" do
    e = error_for(406, "PRECONDITION_FAILED - delivery acknowledgement on channel 1 timed out. " \
                       "Timeout value used: 1800000 ms. This timeout value can be configured, see consumers doc guide to learn more")
    expect(e.delivery_ack_timeout?).to be true
    expect(e.unknown_delivery_tag?).to be false
    expect(e.message_too_large?).to be false
    expect(e.code).to eq(406)
    expect(e.close_method).to be_a(AMQ::Protocol::Channel::Close)
  end

  it "recognises an unknown delivery tag (double ack)" do
    e = error_for(406, "PRECONDITION_FAILED - unknown delivery tag 7")
    expect(e.unknown_delivery_tag?).to be true
    expect(e.delivery_ack_timeout?).to be false
  end

  it "recognises a message that exceeded the configured max size" do
    e = error_for(406, "PRECONDITION_FAILED - message size 268435457 is larger than configured max size 134217728")
    expect(e.message_too_large?).to be true
  end

  it "answers false for every predicate when there is no close method" do
    e = described_class.new("Expected DeclareOk but got CloseOk", channel_id: 3)
    expect(e.delivery_ack_timeout?).to be false
    expect(e.unknown_delivery_tag?).to be false
    expect(e.message_too_large?).to be false
    expect(e.close_method).to be_nil
    expect(e.message).to eq("Expected DeclareOk but got CloseOk")
  end

  it "does not match the predicates on other 406 reasons or other codes" do
    expect(error_for(406, "PRECONDITION_FAILED - inequivalent arg 'durable'").delivery_ack_timeout?).to be false
    expect(error_for(404, "NOT_FOUND - no queue 'x' in vhost '/'").unknown_delivery_tag?).to be false
  end
end
