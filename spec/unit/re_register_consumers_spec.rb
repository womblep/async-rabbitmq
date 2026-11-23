require "spec_helper"

# Unit coverage for Channel#re_register_consumers, the pass-based re-subscribe
# that recovery runs. Two outcomes matter and are hard to force deterministically
# against a live broker: a per-consumer 404 (drop just that one) versus a
# connection drop mid-pass (drop nothing — the next recovery cycle restores all).
RSpec.describe "Channel#re_register_consumers" do
  let(:session)  { double("Session", frame_max: 131_072, open?: true) }
  let(:frame_io) { double("FrameIO") }
  let(:channel) do
    AsyncRabbitMQ::Channel.new(1, session, frame_io, frame_max: 131_072, logger: AsyncRabbitMQ::Log.silent)
  end

  # Seed the consumer registry as recovery would see it, and keep the channel
  # :open so re-registration does not try to reopen.
  def seed_consumers(*names)
    registry = {}
    names.each_with_index do |name, i|
      registry["tag#{i}"] = { queue_name: name, block: proc {}, manual_ack: false,
                              exclusive: false, arguments: {}, on_error: nil }
    end
    channel.instance_variable_set(:@consumers, registry)
    channel.instance_variable_set(:@state, :open)
    allow(channel).to receive(:topology).and_return(nil)
    registry
  end

  it "drops only the consumer whose queue the broker rejects with a ChannelError" do
    seed_consumers("good.a", "doomed", "good.b")

    # The doomed queue's re-subscribe draws a 404 and the broker closes the
    # channel; the next basic_consume therefore reopens first.
    allow(channel).to receive(:reopen_for_reregister!) { channel.instance_variable_set(:@state, :open) }
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      if queue_name == "doomed"
        channel.instance_variable_set(:@state, :closed) # broker closed the channel
        raise AsyncRabbitMQ::ChannelError.new(code: 404, text: "NOT_FOUND", channel_id: 1)
      end
      "ctag-#{queue_name}"
    end

    channel.send(:re_register_consumers)

    remaining = channel.instance_variable_get(:@consumers).values.map { |e| e[:queue_name] }
    expect(remaining).to contain_exactly("good.a", "good.b")
  end

  it "drops nothing when a connection loss interrupts the pass" do
    seed_consumers("a", "b", "c")

    # The second consumer's re-subscribe is interrupted by a connection drop —
    # a ConnectionError, not a per-consumer rejection. Nothing should be dropped.
    calls = 0
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      calls += 1
      raise AsyncRabbitMQ::ConnectionError.new(code: 0, text: "Connection lost") if calls == 2
      "ctag-#{queue_name}"
    end

    channel.send(:re_register_consumers)

    # All three survive for the next recovery cycle to restore.
    remaining = channel.instance_variable_get(:@consumers).values.map { |e| e[:queue_name] }
    expect(remaining).to contain_exactly("a", "b", "c")
  end

  it "aborts without dropping when the reopen after a 404 fails because the session is gone" do
    seed_consumers("doomed", "survivor")

    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      if queue_name == "doomed"
        channel.instance_variable_set(:@state, :closed)
        raise AsyncRabbitMQ::ChannelError.new(code: 404, text: "NOT_FOUND", channel_id: 1)
      end
      "ctag-#{queue_name}"
    end
    # Restart tries to reopen, but the session is mid-recovery: NotOpenError.
    allow(channel).to receive(:reopen_for_reregister!).and_raise(AsyncRabbitMQ::NotOpenError.new("Session is not open"))

    channel.send(:re_register_consumers)

    # Both stay in the registry: the 404'd one is retried next cycle rather than
    # dropped while the connection is down.
    remaining = channel.instance_variable_get(:@consumers).keys
    expect(remaining).to contain_exactly("tag0", "tag1")
  end
end
