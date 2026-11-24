require "spec_helper"

# Unit coverage for Channel#re_register_consumers, the pass-based re-subscribe
# that recovery runs, and the outcomes that are hard to force deterministically
# against a live broker:
#
# - a close over one consumer's own basic.consume drops just that consumer;
# - a close the broker made for anything else (a replayed publish to an exchange
#   that is gone, a refused qos) drops no consumer, reopens without replaying
#   (the unconfirmed messages are reported as nacked), and a second one gives
#   the channel up instead of looping;
# - a connection drop mid-pass drops nothing — the next recovery restores all.
RSpec.describe "Channel#re_register_consumers" do
  let(:session)  { double("Session", frame_max: 131_072, open?: true, channel_closed: nil) }
  let(:frame_io) { double("FrameIO") }
  let(:channel) do
    AsyncRabbitMQ::Channel.new(1, session, frame_io, frame_max: 131_072, logger: AsyncRabbitMQ::Log.silent)
  end

  # Methods, not constants: a constant in a describe block is lexically
  # top-level and would leak into every other spec.
  def basic_class    = AMQ::Protocol::Basic.method_id
  def consume_method = AMQ::Protocol::Basic::Consume.method_id
  def publish_method = AMQ::Protocol::Basic::Publish.method_id

  # The ChannelError a broker channel.close produces, naming the method that
  # caused it, as handle_channel_close builds it.
  def close_error(code, class_id, method_id)
    close = AMQ::Protocol::Channel::Close.new(code, "closed by the broker", class_id, method_id)
    AsyncRabbitMQ::ChannelError.new(code: code, text: "closed by the broker", channel_id: 1, close_method: close)
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

  def remaining_queues
    channel.instance_variable_get(:@consumers).values.map { |e| e[:queue_name] }
  end

  # Reopens succeed and record whether they replayed.
  def record_reopens
    reopens = []
    allow(channel).to receive(:reopen_for_reregister!) do |replay: true|
      reopens << replay
      channel.instance_variable_set(:@state, :open)
    end
    reopens
  end

  it "drops only the consumer whose own basic.consume the broker rejected" do
    seed_consumers("good.a", "doomed", "good.b")
    reopens = record_reopens
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      if queue_name == "doomed"
        channel.instance_variable_set(:@state, :closed) # the broker closed the channel
        raise close_error(404, basic_class, consume_method)
      end
      "ctag-#{queue_name}"
    end

    channel.send(:re_register_consumers)

    expect(remaining_queues).to contain_exactly("good.a", "good.b")
    expect(reopens).to eq([true]) # reopened once, replaying the unconfirmed messages
  end

  it "drops no consumer for a close caused by something else, and reopens without replaying" do
    seed_consumers("a", "b")
    reopens = record_reopens
    failed_once = false
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      if queue_name == "a" && !failed_once
        failed_once = true
        # A replayed publish to an exchange that is gone closed the channel;
        # the close lands on whatever RPC is in flight.
        channel.instance_variable_set(:@state, :closed)
        raise close_error(404, basic_class, publish_method)
      end
      "ctag-#{queue_name}"
    end

    channel.send(:re_register_consumers)

    expect(remaining_queues).to contain_exactly("a", "b") # nobody blamed
    expect(reopens).to eq([false])                         # the poison replay is not repeated
  end

  it "reopens WITH replay for a close caused by something other than a publish" do
    seed_consumers("a")
    reopens = record_reopens
    failed_once = false
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      unless failed_once
        failed_once = true
        # A qos the broker refused (basic.qos, method 10): nothing to do with
        # the replay, so the unconfirmed messages must still be replayed.
        channel.instance_variable_set(:@state, :closed)
        raise close_error(406, basic_class, AMQ::Protocol::Basic::Qos.method_id)
      end
      "ctag-#{queue_name}"
    end

    channel.send(:re_register_consumers)

    expect(reopens).to eq([true])
    expect(remaining_queues).to contain_exactly("a")
  end

  it "keeps a consumer whose ChannelError left the channel open, without counting it as a close" do
    seed_consumers("odd", "fine")
    reopens = record_reopens
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      # An unexpected reply: a ChannelError, but the channel stays open.
      raise AsyncRabbitMQ::ChannelError.new("Expected consume-ok but got something else", channel_id: 1) if queue_name == "odd"

      "ctag-#{queue_name}"
    end

    channel.send(:re_register_consumers)

    expect(channel).to be_open
    expect(reopens).to be_empty # nothing closed, nothing reopened
    expect(remaining_queues).to contain_exactly("odd", "fine")
  end

  it "gives the channel up on a second such close instead of looping, forgetting the rejected consumers" do
    seed_consumers("doomed", "a")
    allow(session).to receive(:channels) { [channel] } # still registered as this id's channel
    record_reopens
    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      channel.instance_variable_set(:@state, :closed)
      raise close_error(404, basic_class, consume_method) if queue_name == "doomed"

      raise close_error(406, basic_class, publish_method)
    end

    channel.send(:re_register_consumers)

    expect(channel).to be_closed
    expect(session).to have_received(:channel_closed).with(1)
    # The rejected one is forgotten even on the way out; the rest are kept
    # for reopen(recover_consumers: true).
    expect(remaining_queues).to contain_exactly("a")
  end

  it "does not tell the session to free an id another channel has taken since" do
    seed_consumers("a")
    other = double("another channel on the same id")
    allow(session).to receive(:channels) { [other] }
    record_reopens
    allow(channel).to receive(:basic_consume) do |_queue_name, **_kw|
      channel.instance_variable_set(:@state, :closed)
      raise close_error(406, basic_class, publish_method)
    end

    channel.send(:re_register_consumers)

    expect(channel).to be_closed
    expect(session).not_to have_received(:channel_closed)
  end

  it "reopens at the end when every consumer was rejected" do
    seed_consumers("doomed")
    reopens = record_reopens
    allow(channel).to receive(:basic_consume) do |_queue_name, **_kw|
      channel.instance_variable_set(:@state, :closed)
      raise close_error(404, basic_class, consume_method)
    end

    channel.send(:re_register_consumers)

    expect(channel).to be_open    # usable, and anything parked on it released
    expect(reopens).to eq([true]) # the final reopen, after the last pass
    expect(remaining_queues).to be_empty
  end

  it "leaves the channel for the next recovery when the final reopen loses the connection" do
    seed_consumers("doomed")
    allow(channel).to receive(:basic_consume) do |_queue_name, **_kw|
      channel.instance_variable_set(:@state, :closed)
      raise close_error(404, basic_class, consume_method)
    end
    allow(channel).to receive(:reopen_for_reregister!)
      .and_raise(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "Connection lost"))

    channel.send(:re_register_consumers)

    # Not given up: nobody freed the id, and the consumer is still there for
    # the next recovery to try.
    expect(session).not_to have_received(:channel_closed)
    expect(remaining_queues).to contain_exactly("doomed")
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

    expect(remaining_queues).to contain_exactly("a", "b", "c")
  end

  it "aborts without dropping when the reopen after a 404 fails because the session is gone" do
    seed_consumers("doomed", "survivor")

    allow(channel).to receive(:basic_consume) do |queue_name, **_kw|
      if queue_name == "doomed"
        channel.instance_variable_set(:@state, :closed)
        raise close_error(404, basic_class, consume_method)
      end
      "ctag-#{queue_name}"
    end
    # Restart tries to reopen, but the session is mid-recovery: NotOpenError.
    allow(channel).to receive(:reopen_for_reregister!).and_raise(AsyncRabbitMQ::NotOpenError.new("Session is not open"))

    channel.send(:re_register_consumers)

    # Both stay in the registry: the 404'd one is retried next cycle rather than
    # dropped while the connection is down.
    expect(channel.instance_variable_get(:@consumers).keys).to contain_exactly("tag0", "tag1")
  end

  describe "abandoning unconfirmed messages" do
    it "reports them as nacked and resets the confirm counter" do
      channel.instance_variable_set(:@confirms_enabled, true)
      channel.instance_variable_set(:@pending_confirms, { 1 => ["", "a", nil], 2 => ["", "b", nil] })
      channel.instance_variable_set(:@delivery_tag, 2)

      channel.send(:abandon_unconfirmed!)

      expect(channel.unconfirmed_tags).to be_empty
      expect(channel.nacked_tags).to eq([1, 2])
      expect(channel.instance_variable_get(:@delivery_tag)).to eq(0)
      expect(channel.instance_variable_get(:@only_acks)).to be false
    end
  end
end
