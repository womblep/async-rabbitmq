require "spec_helper"

# The pending-confirm entries and their replay: the payload is snapshotted at
# publish time, the replay is re-encoded from the kept context (so a queue
# rename or a renegotiated frame_max is honoured), and a rename rewrites both
# the default-exchange routing key and reply_to in the kept context.
RSpec.describe "unconfirmed publish replay" do
  let(:session)  { double("Session", frame_max: 131_072, channel_closed: nil) }
  let(:frame_io) { double("FrameIO") }
  let(:channel) do
    AsyncRabbitMQ::Channel.new(1, session, frame_io, frame_max: 131_072, logger: AsyncRabbitMQ::Log.silent)
  end

  def seed_pending(payload, exchange: "", routing_key: "q", **opts)
    ctx   = channel.send(:publish_context, exchange, routing_key, opts)
    bytes = channel.send(:encode_publish, payload, exchange: exchange, routing_key: routing_key, **opts)
    channel.instance_variable_set(:@confirms_enabled, true)
    channel.send(:reserve_confirm_tag, bytes, payload, ctx)
    channel.instance_variable_get(:@pending_confirms).values.last
  end

  it "snapshots the payload so a caller mutating its buffer cannot corrupt the replay" do
    buffer = +"original bytes"
    entry  = seed_pending(buffer)
    buffer.replace("OVERWRITTEN!!!")
    expect(entry[1]).to eq("original bytes")
  end

  it "re-encodes the replay at the current frame_max rather than replaying stale frames" do
    payload = "a" * 10_000
    entry   = seed_pending(payload)
    original_frames = entry[0].count("\xCE".b) # one terminator per frame

    # The reconnect negotiated a much smaller frame size.
    channel.instance_variable_set(:@frame_max, 4096)
    written = []
    allow(frame_io).to receive(:write_frame) { |bytes, **| written << bytes }
    channel.send(:republish_unconfirmed)

    expect(written.size).to eq(1)
    expect(written.first.count("\xCE".b)).to be > original_frames # body split smaller
    expect(written.first).not_to eq(entry[0])
  end

  it "rewrites the default-exchange routing key and reply_to when the queue is renamed" do
    seed_pending("rpc request", routing_key: "amq.gen-OLD", reply_to: "amq.gen-OLD", correlation_id: "c1")

    channel.rename_unconfirmed_queue("amq.gen-OLD", "amq.gen-NEW")

    ctx = channel.instance_variable_get(:@pending_confirms).values.last[2]
    expect(ctx.routing_key).to eq("amq.gen-NEW")
    expect(ctx.options[:reply_to]).to eq("amq.gen-NEW")
    expect(ctx.options[:correlation_id]).to eq("c1") # everything else untouched
  end

  it "rewrites reply_to even when the publish went to a named exchange" do
    seed_pending("rpc via topic", exchange: "events", routing_key: "orders.create", reply_to: "amq.gen-OLD")

    channel.rename_unconfirmed_queue("amq.gen-OLD", "amq.gen-NEW")

    ctx = channel.instance_variable_get(:@pending_confirms).values.last[2]
    expect(ctx.routing_key).to eq("orders.create") # caller's semantics, untouched
    expect(ctx.options[:reply_to]).to eq("amq.gen-NEW")
  end
end
