require "spec_helper"

# Channel#close when the Close/CloseOk RPC itself fails. An RPC timeout has
# already resynchronised the channel by the time close's rescue sees it (rpc's
# ensure does that), so close retries once on the resynced channel; a second
# timeout raises with the channel alive rather than force-forgetting an id the
# broker still considers open. A connection loss mid-close drops the channel so
# recovery cannot resurrect something the user asked to be rid of.
RSpec.describe "Channel#close when the Close RPC fails" do
  let(:session)  { double("Session", frame_max: 131_072, channel_closed: nil) }
  let(:frame_io) { double("FrameIO") }
  let(:channel) do
    AsyncRabbitMQ::Channel.new(1, session, frame_io, frame_max: 131_072, logger: AsyncRabbitMQ::Log.silent)
  end

  before { channel.instance_variable_set(:@state, :open) }

  it "retries once after an RPC timeout and completes the close" do
    calls = 0
    allow(channel).to receive(:rpc) do
      calls += 1
      # rpc's ensure would have resynced the channel back to :open; it is.
      raise AsyncRabbitMQ::RpcTimeoutError, "no close-ok" if calls == 1
      :ok
    end

    channel.close

    expect(calls).to eq(2)
    expect(channel).to be_closed
    expect(session).to have_received(:channel_closed).with(1)
  end

  it "raises after a second timeout, leaving the (resynced) channel alive" do
    allow(channel).to receive(:rpc).and_raise(AsyncRabbitMQ::RpcTimeoutError, "no close-ok")

    expect { channel.close }.to raise_error(AsyncRabbitMQ::RpcTimeoutError)
    expect(channel).to be_open
    expect(session).not_to have_received(:channel_closed)
  end

  it "drops the channel when the timed-out close's resync left it recovering" do
    allow(channel).to receive(:rpc) do
      # The connection went while the resync waited for its close-ok: the
      # resync early-returned with the channel handed to recovery. Without a
      # drop here, the next recovery pass would resurrect a channel the user
      # asked to be rid of — consumers and all.
      channel.instance_variable_set(:@state, :recovering)
      raise AsyncRabbitMQ::RpcTimeoutError, "no close-ok"
    end

    expect { channel.close }.not_to raise_error
    expect(channel).to be_closed
    expect(session).to have_received(:channel_closed).with(1)
  end

  it "treats a close whose resync already tore the channel down as done" do
    allow(channel).to receive(:rpc) do
      # The resync failed: mark_closed! ran, waiters failed, id released.
      channel.instance_variable_set(:@state, :closed)
      raise AsyncRabbitMQ::RpcTimeoutError, "no close-ok"
    end

    expect { channel.close }.not_to raise_error
    expect(channel).to be_closed
    expect(session).not_to have_received(:channel_closed) # resync already did the bookkeeping
  end

  it "drops the channel when the connection goes mid-close" do
    allow(channel).to receive(:rpc) do
      # The session noticed the drop and marked the channel recovering.
      channel.instance_variable_set(:@state, :recovering)
      raise AsyncRabbitMQ::ConnectionError.new(code: 0, text: "Connection lost")
    end

    expect { channel.close }.not_to raise_error
    expect(channel).to be_closed
    expect(session).to have_received(:channel_closed).with(1)
  end

  it "treats an already-torn-down channel as done" do
    allow(channel).to receive(:rpc) do
      # mark_closed! already ran via the session teardown.
      channel.instance_variable_set(:@state, :closed)
      raise AsyncRabbitMQ::ConnectionError.new(code: 0, text: "Connection lost")
    end

    expect { channel.close }.not_to raise_error
    expect(channel).to be_closed
    # Nothing re-dropped: the teardown already handled bookkeeping.
    expect(session).not_to have_received(:channel_closed)
  end
end
