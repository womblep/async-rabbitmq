require "spec_helper"

# Synchronous channel operations give up after rpc_timeout instead of
# waiting forever for a reply that may never come.
RSpec.describe "RPC timeout", :integration do

  it "raises RpcTimeoutError when the broker never answers, and the channel stays usable" do
    isolated_session(rpc_timeout: 0.5) do |session, _|
      ch       = session.open_channel
      frame_io = session.instance_variable_get(:@frame_io)

      # Swallow the next queue.declare so no DeclareOk ever arrives.
      swallowed = false
      allow(frame_io).to receive(:write_frame).and_wrap_original do |m, data, **kw|
        if !swallowed && data.include?("test.rpc-timeout")
          swallowed = true
          nil
        else
          m.call(data, **kw)
        end
      end

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      expect {
        ch.queue("test.rpc-timeout", durable: true)
      }.to raise_error(AsyncRabbitMQ::RpcTimeoutError, /queue\.declare-ok.*0\.5s.*channel #{ch.channel_id}/)
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be_between(0.4, 3)

      # The frame was never sent, so the channel is intact and the next RPC works.
      expect(ch.open?).to be true
      q = ch.queue("test.rpc-timeout-after", durable: true)
      expect(q.name).to eq("test.rpc-timeout-after")
      ch.close
    end
  end

  it "defaults to Session::RPC_TIMEOUT and can be disabled with nil" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect(ch.instance_variable_get(:@rpc_timeout)).to eq(AsyncRabbitMQ::Session::RPC_TIMEOUT)
      ch.close
    end
    isolated_session(rpc_timeout: nil) do |session, _|
      ch = session.open_channel
      expect(ch.instance_variable_get(:@rpc_timeout)).to be_nil
      expect(ch.queue("test.rpc-no-timeout", durable: true).name).to eq("test.rpc-no-timeout")
      ch.close
    end
  end
end
