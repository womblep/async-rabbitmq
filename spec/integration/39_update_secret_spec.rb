require "spec_helper"

# Session#update_secret (connection.update-secret): rotate the credential on a
# live connection, e.g. a refreshed OAuth 2 token. The internal auth backend
# of the test broker acknowledges any new secret, so the observable proof that
# the new value is really adopted is what happens on the next reconnect.
RSpec.describe "Session#update_secret", :integration do

  it "is acknowledged by the broker and keeps the connection open" do
    isolated_session do |session, _|
      expect(session.update_secret("guest", "rotation test")).to be true
      expect(session.open?).to be true
      expect(session.instance_variable_get(:@password)).to eq("guest")
      session.with_channel { |ch| ch.queue("test.secret.#{SecureRandom.hex(4)}", durable: true) }
    end
  end

  it "uses the new secret for reconnects" do
    exhausted = false
    isolated_session(recovery_interval: 0.2, recovery_max_interval: 0.3) do |session, _|
      session.on_recovery_exhausted { |_s| exhausted = true }
      expect(session.update_secret("no-longer-the-password", "rotate to a bad one")).to be true

      # Drop the connection: recovery must present the *new* secret, which the
      # broker refuses, so recovery is abandoned rather than looping forever.
      session.instance_variable_get(:@frame_io).instance_variable_get(:@socket).close rescue nil
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      sleep 0.1 until session.closed? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      expect(session.closed?).to be true
      expect(exhausted).to be true
    end
  end

  it "raises RpcTimeoutError when the broker never answers" do
    isolated_session(rpc_timeout: 0.5) do |session, _|
      frame_io = session.instance_variable_get(:@frame_io)
      allow(frame_io).to receive(:write_frame).and_wrap_original do |m, data, **kw|
        data.include?("swallow-me") ? nil : m.call(data, **kw)
      end
      expect { session.update_secret("swallow-me", "r") }.to raise_error(AsyncRabbitMQ::RpcTimeoutError, /update-secret/)
      expect(session.open?).to be true
    end
  end

  it "raises NotOpenError when the session is not open" do
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT)
    expect { session.update_secret("x") }.to raise_error(AsyncRabbitMQ::NotOpenError)
  end
end
