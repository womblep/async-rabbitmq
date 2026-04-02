require "spec_helper"

# Step 3: Heartbeat — dedicated task + timeout detection.
RSpec.describe "Heartbeat", :integration do

  it "connection survives an idle period without activity" do
    session = AsyncRabbitMQ::Session.new(
      host:      RABBITMQ_HOST,
      port:      RABBITMQ_PORT,
      heartbeat: 5   # short interval for fast test
    )
    session.connect
    sleep 7  # 1.4× heartbeat interval — session should still be alive
    expect(session.open?).to be true
    session.close
  end

  it "detects heartbeat timeout and triggers recovery (socket close)" do
    # Fallback: forcibly close the underlying socket to simulate abrupt network loss.
    # Use a very short heartbeat so we can force a timeout quickly.
    session = AsyncRabbitMQ::Session.new(
      host:      RABBITMQ_HOST,
      port:      RABBITMQ_PORT,
      heartbeat: 2
    )
    session.connect

    # Forcibly break the socket to simulate network loss
    session.instance_variable_get(:@frame_io)
           .instance_variable_get(:@socket)
           .close rescue nil

    # Allow recovery loop to detect the break and reconnect (1s initial delay)
    sleep 4   # enough for detection + one reconnect attempt
    recovered = session.open?
    session.close rescue nil   # stop recovery task so Sync {} can exit
    expect(recovered).to be true
  end

  it "detects heartbeat timeout when toxiproxy cuts the connection mid-stream", :toxiproxy do
    skip "toxiproxy not available" unless toxiproxy_available?

    # Connect through the proxy, then cut it — simulates a clean TCP reset mid-stream.
    session = AsyncRabbitMQ::Session.new(
      host:      RABBITMQ_HOST,
      port:      TOXIPROXY_PORT,
      heartbeat: 2
    )
    session.connect
    expect(session.open?).to be true

    # Reset the connection through toxiproxy (simulates network partition)
    toxiproxy_rabbitmq.down do
      sleep 1  # give the reset a moment to propagate
    end

    # After the partition, session should either be recovering or have recovered.
    sleep 6   # 3× heartbeat
    expect(
      session.open? || session.instance_variable_get(:@recovery_in_progress)
    ).to be true
    session.close rescue nil
  end

  it "adds network latency via toxiproxy and still completes handshake", :toxiproxy do
    skip "toxiproxy not available" unless toxiproxy_available?

    # Add 200ms latency — connection should still succeed within CONNECT_TIMEOUT.
    toxiproxy_rabbitmq.toxic(:latency, latency: 200, jitter: 10) do
      session = AsyncRabbitMQ::Session.new(
        host: RABBITMQ_HOST,
        port: TOXIPROXY_PORT
      )
      session.connect
      expect(session.open?).to be true
      session.close
    end
  end
end
