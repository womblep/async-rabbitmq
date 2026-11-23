require "spec_helper"

# The half-open connection: a NAT idle-drop, VM freeze or pulled cable delivers
# no FIN and no RST — the socket looks healthy forever. Every other disconnect
# in this suite hands the reader an immediate error; here NOTHING arrives, and
# the only possible detector is the heartbeat watchdog's read-silence check
# (no frame within 2x the negotiated interval). These tests pin that the
# watchdog is the component that notices, and how quickly.
RSpec.describe "heartbeat watchdog on a black-holed connection", :integration, :toxiproxy do

  HEARTBEAT = 2 # negotiated interval; dead after 2xT, checked every T/2

  def wait_until(what, timeout: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  def proxied_session(vhost)
    AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
      heartbeat: HEARTBEAT, connect_timeout: 1.5,
      recovery_interval: 0.3, recovery_max_interval: 0.5
    )
  end

  it "detects a total black-hole via HeartbeatTimeoutError in about 2x the interval" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = proxied_session(vhost)
    session.connect

    lost = nil
    lost_at = nil
    session.on_event("connection.lost") do |_n, payload|
      lost ||= payload
      lost_at ||= Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    blackhole(toxiproxy_rabbitmq) do
      # No byte flows either way and no close ever arrives. Only the watchdog
      # can see this. Give it up to 5x the interval before declaring failure.
      wait_until("the watchdog to notice the dead peer", timeout: HEARTBEAT * 5) { lost }
    end

    # Detection must be the heartbeat watchdog, not a transport error — and in
    # watchdog time: no earlier than the 2xT staleness threshold, no later than
    # ~3xT (threshold + one T/2 check period, plus slack for scheduling).
    expect(lost[:error]).to eq("AsyncRabbitMQ::HeartbeatTimeoutError")
    elapsed = lost_at - started
    expect(elapsed).to be_between(HEARTBEAT * 1.5, HEARTBEAT * 4),
                       "detected in #{elapsed.round(2)}s; expected roughly 2x#{HEARTBEAT}s"

    # Toxics are gone now: recovery must complete and the session must work.
    wait_until("recovery") { session.open? }
    session.with_channel { |ch| ch.queue("test.watchdog.#{SecureRandom.hex(4)}", durable: true) }
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "detects read-silence even while its own writes still flow" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = proxied_session(vhost)
    session.connect

    lost = nil
    session.on_event("connection.lost") { |_n, p| lost ||= p }

    # Downstream only: the broker hears our heartbeats and is content; we hear
    # nothing at all. The pure read-silence branch has to fire on its own.
    blackhole(toxiproxy_rabbitmq, direction: :downstream) do
      wait_until("read-silence detection", timeout: HEARTBEAT * 5) { lost }
    end

    expect(lost[:error]).to eq("AsyncRabbitMQ::HeartbeatTimeoutError")
    wait_until("recovery") { session.open? }
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
