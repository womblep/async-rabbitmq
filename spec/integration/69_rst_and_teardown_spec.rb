require "spec_helper"

# RST instead of FIN, and a peer that lingers in half-close. A crashed broker
# process, a rejecting load balancer or a conntrack flush sends TCP RESET: reads
# raise ECONNRESET rather than returning EOF, writes raise it rather than EPIPE,
# and an RST during the handshake is a different connect failure from refused or
# timed out. Every other test in the suite tears connections down with FIN.
RSpec.describe "RST and lingering teardown", :integration, :toxiproxy do

  def wait_until(what, timeout: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  def with_proxied_session(**opts)
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
      recovery_interval: 0.3, recovery_max_interval: 0.5, **opts
    )
    session.connect
    yield session, vhost
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "recovers from a peer RESET, detected as a transport error rather than by the watchdog" do
    with_proxied_session(heartbeat: 30) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.rst", durable: true)
      received = []
      ch.basic_consume(q.name, manual_ack: false) { |_di, _h, body| received << body }

      lost = nil
      lost_at = nil
      noticed_from = nil
      session.on_event("connection.lost") do |_n, p|
        lost ||= p
        lost_at ||= Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      toxiproxy_rabbitmq.toxic(:reset_peer, timeout: 0).apply do
        # reset_peer fires on traffic, not on apply: an idle connection sits
        # untouched until its next heartbeat. Provoke a write so the RESET
        # lands now, as it would under any real workload.
        noticed_from = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        begin
          ch.basic_publish("poke", routing_key: q.name)
        rescue AsyncRabbitMQ::Error
          # racing the reset is fine; the connection loss is the point
        end
        wait_until("the RESET to be noticed") { lost }
      end

      # With heartbeat: 30 the watchdog could not have fired for a minute: a
      # prompt detection proves the RST surfaced as a transport error.
      expect(lost_at - noticed_from).to be < 10
      expect(lost[:error]).not_to eq("AsyncRabbitMQ::HeartbeatTimeoutError")

      wait_until("recovery") { session.open? && ch.open? }
      ch.basic_publish("after the reset", routing_key: q.name)
      wait_until("the consumer to deliver") { received.include?("after the reset") }
    end
  end

  it "fails a connect promptly when the handshake is RESET, and connects once the resets stop" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         connect_timeout: 8)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    toxiproxy_rabbitmq.toxic(:reset_peer, timeout: 0).apply do
      expect { session.connect }.to raise_error(AsyncRabbitMQ::Error)
    end
    # The RST arrives at once: the failure must come from the error, not from
    # waiting out connect_timeout.
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 6

    # The same session object connects cleanly once the network behaves.
    session.connect
    expect(session.open?).to be true
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "closes in bounded time against a peer that lingers in half-close" do
    with_proxied_session do |session, _|
      session.with_channel { |ch| ch.queue("test.slowclose", durable: true) }

      toxiproxy_rabbitmq.toxic(:slow_close, delay: 3000).apply do
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        session.close
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        # The close handshake itself completes (data still flows; only the TCP
        # close lingers), so close must return promptly, not wait on the peer.
        expect(elapsed).to be < 6
        expect(session.closed?).to be true
      end
    end
  end
end
