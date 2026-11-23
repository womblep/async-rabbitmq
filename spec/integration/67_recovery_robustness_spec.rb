require "spec_helper"

# Recovery under repeated and adverse conditions: flapping links, resource
# alarms followed by death, half-up brokers, operators shutting down mid-outage,
# credentials revoked while disconnected, and a session asked to live on after
# recovery gave up. Each is a distinct path through recover_loop that a single
# clean down/up cycle never touches.
RSpec.describe "recovery robustness", :integration, :toxiproxy do

  def wait_until(what, timeout: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  def proxied_session(vhost, **opts)
    AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
      recovery_interval: 0.3, recovery_max_interval: 0.5, **opts
    )
  end

  def with_proxied_session(**opts)
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = proxied_session(vhost, **opts)
    session.connect
    yield session, vhost
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "reports topology_complete: true in recovery.succeeded on a clean recovery" do
    # Pins wave 3's flag is actually wired into the payload (a regression that
    # never set it would otherwise pass every other test) and defaults true.
    with_proxied_session do |session, _|
      ch = session.open_channel
      ch.queue("test.topo.flag", durable: true) # something to replay

      succeeded = nil
      session.on_event("recovery.succeeded") { |_n, p| succeeded ||= p }

      toxiproxy_rabbitmq.down do
        wait_until("loss") { !session.open? }
      end
      wait_until("recovery") { succeeded }

      expect(succeeded[:topology_complete]).to be(true)
    end
  end

  it "survives four flaps in a row with exactly one consumer and no duplicates" do
    with_proxied_session do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 5)
      q  = ch.queue("test.flap.storm", durable: true)
      received = []
      ch.basic_consume(q.name, manual_ack: true) do |di, _h, body|
        received << body
        ch.basic_ack(di.delivery_tag)
      end

      4.times do |cycle|
        recovered = false
        handler = session.on_recovery { |_s| recovered = true }
        toxiproxy_rabbitmq.down do
          wait_until("cycle #{cycle}: loss noticed") { !session.open? }
        end
        wait_until("cycle #{cycle}: recovery") { recovered && session.open? && ch.open? }

        ch.basic_publish("flap-#{cycle}", routing_key: q.name)
        # Exactly once: a duplicated consumer registration would deliver twice.
        wait_until("cycle #{cycle}: delivery") { received.count("flap-#{cycle}") >= 1 }
        sleep 0.3 # give a would-be duplicate time to show up
        expect(received.count("flap-#{cycle}")).to eq(1)
      end

      expect(received).to match_array(%w[flap-0 flap-1 flap-2 flap-3])
      expect(ch.instance_variable_get(:@consumers).size).to eq(1)
      expect(session.channels.size).to eq(1)
    end
  end

  it "releases a publisher parked on the connection.blocked gate when the broker dies, and replays it under confirms" do
    with_proxied_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.blocked.drop", durable: true)
      ch.confirm_select # so the parked publish is recorded for replay, not fire-and-forget

      # The broker raised a resource alarm (simulated at the gate frame_io
      # keeps for connection.blocked) — and then died, the natural sequence
      # for a broker whose memory alarm precedes an OOM kill.
      session.instance_variable_get(:@frame_io).set_blocked("memory")

      publish_done = false
      publisher = Async::Task.current.async do
        ch.basic_publish("written after the alarm cleared", routing_key: q.name)
        publish_done = true
      end
      sleep 0.3
      expect(publish_done).to be(false) # parked on the blocked gate

      toxiproxy_rabbitmq.down do
        wait_until("recovery to start") { !session.open? }
      end
      wait_until("recovery") { session.open? && ch.open? }

      # The gate was released into recovery rather than hanging forever — the
      # whole point of the branch — so basic_publish returns...
      Async::Task.current.with_timeout(10) { publisher.wait }
      expect(publish_done).to be(true)
      # ...and because it was under confirms, the message was kept and
      # republished on the new connection rather than silently lost.
      wait_until("the replayed publish to land") do
        _di, _h, body = ch.basic_get(q.name, manual_ack: false)
        body == "written after the alarm cleared"
      end
    end
  end

  it "times out, retries and succeeds against a broker that accepts TCP but never answers" do
    with_proxied_session(connect_timeout: 1.2) do |session, _|
      attempts = []
      session.on_recovery_attempt { |n| attempts << n }

      blackhole(toxiproxy_rabbitmq) do
        sever_connection!(session)
        # Each reconnect completes TCP against the proxy listener and then
        # hangs in the handshake; connect_timeout must bound every attempt
        # rather than letting the first one hang forever.
        wait_until("two bounded connect attempts", timeout: 15) { attempts.size >= 2 }
      end

      wait_until("recovery once the black-hole lifts") { session.open? }
      session.with_channel { |c| c.queue("test.halfup.#{SecureRandom.hex(4)}", durable: true) }
    end
  end

  it "close during an outage wins: parked work is released and the session stays closed when the broker returns" do
    with_proxied_session do |session, _|
      ch = session.open_channel
      publish_error = nil
      publisher = nil

      toxiproxy_rabbitmq.down do
        wait_until("recovery to start") { ch.recovering? }
        publisher = Async::Task.current.async do
          ch.basic_publish("never", routing_key: "nowhere")
        rescue => e
          publish_error = e
        end
        sleep 0.2
        session.close
        Async::Task.current.with_timeout(10) { publisher.wait }
      end

      expect(publish_error).to be_a(AsyncRabbitMQ::Error)
      expect(session.closed?).to be true
      sleep 1.0 # the broker is back; a zombie recovery would reopen now
      expect(session.closed?).to be true
      expect(ch.closed?).to be true
    end
  end

  it "is connectable again after recovery exhausts its attempts" do
    with_proxied_session(recovery_attempts: 1, recovery_interval: 0.2) do |session, _|
      exhausted = false
      session.on_recovery_exhausted { |_s| exhausted = true }

      toxiproxy_rabbitmq.down do
        wait_until("recovery to give up") { exhausted }
      end

      expect(session.closed?).to be true
      # The same Session object must be able to start over now that the
      # broker is back — the natural reaction to the exhausted callback.
      session.connect
      expect(session.open?).to be true
      session.with_channel { |c| c.queue("test.after.exhaustion", durable: true) }
    end
  end

  it "abandons recovery with reason :authentication_failed when the user was deleted during the outage" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    user  = "revoked-#{SecureRandom.hex(4)}"
    create_vhost(vhost)
    http_put("/api/users/#{user}", { password: "pw", tags: "" }.to_json)
    http_put("/api/permissions/#{URI.encode_uri_component(vhost)}/#{user}",
             { configure: ".*", write: ".*", read: ".*" }.to_json)

    session = proxied_session(vhost, username: user, password: "pw")
    session.connect
    ch = session.open_channel

    exhausted_payload = nil
    exhausted = false
    session.on_event("recovery.exhausted") { |_n, p| exhausted_payload = p }
    session.on_recovery_exhausted { |_s| exhausted = true }

    toxiproxy_rabbitmq.down do
      wait_until("the outage to be noticed") { !session.open? }
      http_delete("/api/users/#{user}")
    end

    # The broker is reachable again but refuses the credentials: retrying
    # cannot help, so recovery must abandon at once rather than loop.
    wait_until("recovery to abandon") { exhausted }
    expect(exhausted_payload[:reason]).to eq(:authentication_failed)
    expect(session.closed?).to be true
    expect(ch.closed?).to be true
  ensure
    session&.close rescue nil
    http_delete("/api/users/#{user}") rescue nil
    delete_vhost(vhost) rescue nil
  end
end
