require "spec_helper"

# The broker ends the connection ON PURPOSE: an operator's Force Close in the
# management UI, rabbitmqctl close_connection, or a vhost deleted under a live
# session. Unlike transport loss, the broker sends connection.close on channel 0
# and waits for close-ok — a detection path of its own (FrameIO replies inline
# and triggers recovery). The vhost case adds a recovery mode nothing else
# produces: TCP connects fine, the handshake itself is refused, every retry.
RSpec.describe "broker-initiated connection.close", :integration do

  def wait_until(what, timeout: 15)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  it "recovers from an operator force-close, consumers included" do
    name = "force-close-me-#{SecureRandom.hex(4)}"
    isolated_session(connection_name: name,
                     recovery_interval: 0.3, recovery_max_interval: 0.5) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.forced.#{SecureRandom.hex(4)}", durable: true)

      received = []
      ch.basic_consume(q.name, manual_ack: true) do |di, _h, body|
        received << body
        ch.basic_ack(di.delivery_tag)
      end

      lost = nil
      recovered = false
      session.on_event("connection.lost") { |_n, p| lost ||= p }
      session.on_recovery { |_s| recovered = true }

      force_close_connection!(name)

      wait_until("recovery after the forced close") { recovered && session.open? }
      # 320 CONNECTION_FORCED arrives as a connection-level error.
      expect(lost[:error]).to eq("AsyncRabbitMQ::ConnectionError")
      expect(lost[:message]).to match(/320|forced/i)

      # The consumer came back with the new connection.
      ch.basic_publish("after the force-close", routing_key: q.name)
      wait_until("the re-registered consumer to deliver") { received.include?("after the force-close") }
    end
  end

  it "abandons recovery cleanly when the vhost was deleted under the session" do
    vhost = "test-vanishing-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost,
      recovery_attempts: 2, recovery_interval: 0.2, recovery_max_interval: 0.3
    )
    session.connect
    ch = session.open_channel
    ch.queue("test.vanishing", durable: true)

    exhausted = false
    attempts  = []
    session.on_recovery_exhausted { |_s| exhausted = true }
    session.on_recovery_attempt   { |n| attempts << n }

    # Park a publish mid-outage: it must be RELEASED with an error when
    # recovery gives up, not left waiting forever.
    publish_error = nil
    publisher = nil

    # Deleting the vhost force-closes its connections (541/320 family) AND
    # makes every reconnect handshake fail: TCP connects, connection.open is
    # refused. Recovery cannot succeed and must exhaust its attempts.
    delete_vhost(vhost)

    wait_until("recovery to start") { !session.open? }
    publisher = Async::Task.current.async do
      ch.basic_publish("never getting there", routing_key: "test.vanishing")
    rescue => e
      publish_error = e
    end

    wait_until("recovery to give up") { exhausted }
    expect(attempts).not_to be_empty
    expect(session.closed?).to be true

    Async::Task.current.with_timeout(10) { publisher.wait }
    expect(publish_error).to be_a(AsyncRabbitMQ::Error)
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
