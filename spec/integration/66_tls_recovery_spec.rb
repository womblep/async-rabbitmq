require "spec_helper"

# Recovery over TLS. Every other recovery test runs over plain TCP; a TLS
# client's reconnect must redo the full handshake — TCP, then TLS with the
# stored CA material and hostname — inside the recovery loop, and a dying SSL
# socket surfaces different error classes than a plain one. The rabbitmq_tls
# toxiproxy proxy (21112 -> rabbitmq:5671) exists for exactly this and was
# previously used by no test.
RSpec.describe "recovery over TLS", :integration, :toxiproxy do

  def wait_until(what, timeout: 15)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  it "reconnects through a full TLS handshake and restores the consumer" do
    skip "Toxiproxy not available" unless toxiproxy_available?
    ca = File.expand_path("../docker/certs/ca_certificate.pem", __dir__)
    skip "TLS certificates not generated" unless File.exist?(ca)

    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: TOXIPROXY_TLS_PORT, vhost: vhost,
      tls_ca_certificates: ca,
      recovery_interval: 0.3, recovery_max_interval: 0.5, connect_timeout: 3
    )
    session.connect
    ch = session.open_channel
    q  = ch.queue("test.tls.recovery", durable: true)
    received = []
    ch.basic_consume(q.name, manual_ack: false) { |_di, _h, body| received << body }

    recovered = false
    session.on_recovery { |_s| recovered = true }

    toxiproxy_rabbitmq_tls.down do
      wait_until("the TLS connection loss to be noticed") { !session.open? }
    end

    wait_until("recovery over TLS") { recovered && session.open? }

    # Really TLS again, not a plain socket that happened to connect.
    socket = session.instance_variable_get(:@frame_io).instance_variable_get(:@socket)
    expect(socket).to be_a(OpenSSL::SSL::SSLSocket)

    ch.basic_publish("over TLS, after recovery", routing_key: q.name)
    wait_until("the re-registered consumer") { received.include?("over TLS, after recovery") }
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
