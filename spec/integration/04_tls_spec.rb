require "spec_helper"

# Step 4: TLS connect — direct TLS, minimum TLS 1.2.
RSpec.describe "TLS connection", :integration do

  let(:tls_port)  { RABBITMQ_TLS_PORT }
  let(:tls_cert)  { ENV["RABBITMQ_TLS_CERT"] }
  let(:tls_key)   { ENV["RABBITMQ_TLS_KEY"] }
  let(:tls_ca)    { ENV["RABBITMQ_TLS_CA"] }

  # Returns true only when the port accepts an actual TLS handshake.
  # A plain TCP connect is not enough — the port could be open but not
  # serving TLS (e.g. not configured), which would cause SSLError at test time.
  let(:tls_available?) do
    require "openssl"
    require "socket"
    ctx = OpenSSL::SSL::SSLContext.new
    ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_NONE)
    raw = TCPSocket.new(RABBITMQ_HOST, tls_port)
    ssl = OpenSSL::SSL::SSLSocket.new(raw, ctx)
    ssl.connect
    ssl.close rescue nil
    raw.close  rescue nil
    true
  rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, OpenSSL::SSL::SSLError, IOError
    false
  end

  it "connects over TLS when available" do
    skip "RabbitMQ TLS port not available" unless tls_available?

    ctx = OpenSSL::SSL::SSLContext.new
    ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_PEER)
    ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION
    if tls_ca
      ctx.ca_file = tls_ca
      ctx.cert    = OpenSSL::X509::Certificate.new(File.read(tls_cert)) if tls_cert
      ctx.key     = OpenSSL::PKey::RSA.new(File.read(tls_key))          if tls_key
    else
      ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_NONE)
    end

    session = AsyncRabbitMQ::Session.new(
      host:        RABBITMQ_HOST,
      port:        tls_port,
      tls:         true,
      tls_context: ctx
    )
    expect { session.connect }.not_to raise_error
    expect(session.open?).to be true
    session.close
  end

  # The plain options: a CA path (or PEM) and nothing else, verifying the peer
  # and its hostname against the certificate's SANs (DNS:localhost, 127.0.0.1).
  it "connects with tls_ca_certificates: and no hand-built context" do
    skip "RabbitMQ TLS port not available" unless tls_available?
    ca = File.expand_path("../docker/certs/ca_certificate.pem", __dir__)
    skip "CA cert not generated" unless File.exist?(ca)

    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: tls_port, tls_ca_certificates: ca)
    session.connect
    expect(session.open?).to be true
    session.close
  end

  it "takes the CA as PEM text, and turns TLS on when certificate material is given" do
    skip "RabbitMQ TLS port not available" unless tls_available?
    ca = File.expand_path("../docker/certs/ca_certificate.pem", __dir__)
    skip "CA cert not generated" unless File.exist?(ca)

    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: tls_port,
                                         tls_ca_certificates: [File.read(ca)])
    expect(session.instance_variable_get(:@tls)).to be true
    session.connect
    expect(session.open?).to be true
    session.close
  end

  it "rejects a broker whose CA it does not trust when verify_peer is on" do
    skip "RabbitMQ TLS port not available" unless tls_available?

    other_ca = OpenSSL::X509::Certificate.new.tap do |c|
      key = OpenSSL::PKey::RSA.new(2048)
      c.version = 2
      c.serial = 1
      c.subject = c.issuer = OpenSSL::X509::Name.parse("/CN=not the broker's CA")
      c.public_key = key.public_key
      c.not_before = Time.now - 60
      c.not_after = Time.now + 3600
      c.sign(key, OpenSSL::Digest.new("SHA256"))
    end

    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: tls_port, tls_ca_certificates: other_ca.to_pem)
    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError, /TLS handshake failed/)
  end

  it "refuses TLS 1.0 (min version enforced)" do
    skip "RabbitMQ TLS port not available" unless tls_available?

    ctx = OpenSSL::SSL::SSLContext.new
    # Attempt to force TLS 1.0 — should be rejected by our ctx.min_version = TLS1_2
    ctx.max_version = OpenSSL::SSL::TLS1_VERSION rescue nil

    session = AsyncRabbitMQ::Session.new(
      host:        RABBITMQ_HOST,
      port:        tls_port,
      tls:         true,
      tls_context: ctx
    )
    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError)
  end
end
