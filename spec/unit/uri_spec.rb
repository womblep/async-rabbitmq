require "spec_helper"

# Session.from_uri — parsing is delegated to AMQ::URI (amq-protocol). No broker needed.
RSpec.describe "Session.from_uri parsing" do
  def session_for(uri, **kwargs)
    AsyncRabbitMQ::Session.from_uri(uri, **kwargs)
  end

  def ivar(session, name)
    session.instance_variable_get(:"@#{name}")
  end

  it "percent-decodes credentials and keeps a literal + in a password" do
    s = session_for("amqp://us%40er:pa+ss%21@rabbit:5672/%2Fvault")
    expect(s.username).to eq("us@er")
    expect(ivar(s, :password)).to eq("pa+ss!")
    expect(s.vhost).to eq("/vault")
    expect(s.host).to eq("rabbit")
    expect(s.port).to eq(5672)
  end

  it "uses the scheme default port when none is given" do
    expect(session_for("amqp://rabbit/vh").port).to eq(5672)
    expect(session_for("amqps://rabbit/vh").port).to eq(5671)
  end

  it "treats a missing or empty vhost path as the default vhost" do
    expect(session_for("amqp://rabbit").vhost).to eq("/")
    expect(session_for("amqp://rabbit/").vhost).to eq("/")
  end

  it "rejects a multi-segment vhost path" do
    expect { session_for("amqp://rabbit/a/b") }.to raise_error(ArgumentError, /percent-encode/)
  end

  it "rejects non-AMQP schemes" do
    expect { session_for("http://rabbit") }.to raise_error(ArgumentError, /amqp/)
  end

  it "sets tls for amqps://" do
    expect(ivar(session_for("amqps://rabbit/vh"), :tls)).to be true
    expect(ivar(session_for("amqp://rabbit/vh"), :tls)).to be false
  end

  it "honours heartbeat, connection_timeout, channel_max and auth_mechanism query params" do
    s = session_for("amqp://rabbit/vh?heartbeat=10&connection_timeout=5&channel_max=100&auth_mechanism=EXTERNAL")
    expect(ivar(s, :heartbeat)).to eq(10)
    expect(ivar(s, :connect_timeout)).to eq(5)
    expect(ivar(s, :channel_max)).to eq(100)
    expect(ivar(s, :auth_mechanism)).to eq("EXTERNAL")
  end

  it "leaves defaults alone when query params are absent" do
    s = session_for("amqp://rabbit/vh")
    expect(ivar(s, :heartbeat)).to eq(60)
    expect(ivar(s, :connect_timeout)).to eq(AsyncRabbitMQ::Session::CONNECT_TIMEOUT)
    expect(ivar(s, :channel_max)).to eq(2047)
    expect(ivar(s, :auth_mechanism)).to be_nil
  end

  it "builds a verifying TLS context from cacertfile" do
    ca = File.expand_path("../docker/certs/ca_certificate.pem", __dir__)
    skip "CA cert not generated" unless File.exist?(ca)
    s   = session_for("amqps://rabbit/vh?cacertfile=#{URI.encode_www_form_component(ca)}")
    ctx = ivar(s, :tls_context)
    expect(ctx).to be_a(OpenSSL::SSL::SSLContext)
    expect(ctx.verify_mode).to eq(OpenSSL::SSL::VERIFY_PEER)
    # The CA went into the trust store: it vouches for the test broker's certificate.
    server = OpenSSL::X509::Certificate.new(File.read(File.expand_path("../docker/certs/server_certificate.pem", __dir__)))
    expect(ctx.cert_store.verify(server)).to be true
  end

  it "disables peer verification only when verify=false is explicit" do
    ctx = ivar(session_for("amqps://rabbit/vh?verify=false"), :tls_context)
    expect(ctx.verify_mode).to eq(OpenSSL::SSL::VERIFY_NONE)
  end

  it "does not build a TLS context when no TLS query params are present" do
    expect(ivar(session_for("amqps://rabbit/vh"), :tls_context)).to be_nil
  end

  it "rejects TLS query params on a plain amqp:// URI" do
    expect { session_for("amqp://rabbit/vh?verify=true") }.to raise_error(ArgumentError, /amqps/)
  end

  it "lets keyword arguments override URI values" do
    s = session_for("amqp://a:b@rabbit/vh?heartbeat=10", username: "guest", heartbeat: 30)
    expect(s.username).to eq("guest")
    expect(ivar(s, :heartbeat)).to eq(30)
  end

  it "builds the failover address list from several URIs" do
    s = AsyncRabbitMQ::Session.from_uri("amqp://u:p@r1:5672/vh", "amqp://r2:5673/vh")
    expect(s.addresses).to eq([["r1", 5672], ["r2", 5673]])
    expect(s.username).to eq("u")
  end
end
