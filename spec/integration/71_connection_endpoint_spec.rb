require "spec_helper"

# Connection establishment goes through io-endpoint. These pin what the move
# changes or must keep: TCP_NODELAY on (io-endpoint's default, and the
# messaging-client convention), TCP_USER_TIMEOUT still applied to the TCP
# socket underneath (TLS included), name resolution, and IPv6.
RSpec.describe "connection establishment via io-endpoint", :integration do

  def tcp_socket_of(session)
    socket = session.instance_variable_get(:@frame_io).instance_variable_get(:@socket)
    socket.respond_to?(:to_io) ? socket.to_io : socket
  end

  # #bool, not #int: Windows reports this option as a single byte, Linux as an
  # int, and #bool accepts either.
  def nodelay?(socket)
    socket.getsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY).bool
  end

  it "opens plain connections with TCP_NODELAY" do
    isolated_session do |session, _|
      expect(nodelay?(tcp_socket_of(session))).to be true
    end
  end

  it "opens TLS connections with TCP_NODELAY on the underlying socket" do
    ca = File.expand_path("../docker/certs/ca_certificate.pem", __dir__)
    skip "TLS certificates not generated" unless File.exist?(ca)

    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_TLS_PORT, tls_ca_certificates: ca)
    session.connect
    raw = session.instance_variable_get(:@frame_io).instance_variable_get(:@socket)
    expect(raw).to be_a(OpenSSL::SSL::SSLSocket)
    expect(nodelay?(raw.to_io)).to be true
  ensure
    session&.close rescue nil
  end

  it "still applies TCP_USER_TIMEOUT where the platform has it" do
    skip "TCP_USER_TIMEOUT is Linux-only" unless Socket.const_defined?(:TCP_USER_TIMEOUT)

    isolated_session(heartbeat: 10) do |session, _|
      value = tcp_socket_of(session).getsockopt(Socket::IPPROTO_TCP, Socket::TCP_USER_TIMEOUT).int
      expect(value).to eq(20_000) # default: twice the heartbeat, in milliseconds
    end
  end

  it "resolves a hostname and connects" do
    # "localhost" can resolve to ::1 first; the endpoint tries each address
    # until one answers, so this works whether or not the broker listens on v6.
    session = AsyncRabbitMQ::Session.new(host: "localhost", port: RABBITMQ_PORT)
    session.connect
    expect(session.open?).to be true
  ensure
    session&.close rescue nil
  end

  it "connects to an IPv6 literal" do
    begin
      Socket.tcp("::1", RABBITMQ_PORT, connect_timeout: 1).close
    rescue SystemCallError, SocketError, IOError
      skip "broker not reachable on [::1]:#{RABBITMQ_PORT}"
    end

    session = AsyncRabbitMQ::Session.from_uri("amqp://guest:guest@[::1]:#{RABBITMQ_PORT}/%2F")
    session.connect
    expect(session.open?).to be true
    expect(session.host).to eq("::1")
  ensure
    session&.close rescue nil
  end
end
