require "spec_helper"

# Session#connect when the broker refuses the handshake (as opposed to the
# TCP connect failing). The session must clean up its tasks, report the right
# error class, and remain usable for a later connect.
RSpec.describe "Session#connect broker-refused handshake", :integration do

  def frame_io_of(session)
    session.instance_variable_get(:@frame_io)
  end

  it "raises AuthenticationError with code 403 for bad credentials and cleans up" do
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: RABBITMQ_PORT,
      username: "guest", password: "definitely-wrong-#{SecureRandom.hex(4)}",
      logger: Logger.new(nil)
    )

    expect { session.connect }.to raise_error(AsyncRabbitMQ::AuthenticationError) { |e|
      expect(e.code).to eq(403)
      expect(e.text).to include("ACCESS_REFUSED")
    }

    expect(session.closed?).to be true
    expect(frame_io_of(session)).to be_nil
    expect(session.instance_variable_get(:@connecting)).to be false
  end

  it "raises ConnectionError with code 530 for a missing vhost and cleans up" do
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: RABBITMQ_PORT,
      vhost: "no-such-vhost-#{SecureRandom.hex(6)}",
      logger: Logger.new(nil)
    )

    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionError) { |e|
      expect(e).not_to be_a(AsyncRabbitMQ::ChannelError)
      expect(e.code).to eq(530)
      expect(e.text).to include("NOT_ALLOWED")
    }

    expect(session.closed?).to be true
    expect(frame_io_of(session)).to be_nil
  end

  it "can connect later on the same Session once the cause is fixed, with recovery still enabled" do
    vhost   = "test-#{SecureRandom.hex(6)}"
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost, logger: Logger.new(nil)
    )

    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionError)

    create_vhost(vhost)
    session.connect
    expect(session.open?).to be true
    # A failed connect must not leave the session flagged as closed-by-user,
    # otherwise automatic recovery would be silently disabled.
    expect(session.instance_variable_get(:@closed_by_user)).to be false

    session.with_channel { |ch| expect(ch.queue("test.reuse", durable: true).name).to eq("test.reuse") }
    session.close
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  def running_child_tasks
    children = Async::Task.current.children or return 0
    children.each.to_a.count(&:running?)
  end

  it "does not leave reader or writer tasks running after a refused handshake" do
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: RABBITMQ_PORT,
      vhost: "no-such-vhost-#{SecureRandom.hex(6)}",
      logger: Logger.new(nil)
    )
    before = running_child_tasks

    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionError)
    sleep 0.1

    expect(running_child_tasks).to be <= before
  end

  it "fails fast when the broker drops the TCP connection during the handshake" do
    # Simulate a broker (or firewall) that closes the socket mid-negotiation.
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: RABBITMQ_PORT, connect_timeout: 20, logger: Logger.new(nil)
    )
    allow(session).to receive(:send_connection_start_ok).and_wrap_original do |m, *args|
      sever_connection!(session)
      m.call(*args)
    end

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    # A dropped TCP connection is a failure to connect, same class as a refused one.
    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError, /Could not connect/)
    expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 5
    expect(session.closed?).to be true
    expect(session.instance_variable_get(:@connecting)).to be false
  end

  # OpenSSL::SSL::SSLSocket does not close the socket it wraps when the
  # handshake fails, and a failed connect has no frame_io to stop, so a broker
  # with an expired or mismatched certificate would leak one descriptor per
  # attempt — once every recovery interval, forever.
  it "closes the underlying socket when the TLS handshake fails" do
    raw_sockets = []
    allow(TCPSocket).to receive(:new).and_wrap_original do |original, *args|
      original.call(*args).tap { |socket| raw_sockets << socket }
    end

    3.times do
      # The plaintext AMQP port never completes a TLS handshake.
      session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, tls: true,
                                           connect_timeout: 5, logger: Logger.new(nil))
      expect { session.connect }.to raise_error(AsyncRabbitMQ::Error)
    end

    expect(raw_sockets.size).to eq(3)
    expect(raw_sockets).to all(be_closed)
  end
end
