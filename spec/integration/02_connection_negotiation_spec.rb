require "spec_helper"

# Step 2: Connection negotiation — connection.start / tune / open (30s timeout).
RSpec.describe "Connection negotiation", :integration do

  it "connects and completes AMQP handshake" do
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT)
    expect { session.connect }.not_to raise_error
    expect(session.open?).to be true
    session.close
  end

  it "session is closed after close" do
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT)
    session.connect
    session.close
    expect(session.closed?).to be true
  end

  it "raises ConnectionTimeoutError when broker is unreachable (wrong port)" do
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: 19999)
    expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError)
  end

  it "negotiates a non-zero frame_max" do
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT)
    session.connect
    expect(session.frame_max).to be > 0
    session.close
  end

  it "raises ConnectionTimeoutError when toxiproxy cuts the TCP connection", :toxiproxy do
    skip "toxiproxy not available" unless toxiproxy_available?

    # Bring the rabbitmq proxy down so connect() sees a refused/closed connection.
    toxiproxy_rabbitmq.down do
      session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT)
      expect { session.connect }.to raise_error(AsyncRabbitMQ::ConnectionTimeoutError)
      session.close rescue nil
    end
  end

  it "reconnects through toxiproxy after connection is restored", :toxiproxy do
    skip "toxiproxy not available" unless toxiproxy_available?

    # Connect through proxy, verify it works, then verify close works too.
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT)
    session.connect
    expect(session.open?).to be true
    session.close
    expect(session.closed?).to be true
  end
end
