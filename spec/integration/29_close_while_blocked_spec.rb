require "spec_helper"

# connection.blocked only gates publishes. Closing channels or the session
# while the broker has the connection blocked must not hang, and publishers
# parked on the gate are released with an error when the session goes away.
RSpec.describe "closing while connection.blocked is active", :integration do

  def elapsed
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
  end

  it "closes a channel while blocked without waiting for connection.unblocked" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.queue("test.blocked.close.#{SecureRandom.hex(4)}", durable: true)
      session.instance_variable_get(:@frame_io).set_blocked("memory alarm (simulated)")

      expect(elapsed { ch.close }).to be < 2
      expect(ch.closed?).to be true
    end
  end

  it "closes the session while blocked and releases a parked publisher with ConnectionError" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
    session.connect
    ch = session.open_channel
    q  = ch.queue("test.blocked.session.#{SecureRandom.hex(4)}", durable: true)

    session.instance_variable_get(:@frame_io).set_blocked("memory alarm (simulated)")

    publisher_error = nil
    publisher = Async::Task.current.async do
      ch.basic_publish("parked", routing_key: q.name)
    rescue AsyncRabbitMQ::ConnectionError => e
      publisher_error = e
    end
    sleep 0.1
    expect(publisher).to be_running

    expect(elapsed { session.close }).to be < 6   # bounded by the CloseOk timeout at most
    expect(session.closed?).to be true

    publisher.wait
    expect(publisher_error).to be_a(AsyncRabbitMQ::ConnectionError)
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "still parks publishes while blocked and lets them through on unblocked" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.blocked.publish.#{SecureRandom.hex(4)}", durable: true)
      frame_io = session.instance_variable_get(:@frame_io)

      frame_io.set_blocked("memory alarm (simulated)")
      done = false
      Async::Task.current.async do
        ch.basic_publish("after unblock", routing_key: q.name)
        done = true
      end
      sleep 0.2
      expect(done).to be false

      frame_io.set_unblocked
      sleep 0.2
      expect(done).to be true

      _di, _h, body = ch.basic_get(q.name, manual_ack: false)
      expect(body).to eq("after unblock")
      ch.close
    end
  end
end
