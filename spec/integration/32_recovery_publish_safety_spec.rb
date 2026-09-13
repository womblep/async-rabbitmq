require "spec_helper"

# Channel state across a connection loss. Nothing published while the
# connection is down may be silently dropped, and channels of a session that
# is closed or gives up on recovery must be closed too.
RSpec.describe "channel state and publish safety across recovery", :integration do

  def encoded_publish(ch, session, payload, routing_key)
    frames = AMQ::Protocol::Basic::Publish.encode(
      ch.channel_id, payload.b, { delivery_mode: 1 }, "", routing_key, false, false, session.frame_max
    )
    frames.map(&:encode).join
  end

  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  it "parks a publish issued while recovering and delivers it after reconnect", :toxiproxy do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         recovery_interval: 0.3, recovery_max_interval: 0.5)
    session.connect
    ch = session.open_channel
    q  = ch.queue("test.park.#{SecureRandom.hex(4)}", durable: true)

    publisher = nil
    toxiproxy_rabbitmq.down do
      wait_until { ch.recovering? }
      publisher = Async::Task.current.async { ch.basic_publish("published during outage", routing_key: q.name) }
      sleep 0.5
      expect(publisher).to be_running   # parked, not failed, not dropped
    end

    wait_until { session.open? && ch.open? }
    publisher.wait
    _di, _h, body = ch.basic_get(q.name, manual_ack: false)
    expect(body).to eq("published during outage")
    session.close
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "re-publishes messages that were unconfirmed when the connection dropped" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.republish.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select
      ch.basic_publish("confirmed before drop", routing_key: q.name)
      expect(ch.wait_for_confirms).to be true

      # Stand in for a message whose ack never arrived: pending on the channel
      # with its encoded frames, but never received by the broker.
      ch.instance_variable_get(:@pending_confirms)[99] = encoded_publish(ch, session, "replayed after drop", q.name)

      recover_connection!(session)

      expect(ch.wait_for_confirms).to be true
      expect(ch.unconfirmed_tags).to be_empty

      bodies = []
      while (msg = ch.basic_get(q.name, manual_ack: false))
        bodies << msg[2]
      end
      expect(bodies).to contain_exactly("confirmed before drop", "replayed after drop")
      ch.close
    end
  end

  it "marks channels closed when the session is closed" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.closed.#{SecureRandom.hex(4)}", durable: true)
      session.close
      expect(ch.closed?).to be true
      expect { ch.basic_publish("late", routing_key: q.name) }.to raise_error(AsyncRabbitMQ::NotOpenError)
    end
  end

  it "marks channels closed when recovery is disabled and the connection is lost" do
    isolated_session(auto_recover: false) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.norecover.#{SecureRandom.hex(4)}", durable: true)
      session.trigger_recovery(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "simulated"))
      expect(ch.closed?).to be true
      expect(session.closed?).to be true
      expect { ch.basic_publish("late", routing_key: q.name) }.to raise_error(AsyncRabbitMQ::NotOpenError)
    end
  end

  it "fails parked operations when recovery is exhausted", :toxiproxy do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         recovery_attempts: 1, recovery_interval: 0.2, recovery_max_interval: 0.3)
    session.connect
    ch = session.open_channel
    q  = ch.queue("test.exhaust.#{SecureRandom.hex(4)}", durable: true)

    error = nil
    toxiproxy_rabbitmq.down do
      wait_until { ch.recovering? }
      publisher = Async::Task.current.async do
        ch.basic_publish("doomed", routing_key: q.name)
      rescue AsyncRabbitMQ::Error => e
        error = e
      end
      wait_until { session.closed? }
      publisher.wait
    end

    expect(error).to be_a(AsyncRabbitMQ::ConnectionError)
    expect(error.text).to include("exhausted")
    expect(ch.closed?).to be true
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "closing a channel while recovering does not reopen it after reconnect", :toxiproxy do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         recovery_interval: 0.3, recovery_max_interval: 0.5)
    session.connect
    ch  = session.open_channel
    ch2 = session.open_channel

    toxiproxy_rabbitmq.down do
      wait_until { ch.recovering? }
      ch.close
      expect(ch.closed?).to be true
    end

    wait_until { session.open? && ch2.open? }
    expect(ch.closed?).to be true
    expect(session.instance_variable_get(:@channels).keys).to eq([ch2.channel_id])
    session.close
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
