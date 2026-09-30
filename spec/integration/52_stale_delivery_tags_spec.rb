require "spec_helper"

# Delivery tags are scoped to a channel on a connection: the broker restarts
# numbering at 1 after a reconnect or a channel reopen. A handler that was
# still running across that boundary must not have its ack applied to whatever
# message now holds that number.
RSpec.describe "Stale delivery tags", :integration do

  # Acking a tag the broker never issued is a 406, which closes the channel.
  # That is the cheapest way to make the broker restart tag numbering.
  def close_by_broker(ch)
    ch.basic_ack(4242)
    20.times { break if ch.closed?; sleep 0.05 }
    expect(ch.closed?).to be true
  end

  it "tags deliveries with the channel's generation" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.stale.gen.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("one", routing_key: q.name)
      sleep 0.2

      di, = ch.basic_get(q.name, manual_ack: true)
      expect(di.delivery_tag).to be_a(AsyncRabbitMQ::VersionedDeliveryTag)
      expect(di.delivery_tag.generation).to eq(ch.send(:instance_variable_get, :@delivery_generation))
      expect(ch.basic_ack(di.delivery_tag)).to be true
      ch.close
    end
  end

  it "stands in for the Integer it wraps" do
    tag = AsyncRabbitMQ::VersionedDeliveryTag.new(7, 3)

    expect(tag).to eq(7)
    expect(tag.to_i).to eq(7)
    expect(tag.to_s).to eq("7")
    expect([tag].include?(7)).to be true
    expect([tag, AsyncRabbitMQ::VersionedDeliveryTag.new(2, 3)].sort.map(&:to_i)).to eq([2, 7])
    expect(%w[a b c d e f g h][tag]).to eq("h")   # implicit to_int
  end

  it "drops an ack carrying a tag from a previous generation" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.stale.ack.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("first", routing_key: q.name)
      sleep 0.2

      di, = ch.basic_get(q.name, manual_ack: true)
      old_tag = di.delivery_tag
      expect(old_tag.generation).to eq(0)

      # Let the broker close and then reopen the channel: it forgets the
      # delivery and restarts tag numbering, exactly as after a reconnect.
      close_by_broker(ch)
      ch.reopen
      expect(ch.send(:instance_variable_get, :@delivery_generation)).to eq(1)

      # The message was requeued by the channel close; take it again so that
      # tag 1 on this generation is a real, different delivery.
      sleep 0.2
      di2, = ch.basic_get(q.name, manual_ack: true)
      expect(di2.delivery_tag.to_i).to eq(old_tag.to_i)      # same number...
      expect(di2.delivery_tag.generation).to eq(1)           # ...different generation

      # The stale ack must not be sent: it would acknowledge di2, which this
      # caller never processed.
      expect(ch.basic_ack(old_tag)).to be false
      expect(ch.open?).to be true

      # The current tag still works, and the channel survives.
      expect(ch.basic_ack(di2.delivery_tag)).to be true
      sleep 0.2
      expect(ch.open?).to be true
      ch.close
    end
  end

  it "drops a stale nack and reject too" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.stale.nack.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("x", routing_key: q.name)
      sleep 0.2
      di, = ch.basic_get(q.name, manual_ack: true)
      stale = di.delivery_tag

      close_by_broker(ch)
      ch.reopen

      expect(ch.basic_nack(stale)).to be false
      expect(ch.basic_reject(stale)).to be false
      sleep 0.2
      expect(ch.open?).to be true
      ch.close
    end
  end

  it "keeps accepting a bare Integer tag, unversioned" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.stale.int.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("x", routing_key: q.name)
      sleep 0.2
      di, = ch.basic_get(q.name, manual_ack: true)

      expect(ch.basic_ack(di.delivery_tag.to_i)).to be true
      ch.close
    end
  end

  it "survives a reconnect with a handler still holding an old tag", :toxiproxy do
    skip "Toxiproxy not available" unless toxiproxy_available?

    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         recovery_interval: 0.3, recovery_max_interval: 0.5)
    session.connect
    ch = session.open_channel
    ch.basic_qos(prefetch_count: 10)
    qname = "test.stale.recover.#{SecureRandom.hex(4)}"
    q = ch.queue(qname, durable: true)

    held    = nil
    gate    = Async::Condition.new
    handled = []

    ch.basic_consume(qname, manual_ack: true) do |di, _h, _body|
      if held.nil?
        held = di.delivery_tag
        gate.wait          # park this handler across the reconnect
        # Resumes after recovery; this ack belongs to a dead connection.
        handled << ch.basic_ack(held)
      else
        ch.basic_ack(di.delivery_tag)
      end
    end

    ch.basic_publish("a", routing_key: q.name)
    wait_until { !held.nil? }

    toxiproxy_rabbitmq.down { wait_until { ch.recovering? || !session.open? } }
    wait_until(20) { session.open? && ch.open? }

    gate.signal
    wait_until { !handled.empty? }

    # The parked handler's ack was recognised as stale and dropped, so the
    # channel is still alive rather than closed by a 406.
    expect(handled).to eq([false])
    expect(ch.open?).to be true
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end
end
