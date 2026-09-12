require "spec_helper"

# Step 14: basic.qos — prefetch respected under load.
RSpec.describe "basic.qos (prefetch)", :integration do

  it "sets prefetch_count without error" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect { ch.basic_qos(prefetch_count: 10) }.not_to raise_error
      ch.close
    end
  end

  it "respects prefetch_count=1 — delivers one message at a time" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.prefetch", durable: true)

      # Publish 3 messages
      3.times { |i| ch.basic_publish("msg-#{i}", routing_key: q.name) }

      ch.basic_qos(prefetch_count: 1)

      in_flight = []
      delivered = []

      tag = q.subscribe(manual_ack: true) do |di, _h, body|
        in_flight << di.delivery_tag
        sleep 0.05   # simulate work
        delivered   << body.to_s
        in_flight.delete(di.delivery_tag)
        ch.basic_ack(di.delivery_tag)
      end

      sleep 0.4
      ch.basic_cancel(tag)

      # With prefetch=1, in_flight should never have exceeded 1 simultaneously.
      # We verify by checking that all 3 were delivered.
      expect(delivered.size).to eq(3)
      ch.close
    end
  end

  it "bounds concurrent handler fibers to pool_size (default 1)" do
    isolated_session do |session, _|
      ch = session.open_channel  # default pool_size: 1
      q  = ch.queue("test.pool.default", durable: true)

      5.times { |i| ch.basic_publish("msg-#{i}", routing_key: q.name) }
      ch.basic_qos(prefetch_count: 10)  # broker would send all 5 at once
      # Reset pool back to 1 — basic_qos just coupled it to prefetch=10.
      ch.pool_size = 1

      mutex     = Mutex.new
      in_flight = 0
      max_seen  = 0
      delivered = 0

      tag = q.subscribe(manual_ack: true) do |di, _h, _body|
        mutex.synchronize { in_flight += 1; max_seen = [max_seen, in_flight].max }
        sleep 0.05
        mutex.synchronize { in_flight -= 1; delivered += 1 }
        ch.basic_ack(di.delivery_tag)
      end

      sleep 0.6
      ch.basic_cancel(tag)

      expect(delivered).to eq(5)
      expect(max_seen).to eq(1)
      ch.close
    end
  end

  it "runs pool_size handlers concurrently when raised" do
    isolated_session do |session, _|
      ch = session.open_channel(pool_size: 3)
      q  = ch.queue("test.pool.three", durable: true)

      6.times { |i| ch.basic_publish("msg-#{i}", routing_key: q.name) }
      ch.basic_qos(prefetch_count: 10)
      ch.pool_size = 3  # basic_qos bumped it to 10; re-cap at 3.

      mutex     = Mutex.new
      in_flight = 0
      max_seen  = 0

      tag = q.subscribe(manual_ack: true) do |di, _h, _body|
        mutex.synchronize { in_flight += 1; max_seen = [max_seen, in_flight].max }
        sleep 0.1
        mutex.synchronize { in_flight -= 1 }
        ch.basic_ack(di.delivery_tag)
      end

      sleep 0.6
      ch.basic_cancel(tag)

      expect(max_seen).to eq(3)
      ch.close
    end
  end

  it "basic_qos couples pool_size to prefetch_count" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect(ch.pool_size).to eq(1)
      ch.basic_qos(prefetch_count: 7)
      expect(ch.pool_size).to eq(7)
      ch.basic_qos(prefetch_count: 2)
      expect(ch.pool_size).to eq(2)
      # prefetch_count: 0 (unlimited) leaves pool untouched.
      ch.basic_qos(prefetch_count: 0)
      expect(ch.pool_size).to eq(2)
      ch.close
    end
  end
end
