require "spec_helper"

# Step 13: Consumer — basic.consume, basic.cancel, basic.deliver.
RSpec.describe "Consumer (basic.consume)", :integration do

  it "receives a delivery via subscribe" do
    isolated_session do |session, _|
      ch       = session.open_channel
      q        = ch.queue("test.consume", durable: true)
      received = []

      consumer_tag = q.subscribe(manual_ack: true) do |di, _headers, body|
        received << body.to_s
        ch.basic_ack(di.delivery_tag)
      end

      ch.basic_publish("consumed!", routing_key: q.name)
      sleep 0.3   # allow delivery task to run

      expect(received).to include("consumed!")

      ch.basic_cancel(consumer_tag)
      ch.close
    end
  end

  it "cancels a consumer successfully" do
    isolated_session do |session, _|
      ch  = session.open_channel
      q   = ch.queue("test.cancel", durable: true)
      tag = q.subscribe { |*| }

      expect { ch.basic_cancel(tag) }.not_to raise_error
      ch.close
    end
  end

  it "receives deliveries in separate async tasks (concurrent)" do
    isolated_session do |session, _|
      ch          = session.open_channel
      q           = ch.queue("test.concurrent", durable: true)
      received    = []
      deliveries  = 5

      tag = q.subscribe(manual_ack: true) do |di, _h, body|
        received << body.to_s
        ch.basic_ack(di.delivery_tag)
      end

      deliveries.times { |i| ch.basic_publish("msg-#{i}", routing_key: q.name) }
      sleep 0.5

      expect(received.size).to eq(deliveries)
      ch.basic_cancel(tag)
      ch.close
    end
  end

  # Handlers run in the channel's long-lived worker fibers, one per pool_size,
  # rather than a fresh task per delivery. That is what keeps a backlog costing
  # its messages instead of a fiber stack per message, and it is why a pool of
  # one serialises deliveries through a single fiber.
  it "runs the subscriber block in a handler worker, off the calling fiber" do
    isolated_session do |session, _|
      ch    = session.open_channel # pool_size: 1
      q     = ch.queue("test.async.task", durable: true)
      tasks = []

      tag = q.subscribe(manual_ack: false) { |*| tasks << Async::Task.current }

      3.times { ch.basic_publish("x", routing_key: q.name) }
      sleep 0.3

      expect(tasks.size).to eq(3)
      # One worker at pool_size 1, so all three share it.
      expect(tasks.uniq.size).to eq(1)
      # And it is not the fiber that published, nor the one dispatching frames.
      expect(tasks.first).not_to eq(Async::Task.current)
      ch.basic_cancel(tag)
      ch.close
    end
  end

  it "runs handlers in pool_size distinct workers" do
    isolated_session do |session, _|
      ch    = session.open_channel(pool_size: 2)
      q     = ch.queue("test.async.workers", durable: true)
      tasks = []
      gate  = Async::Condition.new

      # Both workers have to be busy at once for the second to be observed, so
      # hold the first handler until its partner has also recorded itself.
      tag = q.subscribe(manual_ack: false) do |*|
        tasks << Async::Task.current
        gate.wait if tasks.size < 2
      end

      2.times { ch.basic_publish("x", routing_key: q.name) }
      sleep 0.4

      expect(tasks.uniq.size).to eq(2)
      gate.signal
      ch.basic_cancel(tag)
      ch.close
    end
  end
end
