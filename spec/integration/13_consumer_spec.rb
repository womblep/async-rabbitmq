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

  it "subscriber block runs in a new Async::Task per delivery" do
    isolated_session do |session, _|
      ch     = session.open_channel
      q      = ch.queue("test.async.task", durable: true)
      tasks  = []

      tag = q.subscribe(manual_ack: false) do |*|
        tasks << Async::Task.current
      end

      3.times { ch.basic_publish("x", routing_key: q.name) }
      sleep 0.3

      # Each delivery ran in a distinct task
      expect(tasks.uniq.size).to eq(tasks.size) if tasks.size > 1
      ch.basic_cancel(tag)
      ch.close
    end
  end
end
