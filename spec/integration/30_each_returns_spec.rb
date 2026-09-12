require "spec_helper"

# Channel#each blocks while the consumer is live and returns when the
# consumer or channel goes away, instead of parking the fiber forever.
RSpec.describe "Channel#each termination", :integration do

  it "yields deliveries and returns when the channel is closed" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.each.close.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("one", routing_key: q.name)

      received = []
      consumer = Async::Task.current.async do
        ch.each(q.name) { |_di, _h, body| received << body }
        :returned
      end

      sleep 0.3
      expect(received).to eq(["one"])
      expect(consumer).to be_running

      ch.close
      expect(consumer.wait).to eq(:returned)
    end
  end

  it "returns when the broker cancels the consumer (queue deleted)" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.each.cancel.#{SecureRandom.hex(4)}", durable: true)

      consumer = Async::Task.current.async do
        ch.each(q.name) { |*| }
        :returned
      end
      sleep 0.2
      expect(consumer).to be_running

      # Deleting the queue makes RabbitMQ send basic.cancel to the consumer
      # (consumer_cancel_notify capability).
      session.with_channel { |other| other.queue_delete(q.name) }

      expect(consumer.wait).to eq(:returned)
      expect(ch.open?).to be true
      ch.close
    end
  end

  it "returns when the broker closes the channel with an error" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.each.error.#{SecureRandom.hex(4)}", durable: true)

      consumer = Async::Task.current.async do
        ch.each(q.name) { |*| }
        :returned
      end
      sleep 0.2

      ch.basic_ack(4242)   # unknown delivery tag: 406, broker closes the channel
      expect(consumer.wait).to eq(:returned)
      expect(ch.closed?).to be true
    end
  end
end
