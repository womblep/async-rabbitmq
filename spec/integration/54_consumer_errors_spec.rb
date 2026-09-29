require "spec_helper"

# A consumer block that raises must not leave its delivery unacked forever:
# once prefetch messages are stuck that way the consumer stops receiving
# anything at all.
RSpec.describe "consumer error handling", :integration do

  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  it "keeps delivering after a handler raises, instead of stalling at prefetch" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 1)     # one stuck message would stall it
      qname = "test.handler.raise.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)

      seen = []
      ch.basic_consume(qname, manual_ack: true) do |di, _h, body|
        seen << body
        raise "boom on #{body}" if body == "a"
        ch.basic_ack(di.delivery_tag)
      end

      %w[a b c].each { |b| ch.basic_publish(b, routing_key: q.name) }
      wait_until(15) { seen.size >= 3 }

      expect(seen).to eq(%w[a b c])
      ch.close
    end
  end

  it "calls on_handler_error with the exception, delivery and queue" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 1)
      qname = "test.handler.hook.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)

      captured = []
      ch.on_handler_error { |err, method, queue| captured << [err, method, queue] }
      ch.basic_consume(qname, manual_ack: true) { |_di, _h, _b| raise ArgumentError, "nope" }

      ch.basic_publish("x", routing_key: q.name)
      wait_until { !captured.empty? }

      err, method, queue = captured.first
      expect(err).to be_a(ArgumentError)
      expect(err.message).to eq("nope")
      expect(method).to be_a(AMQ::Protocol::Basic::Deliver)
      expect(queue).to eq(qname)
      ch.close
    end
  end

  it "dead-letters the failed message rather than requeueing it forever" do
    isolated_session do |session, _|
      ch    = session.open_channel
      suffix = SecureRandom.hex(4)
      dlx    = "test.dlx.#{suffix}"
      ch.exchange(dlx, type: :fanout, durable: true)
      dlq = ch.queue("test.dlq.#{suffix}", durable: true)
      dlq.bind(exchange: dlx)

      qname = "test.handler.dlx.#{suffix}"
      q = ch.queue(qname, durable: true, arguments: { "x-dead-letter-exchange" => dlx })
      ch.basic_qos(prefetch_count: 1)

      attempts = 0
      ch.basic_consume(qname, manual_ack: true) { |_di, _h, _b| attempts += 1; raise "always fails" }

      ch.basic_publish("poison", routing_key: q.name)
      wait_until { attempts >= 1 }
      sleep 1.0   # a requeue loop would keep incrementing

      expect(attempts).to eq(1)
      got = ch.basic_get(dlq.name, manual_ack: false)
      expect(got).not_to be_nil
      expect(got[2]).to eq("poison")
      ch.close
    end
  end

  it "does not nack an auto-ack delivery" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 5)
      qname = "test.handler.autoack.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)

      seen = []
      ch.basic_consume(qname, manual_ack: false) { |_di, _h, b| seen << b; raise "boom" }

      ch.basic_publish("x", routing_key: q.name)
      wait_until { !seen.empty? }
      sleep 0.4

      # Nacking an already-auto-acked tag would be an unknown delivery tag
      # (406) and would take the channel down with it.
      expect(ch.open?).to be true
      ch.close
    end
  end
end
