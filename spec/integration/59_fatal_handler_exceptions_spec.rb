require "spec_helper"

# A handler is application code and can raise anything, not just StandardError.
# `rescue => e` is StandardError only, so before this was fixed a handler
# raising outside that hierarchy escaped both run_handler and dispatch_loop and
# took the channel's frame dispatch with it: the channel stayed "open" while
# delivering nothing -- no confirms, no RPC replies, no close-ok -- so the next
# synchronous call on it hung until rpc_timeout, or forever without one.
#
# SystemStackError is the realistic way in. An accidental infinite recursion
# inside a handler is an ordinary bug and should fail that one delivery, not
# silence the channel.
#
# The other half of the contract is what must still get through. Async stops a
# task by raising Async::Stop into it, and Async::Stop is deliberately not a
# StandardError. A rescue wide enough to catch ScriptError is wide enough to
# swallow that too, which would leave workers unstoppable and a channel close
# or reactor shutdown hanging on a parked handler.
RSpec.describe "handler exceptions outside StandardError", :integration do
  # Enough of a Basic::Deliver for run_handler and its instrumentation.
  FakeDelivery = Struct.new(:consumer_tag, :delivery_tag, :redelivered)

  def fake_entry(queue_name, &block)
    { queue_name: queue_name, block: block, manual_ack: false }
  end

  describe "a handler that raises outside StandardError" do
    it "fails the delivery without killing the channel" do
      # rpc_timeout bounds the assertion: if dispatch died, the passive declare
      # below would park forever rather than fail.
      isolated_session(rpc_timeout: 5) do |session, _|
        ch = session.open_channel
        q  = ch.queue("fatal.script.#{SecureRandom.hex(4)}", durable: true)

        raised = 0
        ch.basic_consume(q.name, manual_ack: false) do |_d, _h, _b|
          raised += 1
          raise ScriptError, "not a StandardError"
        end

        ch.basic_publish("trigger", routing_key: q.name)
        sleep 1.5

        expect(raised).to eq(1)
        # A passive declare needs dispatch_loop to hand back the declare-ok, so
        # this succeeding is the channel proving it still dispatches.
        expect(ch.queue(q.name, passive: true).name).to eq(q.name)
        ch.close
      end
    end

    it "keeps delivering to the same consumer afterwards" do
      isolated_session(rpc_timeout: 5) do |session, _|
        ch = session.open_channel
        q  = ch.queue("fatal.recover.#{SecureRandom.hex(4)}", durable: true)

        seen = []
        ch.basic_consume(q.name, manual_ack: false) do |_d, _h, body|
          seen << body.to_s
          raise SystemStackError, "stack level too deep" if body.to_s == "bad"
        end

        ch.basic_publish("bad",  routing_key: q.name)
        sleep 0.5
        ch.basic_publish("good", routing_key: q.name)
        sleep 1.0

        # One poisoned delivery must not retire the consumer.
        expect(seen).to eq(%w[bad good])
        ch.close
      end
    end

    it "reports it through the same path as any other handler error" do
      isolated_session(rpc_timeout: 5) do |session, _|
        ch = session.open_channel
        q  = ch.queue("fatal.hook.#{SecureRandom.hex(4)}", durable: true)

        seen_error = nil
        ch.on_handler_error { |error, _method, _queue| seen_error = error }

        ch.basic_consume(q.name, manual_ack: false) do |_d, _h, _b|
          raise ScriptError, "not a StandardError"
        end

        ch.basic_publish("trigger", routing_key: q.name)
        sleep 1.5

        expect(seen_error).to be_a(ScriptError)
        ch.close
      end
    end
  end

  # White box on purpose: the propagate-or-swallow decision is the whole fix,
  # and driving it through a live delivery would mean stopping the reactor to
  # observe it, which cannot be asserted without hanging the suite when wrong.
  describe "what must still propagate" do
    it "re-raises Async::Stop rather than treating it as a handler error" do
      isolated_session do |session, _|
        ch = session.open_channel
        entry = fake_entry("q") { raise Async::Stop, "stopping" }

        expect do
          ch.send(:run_handler, FakeDelivery.new("tag", 1, false), nil, "body", entry)
        end.to raise_error(Async::Stop)

        ch.close
      end
    end

    it "re-raises the signals that are not this delivery's problem" do
      isolated_session do |session, _|
        ch = session.open_channel

        [SystemExit, Interrupt, NoMemoryError].each do |fatal|
          entry = fake_entry("q") { raise fatal }
          expect do
            ch.send(:run_handler, FakeDelivery.new("tag", 1, false), nil, "body", entry)
          end.to raise_error(fatal), "expected #{fatal} to propagate"
        end

        ch.close
      end
    end

    it "swallows everything else, including outside StandardError" do
      isolated_session do |session, _|
        ch = session.open_channel

        [ScriptError, SystemStackError, RuntimeError].each do |caught|
          entry = fake_entry("q") { raise caught, "boom" }
          expect do
            ch.send(:run_handler, FakeDelivery.new("tag", 1, false), nil, "body", entry)
          end.not_to raise_error, "expected #{caught} to be handled, not propagated"
        end

        ch.close
      end
    end

    it "does not let an on_handler_error hook raising outside StandardError escape" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.on_handler_error { |_e, _m, _q| raise ScriptError, "hook is application code too" }

        entry = fake_entry("q") { raise "ordinary failure" }
        expect do
          ch.send(:run_handler, FakeDelivery.new("tag", 1, false), nil, "body", entry)
        end.not_to raise_error

        ch.close
      end
    end
  end
end
