require "spec_helper"

# The native consumers against a real broker: Consumer, which is pulled from
# and settles nothing for you, and ManagedConsumer, which runs handlers
# concurrently and settles for them. What matters most here is what only a
# broker can show: where a message is after each way of stopping.
RSpec.describe "Native consumers", :integration do
  def wait_for(timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.02
    end
  end

  def ready(queue)
    queue.status[:message_count]
  end

  def consumers(queue)
    queue.status[:consumer_count]
  end

  def setup(session, name, count: 0, **queue_opts)
    ch = session.open_channel
    q  = ch.durable_queue("test.native.#{name}.#{SecureRandom.hex(4)}", **queue_opts)
    count.times { |i| ch.basic_publish("m#{i}", routing_key: q.name) }
    [ch, q]
  end

  describe AsyncRabbitMQ::Consumer do
    it "yields messages in order in the calling fiber, and returns :stopped after stop" do
      isolated_session do |session, _|
        ch, q = setup(session, "order", count: 5)
        consumer = q.consumer(prefetch: 2)
        caller_fiber = Fiber.current
        seen = []

        result = consumer.each do |message|
          expect(Fiber.current).to equal(caller_fiber)
          seen << message.body
          message.ack
          consumer.stop if seen.size == 5
        end

        expect(result).to eq(:stopped)
        expect(seen).to eq(%w[m0 m1 m2 m3 m4])
        expect(consumer).not_to be_running
        expect(consumers(q)).to eq(0)
        expect(ready(q)).to eq(0)
        ch.close
      end
    end

    it "supports ack, nack and reject, each doing what it says" do
      isolated_session do |session, _|
        ch, q = setup(session, "settle", count: 3)
        consumer = q.consumer(prefetch: 1)
        deliveries = []

        consumer.each do |message|
          deliveries << [message.body, message.redelivered?]
          case deliveries.size
          when 1 then message.nack(requeue: true)       # m0 comes back
          when 2 then message.reject(requeue: false)    # dropped, whichever it is
          else message.ack
          end
          consumer.stop if deliveries.size == 4
        end

        expect(deliveries.size).to eq(4)
        expect(deliveries.count { |_, redelivered| redelivered }).to eq(1)
        expect(ready(q)).to eq(0)
        ch.close
      end
    end

    it "leaves a message the block did not settle unacknowledged, until the channel closes" do
      isolated_session do |session, _|
        ch, q = setup(session, "unsettled", count: 1)
        consumer = q.consumer(prefetch: 1)
        held = nil
        consumer.each do |message|
          held = message
          consumer.stop
        end

        expect(held).not_to be_settled
        expect(ready(q)).to eq(0) # still out with this channel
        ch.close

        other = session.open_channel
        wait_for { other.queue(q.name, passive: true).message_count == 1 }
        other.close
      end
    end

    it "puts back what had arrived but was not handed out when it stops" do
      isolated_session do |session, _|
        ch, q = setup(session, "requeue", count: 5)
        consumer = q.consumer(prefetch: 5)

        consumer.each do |message|
          wait_for { consumer.instance_variable_get(:@subscription).buffer.size == 4 }
          message.ack
          consumer.stop
        end

        # The channel is still open: these were nacked back, not released by a close.
        wait_for { ready(q) == 4 }
        expect(ch).to be_open
        ch.close
      end
    end

    it "raises the block's exception from each, cancels, and puts the rest back" do
      isolated_session do |session, _|
        ch, q = setup(session, "raise", count: 3)
        consumer = q.consumer(prefetch: 3)

        expect do
          consumer.each do |message|
            message.ack
            raise ArgumentError, "bad order"
          end
        end.to raise_error(ArgumentError, "bad order")

        expect(consumer).not_to be_running
        wait_for { ready(q) == 2 }
        expect(consumers(q)).to eq(0)
        ch.close
      end
    end

    it "ends on a break, cancelling the consumer" do
      isolated_session do |session, _|
        ch, q = setup(session, "break", count: 2)
        consumer = q.consumer(prefetch: 1)
        result = consumer.each do |message|
          message.ack
          break
        end

        expect(result).to be_nil
        expect(consumers(q)).to eq(0)
        wait_for { ready(q) == 1 }
        ch.close
      end
    end

    it "returns :cancelled when the queue is deleted" do
      isolated_session do |session, _|
        ch, q = setup(session, "cancelled")
        consumer = q.consumer(prefetch: 1)
        task = Async { consumer.each { |message| message.ack } }
        wait_for { consumer.running? && consumer.consumer_tag }

        other = session.open_channel
        other.queue_delete(q.name)

        expect(task.wait).to eq(:cancelled)
        other.close
        ch.close
      end
    end

    it "returns :closed when the channel is closed under it" do
      isolated_session do |session, _|
        ch, q = setup(session, "closed")
        consumer = q.consumer(prefetch: 1)
        task = Async { consumer.each { |message| message.ack } }
        wait_for { consumer.consumer_tag }

        ch.close
        expect(task.wait).to eq(:closed)
      end
    end

    it "cancels the consumer when the task calling each is stopped" do
      isolated_session do |session, _|
        ch, q = setup(session, "task-stop", count: 3)
        consumer = q.consumer(prefetch: 3)
        got = []
        task = Async do
          consumer.each do |message|
            got << message
            sleep # holds the first message; two more wait behind it
          end
        end
        wait_for { got.size == 1 }
        task.stop
        task.wait

        expect(consumer).not_to be_running
        wait_for { ready(q) == 2 }
        expect(consumers(q)).to eq(0)
        expect(got.first).not_to be_settled
        ch.close
      end
    end

    it "keeps consuming across a connection recovery, and does not yield what the broker took back" do
      isolated_session do |session, _|
        ch, q = setup(session, "recovery", count: 3)
        consumer = q.consumer(prefetch: 3)
        seen = []
        release = Async::Promise.new
        task = Async do
          consumer.each do |message|
            seen << [message.body, message.redelivered?]
            release.wait if seen.size == 1 # hold m0 while the connection goes
            message.ack
            consumer.stop if seen.count { |_, redelivered| redelivered } == 3
          end
        end
        wait_for { seen.size == 1 }

        recover_connection!(session)
        release.resolve(true)

        expect(task.wait).to eq(:stopped)
        # m0 was yielded before the drop; its ack went stale. Then all three
        # again, redelivered, and nothing from the old connection's buffer.
        expect(seen).to eq([["m0", false], ["m0", true], ["m1", true], ["m2", true]])
        wait_for { ready(q) == 0 }
        ch.close
      end
    end

    it "does not hand out what was waiting once the channel has closed" do
      isolated_session do |session, _|
        ch, q = setup(session, "closed-buffer", count: 3)
        consumer = q.consumer(prefetch: 3)
        seen = []
        result = consumer.each do |message|
          seen << message.body
          wait_for { consumer.instance_variable_get(:@subscription).buffer.size == 2 }
          ch.close
        end

        expect(result).to eq(:closed)
        expect(seen).to eq(%w[m0]) # m1 and m2 could not have been settled
      end
    end

    it "is cancelled for good when stopped while the connection is recovering" do
      isolated_session do |session, _|
        ch, q = setup(session, "stop-in-recovery", count: 2)
        consumer = q.consumer(prefetch: 2)
        entered = false
        task = Async do
          consumer.each do |_message|
            entered = true
            sleep 0.01 until ch.recovering?
            consumer.stop # the channel is not open: the cancel has to wait for it
          end
        end
        wait_for { entered }

        recover_connection!(session)
        expect(task.wait).to eq(:stopped)

        # Left registered, it would have come back with the recovery, with
        # nobody reading: each message nacked back and redelivered, for ever.
        wait_for { consumers(q) == 0 }
        wait_for { ready(q) == 2 }
        sleep 0.3
        expect(ready(q)).to eq(2)
        expect(ch).to be_open
        ch.close
      end
    end

    it "can be run again after it has ended" do
      isolated_session do |session, _|
        ch, q = setup(session, "again", count: 2)
        consumer = q.consumer(prefetch: 1)
        bodies = []
        2.times do
          consumer.each do |message|
            bodies << message.body
            message.ack
            consumer.stop
          end
        end

        expect(bodies).to eq(%w[m0 m1])
        ch.close
      end
    end

    it "refuses a second each while one is running" do
      isolated_session do |session, _|
        ch, q = setup(session, "twice")
        consumer = q.consumer(prefetch: 1)
        task = Async { consumer.each { |message| message.ack } }
        wait_for { consumer.consumer_tag }

        expect { consumer.each { |_| nil } }.to raise_error(AsyncRabbitMQ::Error, /already running/)
        expect(consumer).to be_running # the refusal did not disturb the first
        consumer.stop
        expect(task.wait).to eq(:stopped)
        ch.close
      end
    end
  end

  describe AsyncRabbitMQ::ManagedConsumer do
    it "acks each message when its handler returns" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-ack", count: 4)
        consumer = q.managed_consumer
        bodies = []
        result = consumer.run do |message|
          bodies << message.body
          consumer.stop if bodies.size == 4
        end

        expect(result).to eq(:stopped)
        expect(bodies).to eq(%w[m0 m1 m2 m3])
        wait_for { ready(q) == 0 }
        ch.close
        other = session.open_channel
        expect(other.queue(q.name, passive: true).message_count).to eq(0) # acked, not just held
        other.close
      end
    end

    it "runs up to concurrency handlers at once, and no more" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-concurrency", count: 12)
        consumer = q.managed_consumer(concurrency: 4)
        running = 0
        peak    = 0
        done    = 0
        consumer.run do |_message|
          running += 1
          peak = [peak, running].max
          sleep 0.05
          running -= 1
          consumer.stop if (done += 1) == 12
        end

        expect(peak).to eq(4)
        expect(done).to eq(12)
        ch.close
      end
    end

    it "nacks without requeue when the handler raises, and carries on" do
      isolated_session do |session, _|
        ch  = session.open_channel
        dlq = ch.durable_queue("test.native.dlq.#{SecureRandom.hex(4)}")
        q   = ch.durable_queue("test.native.managed-raise.#{SecureRandom.hex(4)}",
                               arguments: { "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => dlq.name })
        3.times { |i| ch.basic_publish("m#{i}", routing_key: q.name) }

        errors = []
        events = []
        session.notifier.subscribe("consumer.handler_failed") { |_name, payload| events << payload }
        consumer = q.managed_consumer.on_error { |error, message| errors << [error.message, message.body] }
        handled = []
        consumer.run do |message|
          handled << message.body
          consumer.stop if handled.size == 3
          raise "boom" if message.body == "m1"
        end

        expect(handled).to eq(%w[m0 m1 m2])
        expect(errors).to eq([%w[boom m1]])
        expect(events.size).to eq(1)
        expect(events.first).to include(queue: q.name, disposition: :nack, requeue: false)
        wait_for { ready(dlq) == 1 }
        expect(ready(q)).to eq(0)
        ch.close
      end
    end

    it "requeues on an exception when told to, and the message comes back" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-requeue", count: 1)
        consumer = q.managed_consumer(exception: :reject, requeue: true)
        attempts = []
        consumer.run do |message|
          attempts << message.redelivered?
          raise "first time" if attempts.size == 1

          consumer.stop
        end

        expect(attempts).to eq([false, true])
        wait_for { ready(q) == 0 }
        ch.close
      end
    end

    it "acks a message whose handler raised when exception: :ack" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-exc-ack", count: 2)
        consumer = q.managed_consumer(exception: :ack)
        seen = 0
        consumer.run do |_message|
          consumer.stop if (seen += 1) == 2
          raise "ignored"
        end

        expect(seen).to eq(2)
        ch.close
        other = session.open_channel
        expect(other.queue(q.name, passive: true).message_count).to eq(0)
        other.close
      end
    end

    it "lets the handler's own settlement stand" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-own", count: 1)
        consumer = q.managed_consumer
        attempts = 0
        consumer.run do |message|
          attempts += 1
          if attempts == 1
            message.nack(requeue: true) # then returns normally: must not be acked on top
          else
            consumer.stop
          end
        end

        expect(attempts).to eq(2)
        sleep 0.2
        expect(ch).to be_open # a double settle would have closed it with a 406
        ch.close
      end
    end

    it "leaves the message unacknowledged with ack_on_success: false" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-noack", count: 1)
        consumer = q.managed_consumer(ack_on_success: false)
        held = nil
        consumer.run do |message|
          held = message
          consumer.stop
        end

        expect(held).not_to be_settled
        expect(held.ack).to be true
        ch.close
      end
    end

    it "drains on stop: running handlers finish and are acked, the rest go back" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-drain", count: 6)
        consumer = q.managed_consumer(concurrency: 2, prefetch: 6)
        started  = []
        finished = []
        release  = Async::Promise.new
        task = Async do
          consumer.run do |message|
            started << message.body
            release.wait
            finished << message.body
          end
        end
        wait_for { started.size == 2 }

        consumer.stop
        sleep 0.1
        expect(task).to be_running # waiting for the two in flight
        release.resolve(true)

        expect(task.wait).to eq(:stopped)
        expect(finished.sort).to eq(%w[m0 m1])
        wait_for { ready(q) == 4 }
        expect(consumers(q)).to eq(0)
        ch.close
        other = session.open_channel
        expect(other.queue(q.name, passive: true).message_count).to eq(4) # m0 and m1 were acked
        other.close
      end
    end

    it "stops handlers that outlast drain_timeout and puts their messages back" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-timeout", count: 1)
        consumer = q.managed_consumer(drain_timeout: 0.2)
        started = false
        task = Async do
          consumer.run do |_message|
            started = true
            sleep # never finishes
          end
        end
        wait_for { started }

        consumer.stop
        expect(task.wait).to eq(:stopped)
        wait_for { ready(q) == 1 }
        expect(ch).to be_open
        ch.close
      end
    end

    it "drains when the task running it is stopped" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-task-stop", count: 3)
        consumer = q.managed_consumer(prefetch: 3)
        finished = []
        release  = Async::Promise.new
        started  = false
        task = Async do
          consumer.run do |message|
            started = true
            release.wait
            finished << message.body
          end
        end
        wait_for { started }

        task.stop
        sleep 0.1
        expect(consumer).not_to be_running # cancelled already; only the handler is left
        release.resolve(true)
        task.wait

        expect(finished).to eq(%w[m0])
        wait_for { ready(q) == 2 }
        expect(consumers(q)).to eq(0)
        ch.close
        other = session.open_channel
        expect(other.queue(q.name, passive: true).message_count).to eq(2) # m0 was acked
        other.close
      end
    end

    it "stops a handler that outlasts drain_timeout after a task stop, and its message goes back" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-task-stop-timeout", count: 1)
        consumer = q.managed_consumer(drain_timeout: 0.2)
        started = false
        task = Async do
          consumer.run do |_message|
            started = true
            sleep
          end
        end
        wait_for { started }

        task.stop
        task.wait
        wait_for { ready(q) == 1 }
        expect(ch).to be_open
        ch.close
      end
    end

    it "keeps running across a connection recovery" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-recovery", count: 2)
        consumer = q.managed_consumer(concurrency: 2)
        bodies = []
        task = Async { consumer.run { |message| bodies << message.body } }
        wait_for { bodies.size == 2 }

        recover_connection!(session)
        ch.basic_publish("after", routing_key: q.name)
        wait_for { bodies.include?("after") }

        consumer.stop
        expect(task.wait).to eq(:stopped)
        ch.close
      end
    end

    it "reuses its handler fibers and never holds more than concurrency of them" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-no-leak", count: 50)
        consumer = q.managed_consumer(concurrency: 5)
        done = 0
        peak_tracked = 0
        consumer.run do |_message|
          peak_tracked = [peak_tracked, consumer.instance_variable_get(:@workers).size].max
          sleep 0.001
          consumer.stop if (done += 1) == 50
        end

        expect(done).to eq(50)
        expect(peak_tracked).to be_between(1, 5) # a pool, not a fiber kept per message
        ch.close
      end
    end

    it "refuses a second run while one is running, without disturbing it" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-twice", count: 1)
        consumer = q.managed_consumer
        bodies = []
        release = Async::Promise.new
        task = Async do
          consumer.run do |message|
            release.wait
            bodies << message.body
          end
        end
        wait_for { consumer.running? && consumer.consumer_tag }

        expect { consumer.run { |_| nil } }.to raise_error(AsyncRabbitMQ::Error, /already running/)
        release.resolve(true)
        wait_for { bodies == %w[m0] }
        consumer.stop
        expect(task.wait).to eq(:stopped)
        ch.close
      end
    end

    it "returns when the channel closes under it, even with every slot held by a stuck handler" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-closed-stuck", count: 2)
        consumer = q.managed_consumer(concurrency: 1, prefetch: 2, drain_timeout: 0.2)
        started = false
        task = Async do
          consumer.run do |_message|
            started = true
            sleep
          end
        end
        wait_for { started }

        ch.close
        expect(task.wait).to eq(:closed)
      end
    end

    it "rejects settings that make no sense" do
      isolated_session do |session, _|
        ch, q = setup(session, "managed-args")
        expect { q.managed_consumer(concurrency: 0) }.to raise_error(ArgumentError, /concurrency/)
        expect { q.managed_consumer(exception: :retry) }.to raise_error(ArgumentError, /exception/)
        expect { q.managed_consumer(drain_timeout: -1) }.to raise_error(ArgumentError, /drain_timeout/)
        expect { q.consumer(prefetch: 0) }.to raise_error(ArgumentError, /prefetch/)
        ch.close
      end
    end
  end
end
