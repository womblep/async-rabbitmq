require "spec_helper"

# Step 57: a consumer backlog must cost a queued item, not a fiber.
#
# dispatch_loop starts the Async::Task *outside* the pool semaphore and takes
# the semaphore from inside the task, so pool_size caps how many handlers run,
# not how many exist. A parked task costs ~15 KB — fiber stack and all —
# against ~200 bytes for a queued tuple, so a few thousand backlogged messages
# are hundreds of MB rather than single-digit MB.
#
# basic_qos does not fix the auto-ack case. RabbitMQ applies prefetch only to
# *unacknowledged* messages, so on a manual_ack: false consumer the limit never
# engages and the broker sends the whole queue however small the prefetch. The
# control example below proves that distinction on the live broker instead of
# assuming it, so a failure here cannot be mistaken for a broken measurement.
RSpec.describe "consumer backlog memory", :integration do
  BACKLOG_DEPTH    = 600
  BACKLOG_PREFETCH = 10

  # A fixed worker pool costs pool_size fibers plus a little bookkeeping. This
  # is far above that and still ~12x below BACKLOG_DEPTH, so a per-delivery fiber
  # cannot slip under it while a real pool cannot bump into it.
  BACKLOG_FIBER_BUDGET = 50

  # Live fibers, after collecting any that have finished. Parked handler tasks
  # stay reachable from the semaphore's wait list, so GC cannot remove them:
  # whatever is still counted here is genuinely retained.
  def live_fibers
    GC.start
    ObjectSpace.each_object(Fiber).count(&:alive?)
  end

  def queue_stats(vhost, name)
    http_get("/api/queues/#{URI.encode_www_form_component(vhost)}/#{URI.encode_www_form_component(name)}")
  end

  # Poll the management API until the queue reaches an expected state, rather
  # than sleeping a guessed interval. The stats database lags publishes by
  # seconds; a fixed sleep would sample the fiber count before the backlog had
  # arrived, which is precisely how this measurement fails silently and passes.
  def await_queue(vhost, name, what, timeout: 30)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    last = nil
    loop do
      last = begin
        queue_stats(vhost, name)
      rescue StandardError
        nil
      end
      return last if last && yield(last)
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        raise "timed out waiting for #{name} to be #{what} (last stats: #{last.inspect})"
      end
      sleep 0.25
    end
  end

  # The broker reporting the queue drained only means the frames are on the
  # wire — the client may still be turning them into tasks. Sample until two
  # consecutive readings agree, so the number is a steady state and any error
  # is in the conservative direction.
  def settled_fiber_count(timeout: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    last = live_fibers
    loop do
      sleep 0.3
      now = live_fibers
      return now if now == last

      last = now
      raise "fiber count never settled (last: #{last})" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    end
  end

  # Fill a queue, start a consumer whose handler never returns, and report how
  # many fibers the client retained while the whole backlog sat there waiting.
  def backlog_fiber_cost(session, vhost, count:, manual_ack:, prefetch: nil)
    ch = session.open_channel
    q  = ch.queue("backlog.#{SecureRandom.hex(4)}", durable: true)

    count.times { |i| ch.basic_publish("m-#{i}", routing_key: q.name) }
    # Measure nothing until the broker demonstrably holds the whole backlog.
    await_queue(vhost, q.name, "fully published") { |s| s["messages_ready"] == count }

    if prefetch
      ch.basic_qos(prefetch_count: prefetch)
      ch.pool_size = 1 # basic_qos coupled the pool to prefetch; keep handlers serialized
    end

    gate   = Async::Condition.new
    before = live_fibers
    tag    = q.subscribe(manual_ack: manual_ack) { |_di, _h, _b| gate.wait }

    if manual_ack
      # Prefetch engages: the broker stops at BACKLOG_PREFETCH and leaves the rest queued.
      await_queue(vhost, q.name, "capped at prefetch") { |s| s["messages_unacknowledged"] == prefetch }
    else
      # Auto-ack: the broker treats every message as acked on send, so it drains.
      await_queue(vhost, q.name, "fully delivered") { |s| s["messages_ready"].to_i.zero? }
    end

    retained = settled_fiber_count - before

    gate.signal # release the parked handlers so teardown can finish
    begin
      ch.basic_cancel(tag)
    rescue StandardError
      nil
    end
    begin
      ch.close
    rescue StandardError
      nil
    end
    retained
  end

  # Control. This passes on the current code: with manual acks the broker
  # honours prefetch, so only BACKLOG_PREFETCH deliveries ever reach the client and the
  # unbounded-task bug has nothing to act on. If this ever fails, the harness
  # is broken, not the gem.
  it "retains at most prefetch_count fibers on a manual-ack consumer" do
    isolated_session do |session, vhost|
      retained = backlog_fiber_cost(session, vhost, count: BACKLOG_DEPTH, manual_ack: true, prefetch: BACKLOG_PREFETCH)
      expect(retained).to be <= BACKLOG_FIBER_BUDGET
    end
  end

  # The defect. Prefetch cannot bound an auto-ack consumer, so the whole
  # backlog arrives and dispatch_loop turns each delivery into a parked task.
  it "does not retain a fiber per backlogged delivery on an auto-ack consumer" do
    isolated_session do |session, vhost|
      retained = backlog_fiber_cost(session, vhost, count: BACKLOG_DEPTH, manual_ack: false)
      expect(retained).to be <= BACKLOG_FIBER_BUDGET
    end
  end

  # ---------------------------------------------------------------------------
  # Workers are created on demand. basic_qos couples pool_size to prefetch, so
  # creating them eagerly meant asking for a prefetch of 500 cost 500 fibers,
  # and setting pool_size back down - the documented escape hatch from that
  # coupling - retired all but pool_size of them without ever handing one a
  # delivery. Their touched stacks stay in RSS: 12.6 MB at prefetch 500 across
  # two channels, measured on the soak box.
  # ---------------------------------------------------------------------------

  it "does not create a fiber per prefetch slot" do
    isolated_session do |session, _|
      ch = session.open_channel

      before = live_fibers
      ch.basic_qos(prefetch_count: 500)
      # Sampled before pool_size is reset, because that is the peak: the eager
      # version created 500 workers here and only then retired 499 of them.
      created = live_fibers - before
      ch.pool_size = 1

      expect(ch.pool_size).to eq(1)
      expect(created).to be <= 5
      ch.close
    end
  end

  it "creates workers only as deliveries need them, up to pool_size" do
    isolated_session do |session, _|
      ch = session.open_channel(pool_size: 4)
      q  = ch.queue("lazy.#{SecureRandom.hex(4)}", durable: true)

      idle = live_fibers
      gate = Async::Condition.new
      seen = 0
      tag  = q.subscribe(manual_ack: false) do |_d, _h, _b|
        seen += 1
        gate.wait # hold every worker, so concurrency has to grow to proceed
      end

      # A consumer with nothing to do needs no workers at all.
      expect(live_fibers - idle).to be <= 5

      4.times { ch.basic_publish("x", routing_key: q.name) }
      wait_until { seen == 4 }

      # Four blocked handlers means four workers really were created, so the
      # concurrency bound is still honoured - it is only reached on demand.
      expect(seen).to eq(4)

      gate.signal
      begin
        ch.basic_cancel(tag)
      rescue StandardError
        nil
      end
      ch.close
    end
  end

  # ---------------------------------------------------------------------------
  # What happens to a backlog that is still queued when the channel goes away.
  # The retirement sentinel goes on the BACK of the work queue, so workers
  # finish what is already queued before standing down. That is right for a
  # graceful close and wrong after a connection loss, where the broker has
  # requeued the same messages and will send them again.
  # ---------------------------------------------------------------------------

  # Build a backlog behind a handler that blocks on +gate+, and return the list
  # the handlers record into. Auto-ack on purpose: the broker then considers
  # every message delivered, so nothing is redelivered after a connection loss
  # and the count below measures the queued backlog alone.
  def backlog_behind_a_blocked_handler(session, count:, gate:, handled:)
    ch = session.open_channel # pool_size 1: one worker, blocked by handler #1
    q  = ch.queue("teardown.#{SecureRandom.hex(4)}", durable: true)
    count.times { |i| ch.basic_publish("m-#{i}", routing_key: q.name) }

    ch.basic_consume(q.name, manual_ack: false) do |_d, _h, body|
      handled << body.to_s
      gate.wait if handled.size == 1
    end

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
    until ch.backlog >= count - 1
      raise "backlog never built (got #{ch.backlog}, handled #{handled.size})" if
        Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.1
    end
    ch
  end

  def wait_until(timeout: 15)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    sleep 0.1 until yield || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  end

  it "finishes the queued backlog on a graceful close" do
    isolated_session do |session, _|
      gate    = Async::Condition.new
      handled = []
      ch      = backlog_behind_a_blocked_handler(session, count: 50, gate: gate, handled: handled)

      # Closing does not need the workers, so this runs while the backlog sits
      # there: the sentinel lands behind it, not in front of it.
      ch.close
      gate.signal

      wait_until { handled.size == 50 }
      expect(handled.size).to eq(50)
    end
  end

  it "discards the queued backlog when the connection goes" do
    isolated_session do |session, _|
      gate    = Async::Condition.new
      handled = []
      backlog_behind_a_blocked_handler(session, count: 50, gate: gate, handled: handled)

      # The broker requeues what was unacknowledged and sends it again on the
      # new connection. Running these handlers too would process each message
      # a second time, so the queued backlog has to go.
      #
      # Wait for the session to have actually noticed and recovered before
      # releasing the handler: severing the socket only makes the reader see
      # EOF on a later tick, and an earlier version of this example let the
      # worker drain all 50 before the loss was detected, proving nothing.
      recover_connection!(session)
      gate.signal

      wait_until(timeout: 5) { handled.size > 1 }
      expect(handled.size).to eq(1)
    end
  end

  # The same defect stated as the property that actually matters: what the
  # client retains must track its concurrency, not the depth of the queue.
  it "retains a fiber count independent of queue depth" do
    isolated_session do |session, vhost|
      shallow = backlog_fiber_cost(session, vhost, count: 200, manual_ack: false)
      deep    = backlog_fiber_cost(session, vhost, count: 800, manual_ack: false)
      expect(deep - shallow).to be <= BACKLOG_FIBER_BUDGET
    end
  end
end
