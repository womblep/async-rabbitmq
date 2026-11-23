require "spec_helper"

# Every infrastructure task a session spawns — reader, writer, heartbeat,
# channel-0 monitor, recovery loop, channel dispatch loops, reconcilers — is
# registered in a per-lifecycle Async::Barrier, and #close cancels the barrier
# as its catch-all. These pin the property the barrier exists for: nothing
# outlives the session, provably, even after recovery churn.
RSpec.describe "background task teardown", :integration do

  def wait_until(what, timeout: 15)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.05
    end
  end

  def live_tasks(barrier)
    alive = []
    barrier.tasks.each { |node| alive << node.task unless node.task.finished? }
    alive
  end

  it "leaves no live infrastructure task after close, even after a recovery" do
    isolated_session(recovery_interval: 0.2, recovery_max_interval: 0.4) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.teardown", durable: true)
      ch.basic_consume(q.name, manual_ack: false) { |*| }

      # One full recovery cycle, so the barrier has seen task churn: old
      # reader/writer/heartbeat die, new ones spawn.
      recover_connection!(session)
      ch.basic_publish("alive", routing_key: q.name)

      barrier = session.instance_variable_get(:@background)
      expect(barrier).not_to be_nil
      expect(live_tasks(barrier)).not_to be_empty # dispatch, reader, writer, heartbeat...

      session.close

      # The barrier handle is dropped by close (next connect gets a fresh one)...
      expect(session.instance_variable_get(:@background)).to be_nil
      # ...and every task it held is stopped: no leaked dispatch loops,
      # monitors or reconcilers keep running against a closed session.
      wait_until("all infrastructure tasks to finish") { live_tasks(barrier).empty? }
    end
  end

  it "refuses to start infrastructure after close rather than resurrecting a barrier" do
    isolated_session do |session, _|
      session.close
      expect(session.spawn_background { sleep 10 }).to be_nil
      expect(session.instance_variable_get(:@background)).to be_nil
    end
  end

  it "leaves nothing behind when a connect fails" do
    session = AsyncRabbitMQ::Session.new(host: "127.0.0.1", port: 1, connect_timeout: 1)
    expect { session.connect }.to raise_error(AsyncRabbitMQ::Error)
    # cleanup_after_failed_connect stopped and dropped the lifecycle barrier.
    expect(session.instance_variable_get(:@background)).to be_nil
  end

  it "starts a fresh barrier on reconnect after close" do
    isolated_session do |session, vhost|
      first = session.instance_variable_get(:@background)
      session.close
      wait_until("old lifecycle to wind down") { live_tasks(first).empty? }

      session.connect
      second = session.instance_variable_get(:@background)
      expect(second).not_to be_nil
      expect(second).not_to equal(first) # a cancelled barrier refuses new tasks
      session.with_channel { |c| c.queue("test.teardown.again", durable: true) }
    end
  end
end
