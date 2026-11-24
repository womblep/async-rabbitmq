require "spec_helper"

# Wave-2 teardown-path fixes: a connection loss during a resync, a close during
# a resync, and a consumer whose queue is gone during re-registration.
RSpec.describe "resync and recovery edges", :integration do

  def wait_until(timeout = 15)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.05
    end
  end

  # Make the channel's resync park: it writes channel.close and then waits for a
  # close-ok that never arrives, staying :resyncing for the full rpc_timeout.
  def swallow_close_ok(ch)
    original = ch.method(:handle_method)
    ch.define_singleton_method(:handle_method) do |method|
      if !@__ate_close_ok && method.is_a?(AMQ::Protocol::Channel::CloseOk)
        @__ate_close_ok = true
        next nil
      end
      original.call(method)
    end
  end

  # Swallow one queue.declare-ok so the next declare times out and triggers a resync.
  def swallow_next_declare_ok(ch)
    original = ch.method(:handle_method)
    ch.define_singleton_method(:handle_method) do |method|
      if !@__ate_declare && method.is_a?(AMQ::Protocol::Queue::DeclareOk)
        @__ate_declare = true
        next nil
      end
      original.call(method)
    end
  end

  it "hands a channel mid-resync to recovery when the connection drops, instead of killing it", :toxiproxy do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         rpc_timeout: 3, recovery_interval: 0.3, recovery_max_interval: 0.5)
    session.connect
    ch = session.open_channel
    id = ch.channel_id
    q  = ch.queue("test.resync.drop.#{SecureRandom.hex(4)}", durable: true)

    received = []
    ch.basic_consume(q.name, manual_ack: false) { |_di, _h, body| received << body }

    swallow_next_declare_ok(ch)
    swallow_close_ok(ch)

    # This declare's reply is swallowed, so after rpc_timeout it raises and the
    # channel enters a resync that will park (close-ok is swallowed too).
    resyncer = Async::Task.current.async do
      ch.queue("test.resync.trigger.#{SecureRandom.hex(4)}", durable: true) rescue nil
    end

    wait_until { ch.resyncing? }

    # Drop the connection while the channel is mid-resync.
    toxiproxy_rabbitmq.down do
      wait_until { ch.recovering? || ch.closed? }
    end

    # Recovery must reopen the channel (same id) and restore the consumer,
    # rather than leaving it permanently closed and dropped from the session.
    wait_until { session.open? && ch.open? }
    expect(ch.channel_id).to eq(id)
    expect(session.channels.map(&:channel_id)).to include(id)

    resyncer.wait

    ch.basic_publish("after drop during resync", routing_key: q.name)
    wait_until { received.include?("after drop during resync") }
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "actually closes a channel asked to close mid-resync, rather than silently no-opping" do
    isolated_session(rpc_timeout: 2) do |session, _|
      ch = session.open_channel
      id = ch.channel_id
      swallow_next_declare_ok(ch)
      swallow_close_ok(ch)

      Async::Task.current.async do
        ch.queue("test.resync.close.#{SecureRandom.hex(4)}", durable: true) rescue nil
      end

      wait_until { ch.resyncing? }

      closer = Async::Task.current.async { ch.close }
      # close parks until the resync settles, then closes the live channel.
      Async::Task.current.with_timeout(15) { closer.wait }

      expect(ch).to be_closed
      expect(session.channels.map(&:channel_id)).not_to include(id)
    end
  end

  it "re-registers the surviving consumers when one consumer's queue is gone after recovery", :toxiproxy do
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    # recover_topology: false so the deleted queue is not re-declared on reopen
    # — that is what forces the doomed consumer's re-subscribe to hit the 404
    # and exercise the re-registration cascade. The survivors were never
    # deleted, so they still exist on the broker and re-subscribe cleanly.
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
                                         recover_topology: false,
                                         recovery_interval: 0.3, recovery_max_interval: 0.5)
    session.connect
    ch = session.open_channel

    good1 = ch.queue("test.survive.a.#{SecureRandom.hex(4)}", durable: true)
    doomed = ch.queue("test.survive.doomed.#{SecureRandom.hex(4)}", durable: true)
    good2 = ch.queue("test.survive.b.#{SecureRandom.hex(4)}", durable: true)

    got1 = []
    got2 = []
    ch.basic_consume(good1.name, manual_ack: false) { |_di, _h, b| got1 << b }
    doomed_tag = ch.basic_consume(doomed.name, manual_ack: false) { |_di, _h, _b| }
    ch.basic_consume(good2.name, manual_ack: false) { |_di, _h, b| got2 << b }

    # Delete the doomed queue *during the outage*, over a direct connection that
    # bypasses toxiproxy, so no broker basic.cancel ever reaches our consumer —
    # it is still in the registry when recovery re-registers it, and the 404
    # then closes the reopened channel. Deleting it while connected would arrive
    # as a cancel and drop the consumer before recovery, missing this path.
    toxiproxy_rabbitmq.down do
      wait_until { ch.recovering? }
      direct = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
      direct.connect
      dch = direct.open_channel
      dch.queue_delete(doomed.name)
      direct.close
    end
    # Wait for re-registration to FINISH, not just for the channel to be open:
    # finish_recovery! marks the channel open and releases parked publishers
    # before it re-registers consumers. A publish sent between the doomed
    # consumer's re-subscribe and the broker's 404 channel.close is dropped
    # by the closing channel (no confirms here), and the delivery wait below
    # times out — which made this example flaky. The doomed consumer is
    # dropped from @consumers only at the very end of re_register_consumers.
    wait_until do
      session.open? && ch.open? && !ch.instance_variable_get(:@consumers).key?(doomed_tag)
    end

    # The doomed consumer is gone; the two good ones work.
    ch.basic_publish("to a", routing_key: good1.name)
    ch.basic_publish("to b", routing_key: good2.name)
    wait_until { got1.include?("to a") && got2.include?("to b") }

    consumers = ch.instance_variable_get(:@consumers)
    expect(consumers.values.map { |e| e[:queue_name] }).to contain_exactly(good1.name, good2.name)
    expect(consumers).not_to have_key(doomed_tag)
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
