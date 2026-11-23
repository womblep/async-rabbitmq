require "spec_helper"

# Topology recovery: exchanges, queues and bindings declared through the
# session are re-declared after a reconnect, server-named queues are renamed
# through to their bindings and consumers, deleted entities stay deleted,
# passive declares are not recorded, and a filter or recover_topology: false
# can limit or disable it.
RSpec.describe "topology recovery", :integration do

  # Drop the connection and wait until recovery has completely finished
  # (on_recovery fires after channels are reopened, topology replayed and
  # consumers re-registered).
  def recover!(session, ch)
    expect(ch.open?).to be true
    recover_connection!(session)
    expect(ch.open?).to be true
  end

  # An auto-delete exchange bound only to an exclusive queue disappears with the
  # connection (the queue goes, the binding goes, the exchange auto-deletes), so
  # only recovery can bring the trio back.
  it "re-declares exchanges, queues and bindings and renames server-named queues" do
    isolated_session do |session, _|
      ch = session.open_channel
      ex = ch.exchange("test.topo.ex.#{SecureRandom.hex(4)}", type: :fanout, auto_delete: true)
      q  = ch.queue("", exclusive: true)
      old_name = q.name
      q.bind(exchange: ex.name)
      received = []
      q.subscribe(manual_ack: false) { |_di, _h, body| received << body }

      recover!(session, ch)

      expect(session.exchange_exists?(ex.name)).to be true
      expect(q.name).not_to eq(old_name)                 # renamed through the Queue object
      expect(q.name).to start_with("amq.gen-")
      expect(session.queue_exists?(q.name)).to be true
      expect(session.queue_exists?(old_name)).to be false
      expect(session.topology.queues.map(&:name)).to include(q.name)
      expect(session.topology.queue_bindings.map(&:queue)).to eq([q.name])

      ex.publish("after recovery")
      sleep 0.5
      expect(received).to eq(["after recovery"])         # binding + consumer follow the rename
      ch.close
    end
  end

  it "does not recover entities that were deleted or unbound before the drop" do
    isolated_session do |session, _|
      ch = session.open_channel
      ex = ch.exchange("test.topo.deleted.#{SecureRandom.hex(4)}", type: :fanout, auto_delete: true)
      kept = ch.exchange("test.topo.kept.#{SecureRandom.hex(4)}", type: :fanout, auto_delete: true)
      q  = ch.queue("", exclusive: true)
      q.bind(exchange: ex.name)
      q.bind(exchange: kept.name)
      ch.exchange_delete(ex.name)
      q.unbind(exchange: kept.name)

      # Unbinding the last queue from an auto-delete exchange makes the broker
      # delete it, so the registry forgets it too rather than resurrecting it.
      expect(session.topology.exchanges).to be_empty
      expect(session.topology.queue_bindings).to be_empty

      recover!(session, ch)
      expect(session.exchange_exists?(ex.name)).to be false
      expect(session.exchange_exists?(kept.name)).to be false
      ch.close
    end
  end

  it "does not record passive declares" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.queue("test.topo.passive.#{SecureRandom.hex(4)}", durable: true)
      expect(session.topology.queues.size).to eq(1)
      expect(session.queue_exists?(session.topology.queues.first.name)).to be true   # passive, not recorded again
      ch.exchange("amq.topic", passive: true)
      expect(session.topology.queues.size).to eq(1)
      expect(session.topology.exchanges).to be_empty
      ch.close
    end
  end

  it "applies a topology recovery filter" do
    filter = Class.new do
      def filter_exchanges(xs) = xs.reject { |x| x.name.start_with?("skip.") }
    end.new
    isolated_session(topology_recovery_filter: filter) do |session, _|
      ch   = session.open_channel
      skip = ch.exchange("skip.#{SecureRandom.hex(4)}", type: :fanout, auto_delete: true)
      keep = ch.exchange("keep.#{SecureRandom.hex(4)}", type: :fanout, auto_delete: true)
      q    = ch.queue("", exclusive: true)
      q.bind(exchange: skip.name)
      q.bind(exchange: keep.name)

      recover!(session, ch)
      expect(session.exchange_exists?(keep.name)).to be true
      expect(session.exchange_exists?(skip.name)).to be false
      ch.close
    end
  end

  it "can be disabled with recover_topology: false (consumers are still re-registered)" do
    isolated_session(recover_topology: false) do |session, _|
      ch = session.open_channel
      q  = ch.queue("", exclusive: true)
      recover!(session, ch)
      expect(session.queue_exists?(q.name)).to be false
      ch.close
    end
  end

  it "replays entities declared on a channel the user has since closed" do
    isolated_session do |session, _|
      ch1  = session.open_channel
      name = "test.topo.orphan.#{SecureRandom.hex(4)}"
      ch1.queue(name, exclusive: true)   # per-connection queue declared on ch1
      ch1.close
      ch2 = session.open_channel

      recover!(session, ch2)
      expect(session.queue_exists?(name)).to be true
      expect(session.instance_variable_get(:@channels).keys).to eq([ch2.channel_id])   # temp channel closed again
      ch2.close
    end
  end

  it "recovers an exchange-to-exchange binding, in dependency order" do
    isolated_session do |session, _|
      ch  = session.open_channel
      tag = SecureRandom.hex(4)
      src  = ch.topic("test.e2e.src.#{tag}", durable: true)
      dest = ch.fanout("test.e2e.dest.#{tag}", durable: true)
      ch.exchange_bind(destination: dest.name, source: src.name, routing_key: "audit.#")
      q = ch.queue("test.e2e.q.#{tag}", durable: true)
      q.bind(exchange: dest.name)

      # Wipe the broker-side graph so only the client's replay can rebuild it.
      # Through the management API, not the client: a client-side delete would
      # (correctly) cascade through the topology registry and forget the
      # binding, leaving nothing to replay. The broker losing state behind the
      # client's back is the case recovery exists for.
      session.with_channel { |_c| nil } # ensure declarations have settled
      http_delete("/api/exchanges/#{URI.encode_uri_component(session.vhost)}/#{URI.encode_uri_component(dest.name)}")

      recover!(session, ch)

      # The full route must be back: src -> (e2e binding) -> dest -> q. If the
      # exchange binding were replayed before both exchanges exist, or not at
      # all, this publish is silently unroutable.
      ch.basic_publish("routed through two exchanges", exchange: src.name, routing_key: "audit.login")
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      body = nil
      while body.nil? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        _di, _h, body = ch.basic_get(q.name, manual_ack: false)
        sleep 0.1 if body.nil?
      end
      expect(body).to eq("routed through two exchanges")
    end
  end

  it "republishes an unconfirmed publish to a renamed server-named queue under its NEW name" do
    # The pending entries keep pre-encoded frames with the routing key baked
    # in; queue_renamed rewrites the ones addressed to the renamed queue via
    # the default exchange before finish_recovery! replays them. Without that
    # the replay went to the old name — unroutable, silently lost under
    # confirms, in exactly the RPC-reply pattern server-named queues serve.
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.temporary_queue # server-named: recovery re-declares it under a new name
      ch.confirm_select

      # Keep the publish unconfirmed across the drop by withholding its ack.
      original = ch.method(:handle_method)
      ch.define_singleton_method(:handle_method) do |method|
        next nil if method.is_a?(AMQ::Protocol::Basic::Ack)
        original.call(method)
      end

      # reply_to names the same server-named queue, as the RPC request pattern
      # does: it must follow the rename too, or the responder answers into a
      # queue that no longer exists.
      ch.basic_publish("follow the rename", routing_key: q.name, reply_to: q.name)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      sleep 0.05 while ch.unconfirmed_messages.empty? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      expect(ch.unconfirmed_messages.size).to eq(1)

      old_name = q.name
      recover_connection!(session)
      expect(q.name).not_to eq(old_name) # the broker handed out a new name

      # The replayed publish was addressed to the OLD name via the default
      # exchange. If the replay does not follow the rename, it is silently
      # unroutable and the message is lost despite confirms.
      body = nil
      header = nil
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
      while body.nil? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        _di, header, body = ch.basic_get(q.name, manual_ack: false)
        sleep 0.1 if body.nil?
      end
      expect(body).to eq("follow the rename")
      expect(header.properties[:reply_to]).to eq(q.name) # the NEW name
    end
  end

  it "reports an incomplete topology replay end to end when a declare times out during recovery" do
    isolated_session do |session, _|
      ch  = session.open_channel
      tag = SecureRandom.hex(4)
      ch.queue("test.incomplete.a.#{tag}", durable: true)
      doomed = ch.queue("test.incomplete.b.#{tag}", durable: true)
      ch.queue("test.incomplete.c.#{tag}", durable: true)

      incomplete = nil
      succeeded  = nil
      session.on_event("recovery.topology_incomplete") { |_n, p| incomplete ||= p }
      session.on_event("recovery.succeeded")           { |_n, p| succeeded ||= p }

      # Everything is real — the drop, the channel reopen, the replay loop, the
      # event plumbing — except that one queue's re-declare stands in for a
      # broker that went slow: it raises the RpcTimeoutError such a declare
      # would. That is the one thing toxiproxy cannot target precisely (a
      # blanket latency toxic fails channel.open first), so it is synthesised
      # here while the rest of the path stays end to end.
      original = ch.method(:recover_queue)
      ch.define_singleton_method(:recover_queue) do |q|
        raise AsyncRabbitMQ::RpcTimeoutError, "synthetic slow declare" if q.name == doomed.name
        original.call(q)
      end

      recover_connection!(session)

      expect(incomplete).not_to be_nil
      expect(incomplete[:kind]).to eq(:queue)
      expect(incomplete[:entity]).to eq(doomed.name)
      expect(incomplete[:error]).to eq("AsyncRabbitMQ::RpcTimeoutError")
      # recovery still SUCCEEDS (the channels are back), but says so honestly.
      expect(succeeded).not_to be_nil
      expect(succeeded[:topology_complete]).to be(false)
      expect(session.open?).to be true
    end
  end
end
