# frozen_string_literal: true

require "spec_helper"

# A consumer the broker cancels is the failure that looks like nothing at all.
# The connection stays up, publishing still works, heartbeats keep going, and
# the only notice there is of it is one frame. What the client does with that
# frame is what this file is about.
#
# The queue is always deleted through the management API rather than
# queue_delete on a channel of our own, because the two are not the same thing:
# our own delete purges the topology record on the way past, which is exactly
# what must NOT be relied on. Somebody pressing Delete in the management UI is
# the case that matters.
#
# On a single node every broker-sent cancel means the queue was deleted, so the
# "queue is still there" path is reached here by an application that
# re-declares the queue for itself. The other cause - the node holding the
# queue stopping, which cancels the consumer and then answers the probe with a
# 404 that says the queue is stopped rather than gone - needs a cluster; what
# that 404 means is covered in spec/unit.
#
# Measured against a three-node RabbitMQ 4.4 cluster, and not otherwise
# obvious: a quorum queue's leader moving does NOT cancel its consumers, nor
# does the queue losing its majority, nor does its member on the consuming node
# being removed. Deletion does.
RSpec.describe "broker-cancelled consumers", :integration do
  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.05
    end
  end

  def esc(name)
    URI.encode_www_form_component(name)
  end

  def queue_on_broker?(vhost, name)
    http_get("/api/queues/#{esc(vhost)}/#{esc(name)}")["name"] == name
  end

  def consumers_on(vhost, name)
    http_get("/api/queues/#{esc(vhost)}/#{esc(name)}")["consumers"]
  end

  # What a human does in the management UI, and the only deletion that leaves
  # this client's topology registry untouched. Callers wait for the consequence
  # they are testing rather than for the queue to disappear: in one of these
  # the application puts it straight back.
  def delete_queue_in_the_portal(vhost, name)
    response = http_delete("/api/queues/#{esc(vhost)}/#{esc(name)}")
    expect(response.code.to_i).to be_between(200, 299)
  end

  def qname(prefix)
    "cancel.#{prefix}.#{SecureRandom.hex(4)}"
  end

  describe "by default" do
    it "tells the application, and does not re-subscribe" do
      isolated_session do |session, vhost|
        ch = session.open_channel
        q  = qname("plain")
        ch.durable_queue(q)
        cancelled = nil
        ch.on_cancel { |tag| cancelled = tag }
        tag = ch.basic_consume(q, manual_ack: true) { |_d, _h, _b| }

        delete_queue_in_the_portal(vhost, q)

        wait_until { cancelled == tag }
        expect(ch).to be_open
        # Nothing re-created it: the client re-declares nothing, ever.
        expect(queue_on_broker?(vhost, q)).to be(false)
      end
    end

    it "forgets the queue, so a later connection recovery does not bring it back" do
      isolated_session do |session, vhost|
        ch = session.open_channel
        q  = qname("forgotten")
        ch.durable_queue(q)
        cancelled = false
        ch.on_cancel { cancelled = true }
        ch.basic_consume(q, manual_ack: true) { |_d, _h, _b| }

        delete_queue_in_the_portal(vhost, q)
        wait_until { cancelled }
        # The callback runs before the probe that decides the queue is gone, so
        # wait for the record to go rather than for the callback.
        wait_until { session.topology.queues.none? { |rec| rec.name == q } }

        # on_recovery fires after the topology has been replayed, so by the
        # time this returns the queue would be back if the record had survived.
        recover_connection!(session)

        expect(queue_on_broker?(vhost, q)).to be(false)
      end
    end

    it "leaves the other consumers on the channel alone" do
      isolated_session do |session, vhost|
        ch      = session.open_channel
        doomed  = qname("doomed")
        kept    = qname("kept")
        ch.durable_queue(doomed)
        ch.durable_queue(kept)
        received  = []
        cancelled = false
        ch.on_cancel { cancelled = true }
        ch.basic_consume(doomed, manual_ack: true) { |_d, _h, _b| }
        ch.basic_consume(kept, manual_ack: true) { |_d, _h, body| received << body }

        delete_queue_in_the_portal(vhost, doomed)
        wait_until { cancelled }

        ch.basic_publish("still here", exchange: "", routing_key: kept)
        wait_until { received == ["still here"] }
        expect(ch).to be_open
      end
    end
  end

  # The callback used to run on the channel's dispatch fiber, which is also the
  # fiber that delivers RPC replies. These two are what that cost.
  describe "the on_cancel callback" do
    it "cannot stop the channel by raising" do
      isolated_session do |session, vhost|
        ch     = session.open_channel
        doomed = qname("raiser")
        kept   = qname("raiser.kept")
        ch.durable_queue(doomed)
        ch.durable_queue(kept)
        received = []
        ch.on_cancel { raise "the application is having a bad day" }
        ch.basic_consume(doomed, manual_ack: true) { |_d, _h, _b| }
        ch.basic_consume(kept, manual_ack: true) { |_d, _h, body| received << body }

        delete_queue_in_the_portal(vhost, doomed)

        # The channel has to still be delivering: before, an exception here
        # escaped dispatch_loop and the channel went quiet for good.
        ch.basic_publish("alive", exchange: "", routing_key: kept)
        wait_until { received == ["alive"] }
        expect(ch).to be_open
      end
    end

    it "can talk to the broker" do
      isolated_session do |session, vhost|
        ch      = session.open_channel
        doomed  = qname("rpc")
        made    = qname("rpc.made")
        ch.durable_queue(doomed)
        ch.basic_consume(doomed, manual_ack: true) { |_d, _h, _b| }
        # A declare and a consume are both RPCs, and an RPC from this callback
        # used to wait on the fiber it was blocking: fifteen seconds to the
        # rpc_timeout, then a resync.
        done = false
        ch.on_cancel do
          ch.durable_queue(made)
          ch.basic_consume(made, manual_ack: true) { |_d, _h, _b| }
          done = true
        end

        delete_queue_in_the_portal(vhost, doomed)

        wait_until(5) { done }
        expect(queue_on_broker?(vhost, made)).to be(true)
      end
    end
  end

  describe "when asked to recover cancelled consumers" do
    it "still does not re-create a queue that was deleted" do
      isolated_session(recover_cancelled_consumers: true) do |session, vhost|
        ch = session.open_channel
        q  = qname("never.recreated")
        ch.durable_queue(q)
        cancelled = false
        ch.on_cancel { cancelled = true }
        ch.basic_consume(q, manual_ack: true) { |_d, _h, _b| }

        delete_queue_in_the_portal(vhost, q)
        wait_until { cancelled }
        wait_until { session.topology.queues.none? { |rec| rec.name == q } }

        recover_connection!(session)

        expect(queue_on_broker?(vhost, q)).to be(false)
      end
    end

    # Both of these are one event on a real broker: deleting a quorum queue
    # answers the re-subscribe with a connection-level 541 INTERNAL_ERROR
    # (".. noproc"), so the connection goes down a beat after the cancel
    # arrives. Severing it from the cancel callback is that ordering on a
    # single node.
    it "does not put the queue back when the connection drops before it knows" do
      isolated_session(recover_cancelled_consumers: true) do |session, vhost|
        ch = session.open_channel
        q  = qname("no.resurrection")
        ch.durable_queue(q)
        ch.on_cancel { sever_connection!(session) }
        ch.basic_consume(q, manual_ack: true) { |_d, _h, _b| }

        delete_queue_in_the_portal(vhost, q)

        # A reconnect re-declares the whole registry, and at this point the
        # client has not established what happened to the queue. Holding it
        # back until it has is the only thing between a deleted queue and a
        # queue that comes back with a consumer attached, which nobody can then
        # delete at all.
        wait_until(20) { session.open? && session.topology.queues.none? { |rec| rec.name == q } }
        expect(queue_on_broker?(vhost, q)).to be(false)
      end
    end

    it "waits out the reconnect instead of giving the consumer up" do
      isolated_session(recover_cancelled_consumers: true) do |session, vhost|
        ch        = session.open_channel
        q         = qname("through.reconnect")
        received  = []
        failures  = []
        recovered = false
        session.on_event("consumer.recovered")       { recovered = true }
        session.on_event("consumer.recovery_failed") { |_n, payload| failures << payload[:reason] }
        ch.durable_queue(q)
        # The connection goes with the cancel, which is what deleting a quorum
        # queue does for real: the re-subscribe is answered with a
        # connection-level 541 rather than a channel error. Severing the socket
        # is synchronous, and notify_app spawns this before the task that goes
        # asking about the queue, so by the time anything asks there is no
        # connection to ask on — the ordering under test, with no race in it.
        ch.on_cancel { sever_connection!(session) }
        ch.basic_consume(q, manual_ack: true) { |_d, _h, body| received << body }

        # Deleted and put straight back, both over HTTP so neither depends on
        # the connection that is about to go. The consumer is no longer in the
        # channel's registry, so the reconnect will not re-register it: the
        # recovery loop is the only thing that will, and it used to stop at the
        # first glimpse of a session that was not open.
        delete_queue_in_the_portal(vhost, q)
        expect(http_put("/api/queues/#{esc(vhost)}/#{esc(q)}", JSON.dump(durable: true)).code.to_i)
          .to be_between(200, 299)

        wait_until(30) { recovered }
        ch.basic_publish("after the reconnect", exchange: "", routing_key: q)
        wait_until { received == ["after the reconnect"] }
        # The connection going is not a failure to re-subscribe. It has its own
        # event and its own recovery, and an application alarming on this one
        # would otherwise be told twice about one outage.
        expect(failures).to be_empty
      end
    end

    it "re-subscribes when the application puts the queue back" do
      isolated_session(recover_cancelled_consumers: true) do |session, vhost|
        ch = session.open_channel
        q  = qname("restored")
        ch.durable_queue(q)
        received = []
        # The division of labour this feature is built around: re-declaring is
        # the application's call, never the client's, and recovery picks the
        # consumer back up if the queue is there when it looks.
        #
        # The ordering is not a race, but only because the client runs this
        # callback to completion before it goes asking. It used to spawn the
        # two alongside each other and rely on Async running a new task up to
        # its first yield, which held most of the time and not all of it: the
        # declare yields at its RPC, and the reconcile could get its answer in
        # first and conclude the queue was gone.
        ch.on_cancel { ch.durable_queue(q) }
        ch.basic_consume(q, manual_ack: true) { |_d, _h, body| received << body }

        delete_queue_in_the_portal(vhost, q)

        wait_until(20) { consumers_on(vhost, q) == 1 }
        ch.basic_publish("after the cancel", exchange: "", routing_key: q)
        wait_until { received == ["after the cancel"] }
      end
    end
  end
end
