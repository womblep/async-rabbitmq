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
# On a single node every broker-sent cancel means the queue was deleted. The
# other causes - a replicated queue's leader moving, the node holding it
# failing - need a cluster, so the "queue is still there" path is reached here
# by an application that re-declares the queue for itself.
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
        # The ordering is not a race. notify_app spawns this task before the
        # task that goes looking, Async runs a new task up to its first yield
        # straight away, so this declare is on the wire before the other task
        # has even opened the channel it will ask on.
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
