require "spec_helper"

# Spec 18: Coverage completion — exercises code paths not reached by other specs.
RSpec.describe "Coverage completion", :integration do

  # -------------------------------------------------------------------------
  # Session pool interface
  # -------------------------------------------------------------------------

  describe "Session pool interface" do
    it "reusable?, viable?, and concurrency return sensible values" do
      isolated_session do |session, _|
        expect(session.reusable?).to be true
        expect(session.viable?).to be true
        expect(session.concurrency).to be_a(Integer)
        expect(session.concurrency).to be > 0
      end
    end
  end

  # -------------------------------------------------------------------------
  # Queue convenience API
  # -------------------------------------------------------------------------

  describe "Queue convenience API" do
    it "Queue#publish delivers a message via the queue object" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.queue-publish.#{SecureRandom.hex(4)}", durable: true)
        q.publish("hello from queue api")
        _di, _hdr, body = ch.basic_get(q.name)
        expect(body).to eq("hello from queue api".b)
        ch.close
      end
    end

    it "Queue#purge empties the queue" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.queue-purge.#{SecureRandom.hex(4)}", durable: true)
        ch.basic_publish("msg1", routing_key: q.name)
        ch.basic_publish("msg2", routing_key: q.name)
        q.purge
        expect(ch.basic_get(q.name)).to be_nil
        ch.close
      end
    end
  end

  # -------------------------------------------------------------------------
  # Exchange convenience API
  # -------------------------------------------------------------------------

  describe "Exchange convenience API" do
    it "Exchange#unbind removes the binding and returns self" do
      isolated_session do |session, _|
        ch  = session.open_channel
        src = ch.exchange("test.unbind-src.#{SecureRandom.hex(4)}", type: :fanout)
        dst = ch.exchange("test.unbind-dst.#{SecureRandom.hex(4)}", type: :fanout)

        src.bind(destination: dst.name)
        result = src.unbind(destination: dst.name)

        expect(result).to eq(src)   # returns self
        ch.exchange_delete(src.name)
        ch.exchange_delete(dst.name)
        ch.close
      end
    end
  end

  # -------------------------------------------------------------------------
  # Channel#exchange_unbind
  # -------------------------------------------------------------------------

  describe "Channel#exchange_unbind" do
    it "unbinds two exchanges without error" do
      isolated_session do |session, _|
        ch  = session.open_channel
        src = "test.ch-unbind-src.#{SecureRandom.hex(4)}"
        dst = "test.ch-unbind-dst.#{SecureRandom.hex(4)}"
        ch.exchange(src, type: :fanout)
        ch.exchange(dst, type: :fanout)
        ch.exchange_bind(destination: dst, source: src)
        expect { ch.exchange_unbind(destination: dst, source: src) }.not_to raise_error
        ch.exchange_delete(src)
        ch.exchange_delete(dst)
        ch.close
      end
    end
  end

  # -------------------------------------------------------------------------
  # Channel#each — duck-typed stream interface
  # -------------------------------------------------------------------------

  describe "Channel#each (stream interface)" do
    it "yields deliveries and cancels cleanly when the channel is closed" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.each.#{SecureRandom.hex(4)}", durable: true)
        q.publish("hello from each")

        received = []
        each_task = Async::Task.current.async do
          ch.each(q.name) do |_delivery, _headers, body|
            received << body
          end
        end

        sleep 0.4   # let the delivery arrive

        # Close the channel first so that the each_task's ensure block
        # hits assert_open! → NotOpenError (rescued with `rescue nil`), preventing
        # basic_cancel from blocking indefinitely on CancelOk.
        ch.close

        each_task.stop rescue nil   # fast: ensure block raises NotOpenError (rescued)

        expect(received).not_to be_empty
      end
    end
  end

  # -------------------------------------------------------------------------
  # reopen_after_recovery — channel reopened after connection is recovered
  # -------------------------------------------------------------------------

  describe "reopen_after_recovery" do
    it "channels are automatically reopened after connection recovery" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.queue("test.recovery-reopen.#{SecureRandom.hex(4)}", durable: true)

        # Force the underlying TCP connection closed
        session.instance_variable_get(:@frame_io)
               .instance_variable_get(:@socket)
               .close rescue nil

        # Recovery: ~1s initial sleep + reconnect + channel reopen
        sleep 4

        expect(session.open?).to be true
        # Channel should be usable after reopen_after_recovery
        expect { ch.basic_publish("after recovery", routing_key: "nowhere") }.not_to raise_error
        session.close rescue nil
      end
    end

    it "channels with publisher confirms re-activate confirms after recovery" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.queue("test.recovery-confirms.#{SecureRandom.hex(4)}", durable: true)
        ch.confirm_select   # enables confirms — reopen_after_recovery must re-select

        session.instance_variable_get(:@frame_io)
               .instance_variable_get(:@socket)
               .close rescue nil

        sleep 4

        expect(session.open?).to be true
        session.close rescue nil
      end
    end
  end

  # -------------------------------------------------------------------------
  # handle_confirm_nack — broker sends Basic::Nack for a confirmed publish
  # -------------------------------------------------------------------------

  describe "Publisher confirms: basic.nack" do
    it "handles Basic::Nack from broker gracefully (queue max-length overflow)" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select

        # x-max-length 0 + reject-publish: the broker immediately nacks every
        # confirmed publish because the queue can hold 0 messages.
        q = ch.queue(
          "test.nack-overflow.#{SecureRandom.hex(4)}",
          durable:   true,
          arguments: { "x-max-length" => 0, "x-overflow" => "reject-publish" }
        )
        ch.basic_publish("will be nacked", routing_key: q.name)

        # wait_for_confirms returns true once all delivery tags are resolved
        # (nack removes the tag just like ack does).
        result = ch.wait_for_confirms
        expect(result).to be true
        ch.close
      end
    end
  end

  # -------------------------------------------------------------------------
  # trigger_recovery while @recovery_in_progress — double-fault handling
  # -------------------------------------------------------------------------

  describe "trigger_recovery during active recovery" do
    it "early-returns and interrupts waiters when called while already recovering" do
      isolated_session do |session, _|
        # Simulate being mid-recovery (no channels open — avoids dispatch-loop teardown)
        session.instance_variable_set(:@recovery_in_progress, true)

        # Should take the early-return branch without raising or starting a new recovery
        expect {
          session.trigger_recovery(RuntimeError.new("double-fault simulation"))
        }.not_to raise_error

        expect(session.instance_variable_get(:@recovery_in_progress)).to be true

        # Restore clean state so isolated_session can close the session properly
        session.instance_variable_set(:@recovery_in_progress, false)
      end
    end
  end
end
