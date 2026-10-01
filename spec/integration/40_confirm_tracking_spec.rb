require "spec_helper"

# confirm_select(tracking: true): publishers get backpressure from the number
# of unconfirmed messages, and a broker nack raises MessageNacked instead of
# being reported as a false return (Bunny 3.0 parity).
RSpec.describe "publisher confirm tracking", :integration do

  it "defaults outstanding_limit to 1000 when tracking and rejects it without tracking" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect { ch.confirm_select(outstanding_limit: 10) }.to raise_error(ArgumentError, /tracking/)
      expect { ch.confirm_select(tracking: true, outstanding_limit: 0) }.to raise_error(ArgumentError, /positive/)
      ch.confirm_select(tracking: true)
      expect(ch.outstanding_limit).to eq(1000)
      expect(ch.tracking_confirms?).to be true
      ch.close
    end
  end

  it "parks a publish while outstanding_limit messages are unconfirmed and resumes on ack" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tracking.park.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select(tracking: true, outstanding_limit: 3)

      # Three tags the broker has (as far as the channel knows) not confirmed
      # yet. The real delivery-tag counter is left alone so the broker's ack
      # for the parked publish (tag 1) still matches.
      pending = ch.instance_variable_get(:@pending_confirms)
      (101..103).each { |t| pending[t] = ["unconfirmed".b, nil, nil] }

      published = false
      publisher = Async::Task.current.async do
        ch.basic_publish("waits for a slot", routing_key: q.name)
        published = true
      end
      sleep 0.3
      expect(published).to be false
      expect(publisher).to be_running

      # A multiple-ack up to 103 frees the slots.
      ch.instance_variable_get(:@queue).push([:method, AMQ::Protocol::Basic::Ack.new(103, true)])
      publisher.wait
      expect(published).to be true
      expect(ch.wait_for_confirms).to be true
      ch.close
    end
  end

  it "raises MessageNacked from wait_for_confirms when the broker rejects a message" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.confirm_select(tracking: true)
      q  = ch.queue("test.tracking.nack.#{SecureRandom.hex(4)}", durable: true,
                    arguments: { "x-max-length" => 0, "x-overflow" => "reject-publish" })
      ch.basic_publish("rejected", routing_key: q.name)

      expect { ch.wait_for_confirms }.to raise_error(AsyncRabbitMQ::MessageNacked) { |e|
        expect(e.nacked_tags).to eq([1])
        expect(e.channel_id).to eq(ch.channel_id)
      }
      # The next clean cycle is fine again.
      expect(ch.wait_for_confirms).to be true
      ch.close
    end
  end

  it "publishes many messages with a small limit and loses none" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tracking.many.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select(tracking: true, outstanding_limit: 50)
      500.times { |i| ch.basic_publish("m#{i}", routing_key: q.name) }
      expect(ch.wait_for_confirms).to be true
      expect(ch.unconfirmed_tags).to be_empty
      expect(session.with_channel { |c| c.queue(q.name, passive: true).message_count }).to eq(500)
      ch.close
    end
  end

  it "keeps tracking settings across reopen and recovery" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.confirm_select(tracking: true, outstanding_limit: 7)
      recover_connection!(session)
      expect(ch.tracking_confirms?).to be true
      expect(ch.outstanding_limit).to eq(7)
      ch.close
    end
  end
end
