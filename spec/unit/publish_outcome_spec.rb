require "spec_helper"

# A PublishOutcome is one publish's answer. These examples drive a real Channel
# over a fake transport and feed it the frames a broker would send, so every
# path that resolves an outcome is covered without one: ack, nack, return,
# replay after a reconnect, an abandoned replay, and each way a channel ends.
RSpec.describe AsyncRabbitMQ::PublishOutcome do
  let(:session)  { double("Session", frame_max: 131_072, channel_closed: nil) }
  let(:frame_io) { double("FrameIO", write_frame: nil) }
  let(:channel) do
    AsyncRabbitMQ::Channel.new(1, session, frame_io, frame_max: 131_072, logger: AsyncRabbitMQ::Log.silent).tap do |ch|
      ch.instance_variable_set(:@state, :open)
      ch.instance_variable_set(:@confirms_enabled, true)
    end
  end

  def broker_ack(tag, multiple: false)
    channel.send(:handle_confirm_ack, AMQ::Protocol::Basic::Ack.new(tag, multiple))
  end

  def broker_nack(tag, multiple: false)
    channel.send(:handle_confirm_nack, AMQ::Protocol::Basic::Nack.new(tag, multiple, false))
  end

  # A basic.return for a message carrying +headers+, as the dispatch loop sees
  # it: the method, with its content already waiting on the channel's queue.
  def broker_return(headers)
    queue = Async::Queue.new
    queue.push([:content, double("Header", properties: { headers: headers }), "body"])
    channel.instance_variable_set(:@queue, queue)
    channel.send(:handle_method, AMQ::Protocol::Basic::Return.new(312, "NO_ROUTE", "", "nowhere"))
  end

  def pending
    channel.instance_variable_get(:@pending_confirms)
  end

  around { |example| Sync { example.run } }

  it "refuses to publish outside confirm mode, where nothing would ever answer" do
    channel.instance_variable_set(:@confirms_enabled, false)
    expect { channel.publish_confirmed("x", routing_key: "q") }
      .to raise_error(AsyncRabbitMQ::Error, /confirm_select/)
  end

  it "is pending until the broker acks, then waits return :acked" do
    outcome = channel.publish_confirmed("x", routing_key: "q")
    expect(outcome).to be_pending
    expect(outcome.delivery_tag).to eq(1)
    expect(outcome.routing_key).to eq("q")

    broker_ack(1)
    expect(outcome).to be_acked
    expect(outcome.wait).to eq(:acked)
    expect(outcome.wait).to eq(:acked) # a latch: asking again is fine
    expect(outcome.error).to be_nil
  end

  it "wakes a fiber already parked in wait" do
    outcome = channel.publish_confirmed("x", routing_key: "q")
    waiter  = Async { outcome.wait }
    expect(waiter).to be_running

    broker_ack(1)
    expect(waiter.wait).to eq(:acked)
  end

  it "raises MessageNacked naming its own tag when the broker nacks it" do
    outcome = channel.publish_confirmed("x", routing_key: "q")
    broker_nack(1)

    expect(outcome).to be_nacked
    expect { outcome.wait }.to raise_error(AsyncRabbitMQ::MessageNacked) { |e|
      expect(e.nacked_tags).to eq([1])
      expect(e.channel_id).to eq(1)
    }
  end

  it "gives each publish its own answer when one of several is nacked" do
    first, second, third = Array.new(3) { |i| channel.publish_confirmed("m#{i}", routing_key: "q") }
    broker_nack(2)
    broker_ack(3, multiple: true)

    expect(first.wait).to eq(:acked)
    expect { second.wait }.to raise_error(AsyncRabbitMQ::MessageNacked)
    expect(third.wait).to eq(:acked)
    expect(pending).to be_empty
  end

  it "resolves every outcome a multiple nack covers" do
    outcomes = Array.new(2) { channel.publish_confirmed("m", routing_key: "q") }
    broker_nack(2, multiple: true)
    expect(outcomes).to all(be_nacked)
  end

  it "leaves a plain basic_publish without an outcome, and confirms it as before" do
    expect(channel.basic_publish("x", routing_key: "q")).to eq(1)
    expect(pending[1][3]).to be_nil
    expect { broker_ack(1) }.not_to raise_error
    expect(pending).to be_empty
  end

  it "raises ConfirmTimeoutError when the answer does not come in time, and can be waited on again" do
    outcome = channel.publish_confirmed("x", routing_key: "q")
    expect { outcome.wait(timeout: 0.05) }.to raise_error(AsyncRabbitMQ::ConfirmTimeoutError) { |e|
      expect(e.unconfirmed_tags).to eq([1])
    }
    expect(outcome).to be_pending

    broker_ack(1)
    expect(outcome.wait(timeout: 0.05)).to eq(:acked)
  end

  it "does not swallow a timeout the caller wrapped around the wait" do
    outcome = channel.publish_confirmed("x", routing_key: "q")
    expect { Async::Task.current.with_timeout(0.05) { outcome.wait } }.to raise_error(Async::TimeoutError)
  end

  describe "mandatory publishes" do
    def sent_headers
      pending.values.last[2].options[:headers]
    end

    it "stamps a publish id header without touching the caller's headers" do
      mine = { "tenant" => "acme" }.freeze
      outcome = channel.publish_confirmed("x", routing_key: "q", mandatory: true, headers: mine)

      expect(mine).to eq({ "tenant" => "acme" })
      expect(sent_headers).to eq({ "tenant" => "acme", AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => outcome.publish_id })
    end

    it "puts the id in properties[:headers] when that is what will be sent" do
      outcome = channel.publish_confirmed("x", routing_key: "q", mandatory: true,
                                               properties: { headers: { "a" => 1 } })
      options = pending.values.last[2].options
      expect(options[:properties][:headers])
        .to eq({ "a" => 1, AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => outcome.publish_id })
      expect(options).not_to have_key(:headers)
    end

    it "adds no header to a publish that is not mandatory" do
      outcome = channel.publish_confirmed("x", routing_key: "q", headers: { "a" => 1 })
      expect(outcome.publish_id).to be_nil
      expect(sent_headers).to eq({ "a" => 1 })
    end

    it "raises MessageReturned, and the ack that follows the return does not overturn it" do
      handler_calls = 0
      channel.on_return { handler_calls += 1 }
      outcome = channel.publish_confirmed("x", routing_key: "nowhere", mandatory: true)

      broker_return(AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => outcome.publish_id)
      broker_ack(1)

      expect(outcome).to be_returned
      expect { outcome.wait }.to raise_error(AsyncRabbitMQ::MessageReturned) { |e|
        expect(e.reply_code).to eq(312)
        expect(e.reply_text).to eq("NO_ROUTE")
        expect(e.routing_key).to eq("nowhere")
        expect(e.channel_id).to eq(1)
      }
      expect(handler_calls).to eq(0) # reported to the publisher, not channel-wide
      expect(channel.instance_variable_get(:@returnable)).to be_empty
    end

    it "matches a return to the right publish among several" do
      kept, lost = Array.new(2) { channel.publish_confirmed("m", routing_key: "q", mandatory: true) }
      broker_return(AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => lost.publish_id)
      broker_ack(2, multiple: true)

      expect(kept.wait).to eq(:acked)
      expect(lost).to be_returned
    end

    it "still hands a return it does not recognise to on_return" do
      returned = []
      channel.on_return { |_method, _header, body| returned << body }
      channel.publish_confirmed("x", routing_key: "q", mandatory: true)

      broker_return("unrelated" => true)
      sleep 0.01
      expect(returned).to eq(["body"])
    end

    it "leaves alone a return naming a publish id that is not this channel's" do
      # Headers travel. A consumer that forwards a message with its headers
      # intact republishes whatever publish id the original carried; if the
      # forward is returned, it must not be taken for one of our own.
      returned = []
      channel.on_return { |_method, _header, body| returned << body }
      mine = channel.publish_confirmed("x", routing_key: "q", mandatory: true)
      other = AsyncRabbitMQ::Channel.new(2, session, frame_io, frame_max: 131_072, logger: AsyncRabbitMQ::Log.silent)
      other.instance_variable_set(:@state, :open)
      other.instance_variable_set(:@confirms_enabled, true)
      theirs = other.publish_confirmed("x", routing_key: "q", mandatory: true)
      expect(theirs.publish_id).not_to eq(mine.publish_id)

      broker_return(AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => theirs.publish_id)
      sleep 0.01
      expect(mine).to be_pending
      expect(returned).to eq(["body"])
    end

    it "reports a return that arrives while the channel is resynchronising" do
      # The ack follows the return. If the return were dropped with the rest
      # of what a resync discards, the outcome would read :acked for a message
      # the broker threw away.
      outcome = channel.publish_confirmed("x", routing_key: "nowhere", mandatory: true)
      channel.instance_variable_set(:@resync_promise, Async::Promise.new)

      broker_return(AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => outcome.publish_id)
      channel.send(:handle_method, AMQ::Protocol::Basic::Ack.new(1, false))

      expect(outcome).to be_returned
    end

    it "does not publish a returned message again when its ack is lost to a reconnect" do
      outcome = channel.publish_confirmed("x", routing_key: "nowhere", mandatory: true)
      broker_return(AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => outcome.publish_id)
      expect(pending).to be_empty

      expect(frame_io).not_to receive(:write_frame)
      channel.send(:republish_unconfirmed)
      expect(outcome).to be_returned
    end

    it "releases a wait_for_confirms that only the returned message was holding up" do
      outcome = channel.publish_confirmed("x", routing_key: "nowhere", mandatory: true)
      waiter  = Async { channel.wait_for_confirms }
      broker_return(AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER => outcome.publish_id)

      expect(waiter.wait).to be true
      broker_ack(1) # the ack that follows finds nothing, and that is fine
      expect(pending).to be_empty
    end

    it "forgets the publish id once the message is confirmed" do
      channel.publish_confirmed("x", routing_key: "q", mandatory: true)
      broker_ack(1)
      expect(channel.instance_variable_get(:@returnable)).to be_empty
    end

    it "does not leave the outcome registered when the publish itself fails" do
      allow(frame_io).to receive(:write_frame).and_raise(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "gone"))
      expect { channel.publish_confirmed("x", routing_key: "q", mandatory: true) }
        .to raise_error(AsyncRabbitMQ::ConnectionError)
      expect(channel.instance_variable_get(:@returnable)).to be_empty
      expect(pending).to be_empty
    end
  end

  describe "across a reconnect" do
    it "stays pending through the replay and follows the message to its new tag" do
      channel.basic_publish("earlier", routing_key: "q")
      broker_ack(1)
      outcome = channel.publish_confirmed("x", routing_key: "q")
      expect(outcome.delivery_tag).to eq(2)

      # The connection drops: waiters are failed, the entry is kept for replay.
      channel.mark_recovering!(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "lost"))
      expect(outcome).to be_pending

      channel.send(:republish_unconfirmed) # numbering restarts at 1
      expect(outcome).to be_pending
      expect(outcome.delivery_tag).to eq(1)

      broker_ack(1)
      expect(outcome.wait).to eq(:acked)
    end

    it "keeps what a replay could not send, so the next one sends it and a teardown can fail it" do
      outcomes = Array.new(4) { |i| channel.publish_confirmed("m#{i}", routing_key: "q") }
      writes = 0
      allow(frame_io).to receive(:write_frame) do
        raise AsyncRabbitMQ::ConnectionError.new(code: 0, text: "gone again") if (writes += 1) > 1
      end

      expect { channel.send(:republish_unconfirmed) }.to raise_error(AsyncRabbitMQ::ConnectionError)
      expect(pending.size).to eq(4)
      expect(pending.values.map { |entry| entry[1] }).to eq(%w[m0 m1 m2 m3])
      expect(outcomes).to all(be_pending)

      allow(frame_io).to receive(:write_frame).and_return(nil)
      channel.send(:republish_unconfirmed)
      expect(outcomes.map(&:delivery_tag)).to eq([1, 2, 3, 4])
      broker_ack(4, multiple: true)
      expect(outcomes.map(&:wait)).to all(eq(:acked))
    end

    it "fails the outcomes of a cut-short replay when the connection is then given up" do
      outcomes = Array.new(3) { |i| channel.publish_confirmed("m#{i}", routing_key: "q") }
      allow(frame_io).to receive(:write_frame).and_raise(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "gone"))
      expect { channel.send(:republish_unconfirmed) }.to raise_error(AsyncRabbitMQ::ConnectionError)

      channel.mark_closed!(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "recovery exhausted"))
      expect(outcomes).to all(be_failed)
    end

    it "is nacked when the replay is abandoned" do
      outcome = channel.publish_confirmed("x", routing_key: "q", mandatory: true)
      channel.send(:abandon_unconfirmed!)

      expect { outcome.wait }.to raise_error(AsyncRabbitMQ::MessageNacked)
      expect(channel.instance_variable_get(:@returnable)).to be_empty
    end
  end

  describe "when the channel ends" do
    it "fails with the teardown error when the connection is gone for good" do
      outcome = channel.publish_confirmed("x", routing_key: "q", mandatory: true)
      error   = AsyncRabbitMQ::ConnectionError.new(code: 0, text: "recovery exhausted")
      channel.mark_closed!(error)

      expect(outcome).to be_failed
      expect { outcome.wait }.to raise_error(error)
      expect(channel.instance_variable_get(:@returnable)).to be_empty
    end

    it "fails with the broker's channel error, and keeps the entry for a reopen to replay" do
      outcome = channel.publish_confirmed("x", routing_key: "q")
      channel.send(:handle_channel_close, AMQ::Protocol::Channel::Close.new(404, "NOT_FOUND - no exchange", 60, 40))

      expect { outcome.wait }.to raise_error(AsyncRabbitMQ::ChannelError) { |e| expect(e.code).to eq(404) }
      expect(pending.size).to eq(1)
      expect(pending[1][3]).to be_nil
    end

    it "fails an outcome whose publish was still parked in the write when the channel ended" do
      # The write parks on a full write queue or behind connection.blocked.
      # The teardown runs meanwhile and cannot see an outcome that has no
      # confirm tag yet; left alone it would never be answered.
      gate = Async::Promise.new
      allow(frame_io).to receive(:write_frame) { |*, **opts| gate.wait if opts[:publish] }
      publisher = Async { channel.publish_confirmed("x", routing_key: "q", mandatory: true) }
      expect(publisher).to be_running

      channel.send(:handle_channel_close, AMQ::Protocol::Channel::Close.new(404, "NOT_FOUND", 60, 40))
      gate.resolve(true)
      outcome = publisher.wait

      expect(outcome).to be_failed
      expect { outcome.wait }.to raise_error(AsyncRabbitMQ::ChannelError) { |e| expect(e.code).to eq(404) }
      expect(channel.instance_variable_get(:@returnable)).to be_empty
    end

    it "is not resolved a second time by the replay after a reopen" do
      outcome = channel.publish_confirmed("x", routing_key: "q")
      channel.send(:handle_channel_close, AMQ::Protocol::Channel::Close.new(404, "NOT_FOUND", 60, 40))
      channel.send(:republish_unconfirmed)
      broker_ack(1)

      expect(outcome).to be_failed
    end
  end
end
