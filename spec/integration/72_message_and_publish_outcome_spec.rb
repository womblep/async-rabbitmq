require "spec_helper"

# The two building blocks of the native API, against a real broker: a Message
# that settles itself, and a PublishOutcome that answers for one publish. The
# unit specs cover the state machines; these cover what only a broker can
# show - that a double settle really is harmless, that a return really does
# arrive before its ack, and that the correlation header survives the trip.
RSpec.describe "Message and PublishOutcome", :integration do

  def fetch(channel, queue, manual_ack: true)
    triple = nil
    20.times do
      triple = channel.basic_get(queue.name, manual_ack: manual_ack)
      break if triple

      sleep 0.05
    end
    expect(triple).not_to be_nil
    AsyncRabbitMQ::Message.new(channel, *triple, manual_ack: manual_ack)
  end

  describe AsyncRabbitMQ::Message do
    it "acks itself, and a second settle does not close the channel" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.durable_queue("test.message.ack.#{SecureRandom.hex(4)}")
        ch.basic_publish("hello", routing_key: q.name, headers: { "tenant" => "acme" }, correlation_id: "c-1")

        message = fetch(ch, q)
        expect(message.body).to eq("hello")
        expect(message.headers).to eq({ "tenant" => "acme" })
        expect(message.correlation_id).to eq("c-1")
        expect(message.routing_key).to eq(q.name)

        expect(message.ack).to be true
        # Sent to the broker, each of these would be a 406 that closes the channel.
        expect(message.ack).to be false
        expect(message.nack).to be false
        sleep 0.2
        expect(ch).to be_open
        expect(q.status[:message_count]).to eq(0)
        ch.close
      end
    end

    it "puts the message back with nack, and drops it with reject(requeue: false)" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.durable_queue("test.message.nack.#{SecureRandom.hex(4)}")
        ch.basic_publish("again", routing_key: q.name)

        first = fetch(ch, q)
        expect(first.redelivered?).to be false
        expect(first.nack(requeue: true)).to be true

        second = fetch(ch, q)
        expect(second.redelivered?).to be true
        expect(second.reject(requeue: false)).to be true
        sleep 0.2
        expect(q.status[:message_count]).to eq(0)
        ch.close
      end
    end

    it "settles from inside a consumer block" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.durable_queue("test.message.consume.#{SecureRandom.hex(4)}")
        ch.basic_qos(prefetch_count: 1)
        seen = []
        q.subscribe(manual_ack: true) do |delivery_info, header, body|
          message = AsyncRabbitMQ::Message.new(ch, delivery_info, header, body)
          seen << [message.body, message.consumer_tag, message.ack]
        end
        3.times { |i| ch.basic_publish("m#{i}", routing_key: q.name) }

        40.times { break if seen.size == 3; sleep 0.05 }
        # Prefetch is 1, so all three arriving proves each ack reached the broker.
        expect(seen.map(&:first)).to eq(%w[m0 m1 m2])
        expect(seen.map(&:last)).to all(be true)
        expect(seen.map { |s| s[1] }.uniq.size).to eq(1)
        ch.close
      end
    end

    it "does not ack a message fetched with automatic acknowledgement" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.durable_queue("test.message.auto.#{SecureRandom.hex(4)}")
        ch.basic_publish("auto", routing_key: q.name)

        message = fetch(ch, q, manual_ack: false)
        expect(message.settled?).to be true
        expect(message.ack).to be false
        sleep 0.2
        expect(ch).to be_open
        ch.close
      end
    end

    it "goes stale across a reconnect instead of acking someone else's message" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.durable_queue("test.message.stale.#{SecureRandom.hex(4)}")
        ch.basic_publish("survivor", routing_key: q.name)
        message = fetch(ch, q)

        recover_connection!(session)

        expect(message.stale?).to be true
        expect(message.ack).to be false
        expect(message.settlement).to eq(:stale)
        expect(ch).to be_open
        # The broker requeued it when the old connection went.
        redelivered = fetch(ch, q)
        expect(redelivered.body).to eq("survivor")
        expect(redelivered.redelivered?).to be true
        expect(redelivered.ack).to be true
        ch.close
      end
    end
  end

  describe AsyncRabbitMQ::PublishOutcome do
    it "is acked for a message the broker takes" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.durable_queue("test.outcome.ack.#{SecureRandom.hex(4)}")
        ch.confirm_select

        outcome = q.publish_confirmed("hello")
        expect(outcome.wait(timeout: 5)).to eq(:acked)
        expect(outcome).to be_acked
        expect(ch.unconfirmed_tags).to be_empty
        expect(q.status[:message_count]).to eq(1)
        ch.close
      end
    end

    it "raises MessageNacked when the broker refuses the message" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select
        q = ch.queue("test.outcome.nack.#{SecureRandom.hex(4)}", durable: true,
                     arguments: { "x-max-length" => 0, "x-overflow" => "reject-publish" })

        outcome = ch.publish_confirmed("rejected", routing_key: q.name)
        expect { outcome.wait(timeout: 5) }.to raise_error(AsyncRabbitMQ::MessageNacked) { |e|
          expect(e.nacked_tags).to eq([outcome.delivery_tag])
        }
        ch.close
      end
    end

    it "raises MessageReturned for an unroutable mandatory message, and keeps on_return out of it" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select
        returned = []
        ch.on_return { |*args| returned << args }
        key = "no.such.queue.#{SecureRandom.hex(8)}"

        outcome = ch.publish_confirmed("lost", routing_key: key, mandatory: true)
        expect { outcome.wait(timeout: 5) }.to raise_error(AsyncRabbitMQ::MessageReturned) { |e|
          expect(e.reply_code).to eq(312)
          expect(e.routing_key).to eq(key)
          expect(e.exchange).to eq("")
        }
        expect(outcome).to be_returned
        sleep 0.2
        expect(returned).to be_empty
        expect(ch.unconfirmed_tags).to be_empty # the ack that follows the return was consumed
        ch.close
      end
    end

    it "publishes through an exchange, and is acked when the mandatory message is routed" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select
        x = ch.direct("test.outcome.x.#{SecureRandom.hex(4)}", durable: true)
        q = ch.durable_queue("test.outcome.routed.#{SecureRandom.hex(4)}").bind(exchange: x.name, routing_key: "k")

        outcome = x.publish_confirmed("routed", routing_key: "k", mandatory: true, headers: { "tenant" => "acme" })
        expect(outcome.wait(timeout: 5)).to eq(:acked)

        message = fetch(ch, q)
        expect(message.headers["tenant"]).to eq("acme")
        expect(message.headers[AsyncRabbitMQ::Channel::PUBLISH_ID_HEADER]).to eq(outcome.publish_id)
        message.ack
        ch.close
      end
    end

    it "tells concurrent publishers on one channel apart" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select
        q = ch.durable_queue("test.outcome.mixed.#{SecureRandom.hex(4)}")

        results = Array.new(20) do |i|
          Async do
            routed = i.even?
            outcome = ch.publish_confirmed("m#{i}", routing_key: routed ? q.name : "nowhere.#{SecureRandom.hex(6)}",
                                                    mandatory: true)
            begin
              [routed, outcome.wait(timeout: 10)]
            rescue AsyncRabbitMQ::MessageReturned
              [routed, :returned]
            end
          end
        end.map(&:wait)

        expect(results.select(&:first).map(&:last)).to all(eq(:acked))
        expect(results.reject(&:first).map(&:last)).to all(eq(:returned))
        expect(q.status[:message_count]).to eq(10)
        ch.close
      end
    end

    it "leaves wait_for_confirms working alongside it" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select
        q = ch.durable_queue("test.outcome.both.#{SecureRandom.hex(4)}")

        ch.basic_publish("plain", routing_key: q.name)
        outcome = ch.publish_confirmed("tracked", routing_key: q.name)
        expect(ch.wait_for_confirms(timeout: 5)).to be true
        expect(outcome.wait(timeout: 5)).to eq(:acked)
        ch.close
      end
    end

    it "is acked on the new connection when it was published into an outage" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select
        q = ch.durable_queue("test.outcome.recover.#{SecureRandom.hex(4)}")

        recovered = false
        session.on_recovery { |_s| recovered = true }
        sever_connection!(session)
        20.times { break if ch.recovering?; sleep 0.05 }
        expect(ch).to be_recovering

        # Parks until the channel is back, then publishes on the new connection.
        outcome = ch.publish_confirmed("through the outage", routing_key: q.name)
        expect(recovered).to be true
        expect(outcome.wait(timeout: 10)).to eq(:acked)
        expect(q.status[:message_count]).to eq(1)
        ch.close
      end
    end

    it "fails pending outcomes when the broker closes the channel" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.confirm_select

        # A publish to an exchange that does not exist is a 404 that closes the
        # channel; the broker never confirms the message.
        outcome = ch.publish_confirmed("nowhere", exchange: "no.such.exchange.#{SecureRandom.hex(6)}", routing_key: "k")
        expect { outcome.wait(timeout: 5) }.to raise_error(AsyncRabbitMQ::ChannelError) { |e|
          expect(e.code).to eq(404)
        }
        expect(outcome).to be_failed
      end
    end
  end
end
