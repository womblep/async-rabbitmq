require "spec_helper"

# Message wraps the (delivery_info, header, body) triple and settles itself at
# most once. No broker needed: the channel is a double that records what the
# message asked it to send.
RSpec.describe AsyncRabbitMQ::Message do
  let(:channel) { double("Channel", delivery_generation: 0, basic_ack: true, basic_nack: true, basic_reject: true) }
  let(:tag)     { AsyncRabbitMQ::VersionedDeliveryTag.new(7, 0) }
  let(:deliver) { AMQ::Protocol::Basic::Deliver.new("ctag-1", tag, true, "orders", "orders.created") }
  let(:header) do
    double("Header", properties: { content_type: "application/json", correlation_id: "c-1", delivery_mode: 2,
                                   headers: { "tenant" => "acme" } })
  end

  def message(**opts)
    described_class.new(channel, deliver, header, "{}", **opts)
  end

  it "exposes the delivery, the properties and the body" do
    m = message
    expect(m.body).to eq("{}")
    expect(m.delivery_tag).to eq(7)
    expect(m.exchange).to eq("orders")
    expect(m.routing_key).to eq("orders.created")
    expect(m.consumer_tag).to eq("ctag-1")
    expect(m.redelivered?).to be true
    expect(m.content_type).to eq("application/json")
    expect(m.correlation_id).to eq("c-1")
    expect(m.reply_to).to be_nil
    expect(m.persistent?).to be true
    expect(m.headers).to eq({ "tenant" => "acme" })
  end

  it "has no consumer tag when it came from basic.get, and empty headers when none were sent" do
    get_ok = AMQ::Protocol::Basic::GetOk.new(tag, false, "", "q", 3)
    m = described_class.new(channel, get_ok, double("Header", properties: {}), "body")
    expect(m.consumer_tag).to be_nil
    expect(m.redelivered?).to be false
    expect(m.headers).to eq({})
    expect(m.persistent?).to be false
  end

  it "unpacks into the triple a Bunny-style block receives" do
    info, props, body = *message
    expect([info, props, body]).to eq([deliver, header, "{}"])

    case message
    in [_, _, String => matched]
      expect(matched).to eq("{}")
    end
  end

  it "acks through its channel with its own delivery tag" do
    m = message
    expect(channel).to receive(:basic_ack).with(tag).and_return(true)
    expect(m.settled?).to be false
    expect(m.ack).to be true
    expect(m.settled?).to be true
    expect(m.settlement).to eq(:ack)
  end

  it "passes requeue through on nack and reject" do
    expect(channel).to receive(:basic_nack).with(tag, requeue: false).and_return(true)
    n = message
    expect(n.nack(requeue: false)).to be true
    expect(n.settlement).to eq(:nack)

    expect(channel).to receive(:basic_reject).with(tag, requeue: true).and_return(true)
    r = message
    expect(r.reject).to be true
    expect(r.settlement).to eq(:reject)
  end

  it "settles only once: the first call wins and later ones send nothing" do
    m = message
    expect(channel).to receive(:basic_ack).once.and_return(true)
    expect(channel).not_to receive(:basic_nack)
    expect(channel).not_to receive(:basic_reject)

    expect(m.ack).to be true
    expect(m.ack).to be false
    expect(m.nack).to be false
    expect(m.reject(requeue: false)).to be false
    expect(m.settlement).to eq(:ack)
  end

  it "never acknowledges a message the broker settled on delivery" do
    m = message(manual_ack: false)
    expect(channel).not_to receive(:basic_ack)
    expect(m.settled?).to be true
    expect(m.manual_ack?).to be false
    expect(m.ack).to be false
    expect(m.settlement).to be_nil
  end

  it "reads as stale when the channel drops the tag as belonging to an earlier connection" do
    allow(channel).to receive(:delivery_generation).and_return(1)
    allow(channel).to receive(:basic_ack).and_return(false)
    m = message
    expect(m.stale?).to be true
    expect(m.ack).to be false
    expect(m.settlement).to eq(:stale)
    expect(m.settled?).to be true
  end

  it "stays unsettled when the write raises, so the caller can try again" do
    m = message
    calls = 0
    allow(channel).to receive(:basic_ack) do
      calls += 1
      raise AsyncRabbitMQ::NotOpenError, "closed" if calls == 1

      true
    end

    expect { m.ack }.to raise_error(AsyncRabbitMQ::NotOpenError)
    expect(m.settled?).to be false
    expect(m.ack).to be true
  end

  it "can still be settled after the fiber settling it was stopped mid-write" do
    # A consumer shutting down stops its handlers and nacks what they left.
    # A handler stopped while its ack was parked sent nothing, so the message
    # must not be left looking acked.
    m = message
    gate = Async::Promise.new
    allow(channel).to receive(:basic_ack) { gate.wait }

    Sync do
      handler = Async { m.ack }
      expect(handler).to be_running
      handler.stop
      expect(m.settled?).to be false
      expect(m.nack(requeue: true)).to be true
    end
    expect(m.settlement).to eq(:nack)
  end

  it "takes the settlement before the write, so a second fiber cannot settle it too" do
    m = message
    second = nil
    allow(channel).to receive(:basic_ack) do
      # Stands in for a write that yields to another fiber mid-ack.
      second = m.nack
      true
    end
    expect(channel).not_to receive(:basic_nack)

    expect(m.ack).to be true
    expect(second).to be false
  end
end
