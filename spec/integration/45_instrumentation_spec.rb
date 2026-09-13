require "spec_helper"

# Structured events for metrics and tracing. The contract is: nothing is emitted
# until something subscribes, names are stable, a subscriber that raises cannot
# take the connection down, and the payloads carry enough to be useful.
RSpec.describe "instrumentation", :integration do

  # Collect every event a block produces, as [name, payload] pairs.
  def recording(session)
    seen = []
    session.on_event { |name, payload| seen << [name, payload] }
    yield
    seen
  end

  def names(seen)
    seen.map(&:first)
  end

  it "emits nothing until something subscribes" do
    isolated_session do |session, _|
      expect(session.notifier.subscribed?).to be false

      channel = session.open_channel
      queue   = channel.durable_queue("test.events.quiet.#{SecureRandom.hex(4)}")
      channel.basic_publish("no listeners", routing_key: queue.name)

      expect(session.notifier.subscriber_count).to eq(0)
    end
  end

  it "reports a publish, a delivery and the confirm in between" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.events.roundtrip.#{SecureRandom.hex(4)}")
      channel.confirm_select
      delivered = []

      seen = recording(session) do
        channel.basic_consume(queue.name, manual_ack: false) { |_d, _h, body| delivered << body }
        channel.basic_publish("hello events", routing_key: queue.name, persistent: true)
        channel.wait_for_confirms
        50.times { break unless delivered.empty?; sleep 0.1 }
        sleep 0.1 # the consumed event lands after the handler returns
      end
      expect(delivered.size).to eq(1)

      expect(names(seen)).to include("consumer.registered", "message.published", "message.confirmed", "message.consumed")

      published = seen.find { |name, _| name == "message.published" }.last
      expect(published).to include(channel: channel.channel_id, routing_key: queue.name, count: 1)
      expect(published[:delivery_tag]).to eq(1)
      expect(published[:bytes]).to be > "hello events".bytesize # frames, not just the body

      confirmed = seen.find { |name, _| name == "message.confirmed" }.last
      expect(confirmed).to include(delivery_tag: 1, acked: true)

      consumed = seen.find { |name, _| name == "message.consumed" }.last
      expect(consumed).to include(queue: queue.name, bytes: "hello events".bytesize, redelivered: false)
      expect(consumed[:duration]).to be_a(Float)
    end
  end

  it "times each synchronous call in channel.rpc" do
    isolated_session do |session, _|
      channel = session.open_channel

      seen = recording(session) do
        channel.durable_queue("test.events.rpc.#{SecureRandom.hex(4)}")
      end

      rpc = seen.select { |name, _| name == "channel.rpc" }
      expect(rpc).not_to be_empty
      declare = rpc.map(&:last).find { |p| p[:method] == "queue.declare-ok" }
      expect(declare).not_to be_nil
      expect(declare[:duration]).to be_between(0, 15)
      expect(declare[:channel]).to eq(channel.channel_id)
    end
  end

  it "counts a batch as one publish event" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.events.batch.#{SecureRandom.hex(4)}")

      seen = recording(session) do
        channel.basic_publish_batch(Array.new(25, "batched"), routing_key: queue.name)
      end

      published = seen.select { |name, _| name == "message.published" }
      expect(published.size).to eq(1)
      expect(published.first.last[:count]).to eq(25)
    end
  end

  it "reports an unroutable message and the channel the broker closed" do
    isolated_session do |session, _|
      channel = session.open_channel

      returned = recording(session) do
        channel.basic_publish("nowhere", routing_key: "no.such.queue.#{SecureRandom.hex(4)}", mandatory: true)
        sleep 0.3
      end
      expect(names(returned)).to include("message.returned")
      expect(returned.find { |name, _| name == "message.returned" }.last[:code]).to eq(312)

      closed = recording(session) do
        other = session.open_channel
        begin
          other.queue("test.events.missing.#{SecureRandom.hex(4)}", passive: true)
        rescue AsyncRabbitMQ::ChannelError
          # expected: 404 closes the channel
        end
      end
      broker_closed = closed.select { |name, _| name == "channel.closed" }.map(&:last)
      expect(broker_closed.map { |p| p[:reason] }).to include(:broker)
      expect(broker_closed.find { |p| p[:reason] == :broker }[:code]).to eq(404)
    end
  end

  it "filters by prefix, exact name and regexp" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.events.filter.#{SecureRandom.hex(4)}")

      prefixed = []
      exact    = []
      matched  = []
      session.on_event("message.")        { |name, _| prefixed << name }
      session.on_event("channel.open")    { |name, _| exact << name }
      session.on_event(/^consumer\./)     { |name, _| matched << name }

      tag = channel.basic_consume(queue.name) { |_d, _h, _b| nil }
      channel.basic_publish("filtered", routing_key: queue.name)
      channel.basic_cancel(tag)
      session.open_channel

      # The consumer above may also deliver the message, so the prefix subscriber
      # sees message.published and possibly message.consumed, but nothing else.
      expect(prefixed).to include("message.published")
      expect(prefixed).to all(start_with("message."))
      expect(exact).to eq(["channel.open"])
      expect(matched).to eq(%w[consumer.registered consumer.cancelled])
    end
  end

  it "survives a subscriber that raises, and logs it" do
    log = StringIO.new
    isolated_session(logger: Logger.new(log)) do |session, _|
      session.on_event { |_name, _payload| raise "subscriber is broken" }

      channel = session.open_channel
      queue   = channel.durable_queue("test.events.raise.#{SecureRandom.hex(4)}")
      channel.confirm_select
      channel.basic_publish("still delivered", routing_key: queue.name)
      channel.wait_for_confirms # the broker has it, so the count below is not a race

      expect(channel.queue(queue.name, passive: true).message_count).to eq(1)
      expect(log.string).to include("subscriber is broken")
    end
  end

  it "accepts an instrumenter object at construction" do
    seen = []
    collector = ->(name, payload) { seen << [name, payload] }
    isolated_session(instrumenter: collector) do |session, _|
      channel = session.open_channel
      channel.durable_queue("test.events.instrumenter.#{SecureRandom.hex(4)}")
      expect(names(seen)).to include("connection.open", "channel.open")
      expect(seen.find { |name, _| name == "connection.open" }.last[:vhost]).to eq(session.vhost)
    end
  end

  it "reports the loss and the recovery around a severed connection" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
    session.connect
    channel = session.open_channel
    channel.durable_queue("test.events.recovery.#{SecureRandom.hex(4)}")

    seen = []
    session.on_event(/^(connection|recovery)\./) { |name, payload| seen << [name, payload] }

    recover_connection!(session)

    expect(names(seen)).to include("connection.lost", "recovery.attempt", "recovery.succeeded")
    succeeded = seen.find { |name, _| name == "recovery.succeeded" }.last
    expect(succeeded[:attempts]).to be >= 1
    expect(succeeded[:channels]).to eq(1)
    expect(succeeded[:duration]).to be_a(Float)
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
