require "spec_helper"

begin
  require "opentelemetry/sdk"
  require "async_rabbitmq/telemetry/open_telemetry"
  OTEL_LOADED = true
rescue LoadError
  OTEL_LOADED = false
end

# The optional OpenTelemetry layer: producer and consumer spans with the same
# names and attributes as opentelemetry-instrumentation-bunny, and W3C context
# carried through the broker in the message headers.
RSpec.describe "OpenTelemetry tracing", :integration do
  before(:all) do
    skip "opentelemetry-sdk not installed" unless OTEL_LOADED

    @exporter = ::OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
    ::OpenTelemetry::SDK.configure do |config|
      config.add_span_processor(::OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(@exporter))
    end
    AsyncRabbitMQ::Telemetry::OpenTelemetry.install
  end

  after(:all) do
    AsyncRabbitMQ::Telemetry::OpenTelemetry.uninstall if OTEL_LOADED
  end

  before { @exporter.reset }

  def spans
    @exporter.finished_spans
  end

  def span_named(suffix)
    spans.find { |span| span.name.end_with?(suffix) }
  end

  it "traces a publish as a producer span with the Bunny attribute set" do
    isolated_session do |session, _|
      channel  = session.open_channel
      exchange = "test.otel.x.#{SecureRandom.hex(4)}"
      channel.exchange(exchange, type: :topic, durable: true)
      channel.basic_publish("traced", exchange: exchange, routing_key: "rk.one")

      publish = span_named("publish")
      expect(publish).not_to be_nil
      expect(publish.name).to eq("#{exchange}.rk.one publish")
      expect(publish.kind).to eq(:producer)
      expect(publish.attributes).to include(
        "messaging.system"           => "rabbitmq",
        "messaging.destination"      => exchange,
        "messaging.destination_kind" => "topic",
        "messaging.protocol"         => "AMQP",
        "messaging.protocol_version" => "0.9.1",
        "messaging.rabbitmq.routing_key" => "rk.one",
        "net.peer.name"              => RABBITMQ_HOST,
        "net.peer.port"              => RABBITMQ_PORT
      )
    end
  end

  it "calls the default exchange a queue, as Bunny does" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.otel.default.#{SecureRandom.hex(4)}")
      channel.basic_publish("direct", routing_key: queue.name)

      publish = span_named("publish")
      expect(publish.name).to eq("#{queue.name} publish")
      expect(publish.attributes["messaging.destination"]).to eq("")
      expect(publish.attributes["messaging.destination_kind"]).to eq("queue")
    end
  end

  it "carries the trace through the broker into the consumer handler" do
    isolated_session do |session, _|
      channel  = session.open_channel
      queue    = channel.durable_queue("test.otel.consume.#{SecureRandom.hex(4)}")
      received = []
      channel.basic_consume(queue.name, manual_ack: false) do |_delivery, header, body|
        received << [header, body]
      end

      channel.basic_publish("cross the broker", routing_key: queue.name)
      50.times { break unless received.empty?; sleep 0.1 }
      sleep 0.2 # let the process span finish and export

      expect(received.size).to eq(1)
      header, = received.first
      expect(header.properties[:headers]).to have_key("traceparent")

      publish = span_named("publish")
      process = span_named("process")
      expect(process).not_to be_nil
      expect(process.kind).to eq(:consumer)
      expect(process.attributes["messaging.operation"]).to eq("process")
      # Same trace, and the handler's span hangs off the publish.
      expect(process.hex_trace_id).to eq(publish.hex_trace_id)
      expect(process.hex_parent_span_id).to eq(publish.hex_span_id)
    end
  end

  it "traces basic_get as a receive span named for the delivery" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.otel.get.#{SecureRandom.hex(4)}")
      channel.confirm_select
      channel.basic_publish("fetched", routing_key: queue.name)
      channel.wait_for_confirms

      _delivery, _header, body = channel.basic_get(queue.name, manual_ack: false)
      expect(body).to eq("fetched")

      receive = span_named("receive")
      expect(receive).not_to be_nil
      expect(receive.kind).to eq(:consumer)
      expect(receive.name).to eq("#{queue.name} receive")
      expect(receive.attributes["messaging.operation"]).to eq("receive")
    end
  end

  it "counts the messages in a batch publish" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.otel.batch.#{SecureRandom.hex(4)}")
      channel.basic_publish_batch(Array.new(7, "batched"), routing_key: queue.name)

      publish = span_named("publish")
      expect(publish.attributes["messaging.batch.message_count"]).to eq(7)
    end
  end

  it "marks the wrapped handler so recovery does not nest a span per reconnect" do
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.otel.rewrap.#{SecureRandom.hex(4)}")
      channel.basic_consume(queue.name) { |_d, _h, _b| nil }

      stored = channel.instance_variable_get(:@consumers).values.first[:block]
      expect(stored).to respond_to(:async_rabbitmq_traced?)
    end
  end

  it "stops tracing after uninstall and starts again after install" do
    AsyncRabbitMQ::Telemetry::OpenTelemetry.uninstall
    isolated_session do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.otel.off.#{SecureRandom.hex(4)}")
      channel.basic_publish("untraced", routing_key: queue.name)
      expect(spans).to be_empty

      AsyncRabbitMQ::Telemetry::OpenTelemetry.install
      channel.basic_publish("traced again", routing_key: queue.name)
      expect(span_named("publish")).not_to be_nil
    end
  end
end
