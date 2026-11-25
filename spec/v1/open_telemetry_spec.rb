# frozen_string_literal: true

require_relative "helper"

begin
  require "opentelemetry/sdk"
  require "opentelemetry-metrics-sdk"
  require "async/rabbitmq/open_telemetry"
  V1_OTEL_LOADED = true
rescue LoadError
  V1_OTEL_LOADED = false
end

# The optional OpenTelemetry layer. It is given providers of its own here, so
# nothing global is configured except the propagator, which is put back.
RSpec.describe "Async::RabbitMQ::OpenTelemetry", :integration do
  before(:all) do
    skip "opentelemetry sdk not installed" unless V1_OTEL_LOADED

    @spans    = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
    @provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    @provider.add_span_processor(OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(@spans))
    @propagation = OpenTelemetry.propagation
    OpenTelemetry.propagation = OpenTelemetry::Trace::Propagation::TraceContext.text_map_propagator
  end

  after(:all) do
    next unless V1_OTEL_LOADED

    Async::RabbitMQ::OpenTelemetry.uninstall
    OpenTelemetry.propagation = @propagation
  end

  # Meters count for as long as they live, so each example has its own.
  before do
    @spans.reset
    @metrics = OpenTelemetry::SDK::Metrics::Export::InMemoryMetricPullExporter.new
    @meters  = OpenTelemetry::SDK::Metrics::MeterProvider.new
    @meters.add_metric_reader(@metrics)
    Async::RabbitMQ::OpenTelemetry.install(tracer_provider: @provider, meter_provider: @meters)
  end

  def with_client
    with_vhost do |vhost|
      Async::RabbitMQ::Client.open(endpoint(vhost: vhost), reconnect_interval: 0.1) { |client| yield client, vhost }
    end
  end

  def spans = @spans.finished_spans
  def span(name) = spans.find { |span| span.name == name }

  # Data points of the metric called +name+ since the example began.
  def points(name)
    @metrics.reset
    @metrics.pull
    @metrics.metric_snapshots.select { |snapshot| snapshot.name == name }.flat_map(&:data_points)
  end

  def total(name) = points(name).sum(&:value)

  describe "publishing" do
    it "makes a producer span and carries its context in the message" do
      with_client do |client, _|
        work = client.queue("work")
        client.publisher do |publisher|
          publisher.publish("hello", routing_key: "work", message_id: "m-1", correlation_id: "c-1")
        end

        sent = span("publish amq.default")
        expect(sent.kind).to eq(:producer)
        expect(sent.attributes).to include(
          "messaging.system" => "rabbitmq", "messaging.operation.name" => "publish",
          "messaging.operation.type" => "send", "messaging.destination.name" => "work",
          "messaging.rabbitmq.destination.routing_key" => "work", "messaging.message.body.size" => 5,
          "messaging.message.id" => "m-1", "messaging.message.conversation_id" => "c-1",
          "server.address" => RABBITMQ_HOST, "server.port" => RABBITMQ_PORT
        )

        carried = nil
        work.consumer do |consumer|
          consumer.each do |message|
            carried = message.headers["traceparent"]
            message.ack
            consumer.stop
          end
        end
        expect(carried).to eq("00-#{sent.hex_trace_id}-#{sent.hex_span_id}-01")
      end
    end

    it "names the exchange, and leaves the caller's headers alone" do
      with_client do |client, _|
        events = client.exchange("events", :topic)
        client.queue("all").bind(events, routing_key: "#")
        headers = { "tenant" => "acme" }.freeze
        events.publisher { |publisher| publisher.publish("x", routing_key: "order.created", headers: headers) }

        expect(headers).to eq("tenant" => "acme")
        sent = span("publish events")
        expect(sent.attributes).to include("messaging.destination.name" => "events:order.created")
      end
    end

    it "counts each answer from the broker and times it" do
      with_client do |client, _|
        client.queue("work")
        client.publisher do |publisher|
          3.times { publisher.publish("x", routing_key: "work") }
          expect { publisher.publish("x", routing_key: "nowhere", mandatory: true) }
            .to raise_error(Async::RabbitMQ::ReturnedError)
        end

        sent = points("messaging.client.sent.messages")
        ok   = sent.find { |point| !point.attributes.key?("error.type") }
        bad  = sent.find { |point| point.attributes.key?("error.type") }
        expect(ok.value).to eq(3)
        expect(ok.attributes).to eq("messaging.system" => "rabbitmq", "messaging.operation.name" => "publish",
                                    "messaging.destination.name" => "amq.default")
        expect(bad.value).to eq(1)
        expect(bad.attributes["error.type"]).to eq("Async::RabbitMQ::ReturnedError")
        expect(points("messaging.client.operation.duration").sum(&:count)).to eq(4)
      end
    end
  end

  describe "consuming" do
    it "runs the block of a Consumer in a consumer span, a child of the publish" do
      with_client do |client, _|
        work = client.queue("work")
        client.publisher { |publisher| publisher.publish("hello", routing_key: "work", message_id: "m-1") }
        inside = nil
        work.consumer do |consumer|
          consumer.each do |message|
            inside = OpenTelemetry::Trace.current_span.context.hex_span_id
            message.ack
            consumer.stop
          end
        end

        sent      = span("publish amq.default")
        processed = span("process work")
        expect(processed.kind).to eq(:consumer)
        expect(processed.hex_trace_id).to eq(sent.hex_trace_id)
        expect(processed.hex_parent_span_id).to eq(sent.hex_span_id)
        expect(inside).to eq(processed.hex_span_id)
        expect(processed.attributes).to include(
          "messaging.operation.name" => "process", "messaging.destination.name" => "work",
          "messaging.rabbitmq.destination.routing_key" => "work", "messaging.message.id" => "m-1",
          "messaging.message.body.size" => 5, "messaging.rabbitmq.message.delivery_tag" => 1
        )
        expect(total("messaging.client.consumed.messages")).to eq(1)
        expect(points("messaging.process.duration").sum(&:count)).to eq(1)
        expect(points("async.rabbitmq.message.age").sum(&:count)).to eq(1)
      end
    end

    it "gives each handler of a ManagedConsumer a span of its own, and no other" do
      with_client do |client, _|
        work = client.queue("work")
        client.publisher { |publisher| 4.times { |i| publisher.publish("m#{i}", routing_key: "work") } }
        seen     = []
        consumer = work.managed_consumer(concurrency: 2)
        running  = Async do
          consumer.run do |message|
            seen << OpenTelemetry::Trace.current_span.context.hex_span_id
            sleep 0.02
          end
        end
        wait_for { seen.size == 4 }
        consumer.close
        running.wait

        processed = spans.select { |span| span.name == "process work" }
        expect(processed.size).to eq(4)
        expect(processed.map(&:hex_span_id)).to match_array(seen)
        published = spans.select { |span| span.name == "publish amq.default" }.map(&:hex_span_id)
        expect(processed.map(&:hex_parent_span_id)).to match_array(published)
        expect(total("messaging.client.consumed.messages")).to eq(4)
      end
    end

    it "records a handler's exception on the span and in the metrics, and the consumer carries on" do
      with_client do |client, _|
        work = client.queue("work")
        client.publisher { |publisher| %w[bad good].each { |body| publisher.publish(body, routing_key: "work") } }
        seen     = []
        consumer = work.managed_consumer
        running  = Async do
          consumer.run do |message|
            seen << message.body
            raise ArgumentError, "no" if message.body == "bad"
          end
        end
        wait_for { seen.size == 2 }
        consumer.close
        running.wait

        failed, passed = spans.select { |span| span.name == "process work" }
        expect(failed.status.code).to eq(OpenTelemetry::Trace::Status::ERROR)
        expect(failed.events.map(&:name)).to include("exception")
        expect(passed.status.code).not_to eq(OpenTelemetry::Trace::Status::ERROR)
        errors = points("messaging.client.consumed.messages").select { |point| point.attributes["error.type"] }
        expect(errors.map { |point| [point.attributes["error.type"], point.value] }).to eq([["ArgumentError", 1]])
      end
    end

    it "starts a new trace for a message that carries no context" do
      with_client do |client, _|
        work = client.queue("work")
        Async::RabbitMQ::OpenTelemetry.uninstall
        client.publisher { |publisher| publisher.publish("plain", routing_key: "work") }
        Async::RabbitMQ::OpenTelemetry.install(tracer_provider: @provider, meter_provider: @meters)
        work.consumer do |consumer|
          consumer.each do |message|
            expect(message.headers).not_to have_key("traceparent")
            message.ack
            consumer.stop
          end
        end
        expect(span("publish amq.default")).to be_nil
        expect(span("process work").hex_parent_span_id).to eq("0" * 16)
      end
    end
  end

  describe "connections" do
    it "counts connections made and lost" do
      with_client do |client, _|
        sever!(client.connection)
        wait_for { total("async.rabbitmq.client.connection.losses") == 1 }
        wait_for { client.connected? }
        made = points("async.rabbitmq.client.connections")
        expect(made.sum(&:value)).to eq(2)
        expect(made.first.attributes).to eq("messaging.system" => "rabbitmq", "server.address" => RABBITMQ_HOST,
                                            "server.port" => RABBITMQ_PORT)
      end
    end
  end

  describe "switched off" do
    it "does nothing when uninstalled, or with either half left out" do
      with_client do |client, _|
        client.queue("work")
        Async::RabbitMQ::OpenTelemetry.uninstall
        expect(Async::RabbitMQ::OpenTelemetry).not_to be_installed
        client.publisher { |publisher| publisher.publish("x", routing_key: "work") }
        expect(spans).to be_empty
        expect(points("messaging.client.sent.messages")).to be_empty

        Async::RabbitMQ::OpenTelemetry.install(tracer_provider: nil, meter_provider: @meters)
        client.publisher { |publisher| publisher.publish("x", routing_key: "work") }
        expect(spans).to be_empty
        expect(total("messaging.client.sent.messages")).to eq(1)

        Async::RabbitMQ::OpenTelemetry.install(tracer_provider: @provider, meter_provider: nil)
        client.publisher { |publisher| publisher.publish("x", routing_key: "work") }
        expect(spans.map(&:name)).to eq(["publish amq.default"])
        expect(total("messaging.client.sent.messages")).to eq(1)
      ensure
        Async::RabbitMQ::OpenTelemetry.install(tracer_provider: @provider, meter_provider: @meters)
      end
    end
  end
end
