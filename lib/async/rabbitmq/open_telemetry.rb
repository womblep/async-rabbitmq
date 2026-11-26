# frozen_string_literal: true

require "opentelemetry"

require_relative "../rabbitmq"

module Async
  module RabbitMQ
    # Optional OpenTelemetry spans and metrics. Not loaded by default and not
    # a dependency of the gem: require it yourself, with opentelemetry-api in
    # your bundle, and opentelemetry-metrics-api too if you want the metrics.
    #
    #   require "async/rabbitmq/open_telemetry"
    #   Async::RabbitMQ::OpenTelemetry.install
    #
    # Call it after OpenTelemetry itself is configured. Without the require
    # the client carries no trace of this at all.
    #
    # == Spans
    #
    # [publish <exchange>] kind producer, around Publisher#submit (and so
    #                      #publish). The W3C trace context is written into
    #                      the message's headers. The span ends when the
    #                      message has been written, not when the broker
    #                      confirms it: the outcome may never be waited on.
    # [process <queue>]    kind consumer, around the block of Consumer#each
    #                      and the handler of ManagedConsumer#run, as a child
    #                      of the context read from the message's headers. A
    #                      trace therefore crosses the broker. An exception
    #                      from the handler is recorded on the span.
    #
    # Attributes follow OpenTelemetry's messaging conventions:
    # messaging.system, messaging.operation.name and .type,
    # messaging.destination.name, messaging.rabbitmq.destination.routing_key,
    # messaging.message.id, messaging.message.conversation_id (the
    # correlation id), messaging.message.body.size, server.address and
    # server.port.
    #
    # == Metrics
    #
    # [messaging.client.sent.messages]        counter. One per answer from the
    #                                         broker, with error.type when it
    #                                         was not an ack.
    # [messaging.client.operation.duration]   histogram, seconds, from
    #                                         #submit to the broker's answer.
    # [messaging.client.consumed.messages]    counter.
    # [messaging.process.duration]            histogram, seconds, of the
    #                                         block or handler.
    # [async.rabbitmq.message.age]            histogram, seconds from publish
    #                                         to delivery, for messages that
    #                                         carry a timestamp.
    # [async.rabbitmq.client.connections]     counter of connections made.
    # [async.rabbitmq.client.connection.losses] counter of connections lost.
    #
    # Metric attributes are kept few, since each distinct set is a series: the
    # exchange for a publish, the queue for a consumer, the node for a
    # connection.
    module OpenTelemetry
      NAME             = "async-rabbitmq"
      SYSTEM           = "rabbitmq"
      DEFAULT_EXCHANGE = "amq.default"

      Instruments = Struct.new(:sent, :publish_duration, :consumed, :process_duration, :age, :connections, :losses)

      class << self
        attr_reader :tracer, :instruments

        # Start tracing and measuring. Either provider may be nil to leave
        # that half off. Safe to call again, to change providers.
        def install(tracer_provider: ::OpenTelemetry.tracer_provider, meter_provider: default_meter_provider)
          @tracer      = tracer_provider&.tracer(NAME, VERSION)
          @instruments = meter_provider && instruments_of(meter_provider.meter(NAME, version: VERSION))
          patch
          true
        end

        # Stop. What was prepended stays, and falls straight through.
        def uninstall
          @tracer = @instruments = nil
          true
        end

        def installed?
          !(@tracer.nil? && @instruments.nil?)
        end

        def clock
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end

        # ---------------------------------------------------------------------
        # What the prepended modules call.
        # ---------------------------------------------------------------------

        def exchange_name(exchange)
          exchange.empty? ? DEFAULT_EXCHANGE : exchange
        end

        def publish_attributes(publisher, exchange, routing_key, body, options)
          attributes = {
            "messaging.system"           => SYSTEM,
            "messaging.operation.name"   => "publish",
            "messaging.operation.type"   => "send",
            "messaging.destination.name" => destination(exchange, routing_key)
          }
          attributes["messaging.rabbitmq.destination.routing_key"] = routing_key unless routing_key.empty?
          attributes["messaging.message.body.size"] = body.bytesize if body.is_a?(String)
          attributes["messaging.message.id"] = options[:message_id].to_s if options[:message_id]
          if options[:correlation_id]
            attributes["messaging.message.conversation_id"] = options[:correlation_id].to_s
          end
          server(attributes, publisher.client)
        end

        # Run the block as the processing of +message+ by +consumer+.
        def process(consumer, message, &block)
          tracer      = @tracer
          instruments = @instruments
          return yield unless tracer || instruments

          started = clock
          failure = nil
          begin
            if tracer
              parent = ::OpenTelemetry.propagation.extract(message.headers)
              ::OpenTelemetry::Context.with_current(parent) do
                tracer.in_span("process #{consumer.queue_name}", attributes: process_attributes(consumer, message),
                                                                 kind: :consumer, &block)
              end
            else
              yield
            end
          rescue StandardError, ScriptError => error
            failure = error
            raise
          ensure
            consumed(instruments, consumer, message, started, failure) if instruments
          end
        end

        # An Outcome got its answer: nil for an ack, the error otherwise.
        def answered(outcome, started, error)
          instruments = @instruments or return
          attributes  = {
            "messaging.system"           => SYSTEM,
            "messaging.operation.name"   => "publish",
            "messaging.destination.name" => exchange_name(outcome.exchange)
          }
          attributes["error.type"] = error.class.name if error
          instruments.sent.add(1, attributes: attributes)
          instruments.publish_duration.record(clock - started, attributes: attributes) if started
        rescue StandardError => failure
          # Called from the connection's reader: nothing may escape into it.
          Console.debug(self, "Could not record a publish", exception: failure)
        end

        def connection(client, instrument)
          instruments = @instruments or return
          instruments[instrument].add(1, attributes: server({ "messaging.system" => SYSTEM }, client))
        rescue StandardError => failure
          Console.debug(self, "Could not record a connection", exception: failure)
        end

        private

        def default_meter_provider
          ::OpenTelemetry.meter_provider if ::OpenTelemetry.respond_to?(:meter_provider)
        end

        def patch
          return if @patched

          Publisher.prepend(PublisherSpans)
          Publisher::Outcome.prepend(OutcomeMetrics)
          Consumer.prepend(ConsumerSpans)
          ManagedConsumer.prepend(ManagedConsumerSpans)
          Client.prepend(ClientMetrics)
          @patched = true
        end

        def instruments_of(meter)
          Instruments.new(
            meter.create_counter("messaging.client.sent.messages", unit: "{message}",
                                                                   description: "Messages the broker answered for"),
            meter.create_histogram("messaging.client.operation.duration", unit: "s",
                                                                          description: "Publish to broker's answer"),
            meter.create_counter("messaging.client.consumed.messages", unit: "{message}",
                                                                       description: "Messages given to a consumer"),
            meter.create_histogram("messaging.process.duration", unit: "s",
                                                                 description: "Time a consumer spent on a message"),
            meter.create_histogram("async.rabbitmq.message.age", unit: "s",
                                                                 description: "Publish to delivery"),
            meter.create_counter("async.rabbitmq.client.connections", unit: "{connection}",
                                                                      description: "Connections made"),
            meter.create_counter("async.rabbitmq.client.connection.losses", unit: "{connection}",
                                                                            description: "Connections lost")
          )
        end

        # The convention's name for where a message goes: both parts when
        # there are both, else whichever there is.
        def destination(exchange, routing_key)
          return exchange_name(exchange) if routing_key.empty?
          return routing_key if exchange.empty?

          "#{exchange}:#{routing_key}"
        end

        def server(attributes, client)
          endpoint = client.endpoint
          attributes["server.address"] = endpoint.hostname
          attributes["server.port"]    = endpoint.port
          attributes
        end

        def process_attributes(consumer, message)
          attributes = {
            "messaging.system"                        => SYSTEM,
            "messaging.operation.name"                => "process",
            "messaging.operation.type"                => "process",
            "messaging.destination.name"              => consumer.queue_name.to_s,
            "messaging.rabbitmq.message.delivery_tag" => message.delivery_tag,
            "messaging.message.body.size"             => message.body.bytesize
          }
          routing_key = message.routing_key.to_s
          attributes["messaging.rabbitmq.destination.routing_key"] = routing_key unless routing_key.empty?
          attributes["messaging.message.id"] = message.message_id.to_s if message.message_id
          if message.correlation_id
            attributes["messaging.message.conversation_id"] = message.correlation_id.to_s
          end
          server(attributes, consumer.client)
        end

        def consumed(instruments, consumer, message, started, failure)
          attributes = {
            "messaging.system"           => SYSTEM,
            "messaging.operation.name"   => "process",
            "messaging.destination.name" => consumer.queue_name.to_s
          }
          age = message.elapsed
          instruments.age.record(age, attributes: attributes) if age
          attributes = attributes.merge("error.type" => failure.class.name) if failure
          instruments.consumed.add(1, attributes: attributes)
          instruments.process_duration.record(clock - started, attributes: attributes)
        rescue StandardError => error
          Console.debug(self, "Could not record a consumed message", exception: error)
        end
      end

      # Prepended to Publisher.
      module PublisherSpans
        def submit(body, **options)
          tracer = OpenTelemetry.tracer
          return super unless tracer

          exchange    = (options[:exchange] || @exchange).to_s
          routing_key = options[:routing_key].to_s
          tracer.in_span("publish #{OpenTelemetry.exchange_name(exchange)}", kind: :producer,
                         attributes: OpenTelemetry.publish_attributes(self, exchange, routing_key, body, options)) do
            # A copy: the context is written into it, and the hash given is
            # the caller's own, perhaps frozen, perhaps shared between fibers.
            headers = options[:headers] ? options[:headers].dup : {}
            ::OpenTelemetry.propagation.inject(headers)
            options = options.merge(headers: headers) unless headers.empty?
            super(body, **options)
          end
        end
      end

      # Prepended to Publisher::Outcome.
      module OutcomeMetrics
        def initialize(...)
          super
          @otel_started = OpenTelemetry.instruments ? OpenTelemetry.clock : nil
        end

        def ack!
          pending = @status == :pending
          super
          OpenTelemetry.answered(self, @otel_started, nil) if pending
        end

        def rearm!(...)
          super
          @otel_started = OpenTelemetry.instruments ? OpenTelemetry.clock : nil
        end

        private

        def finish(status, error)
          pending = @status == :pending
          super
          OpenTelemetry.answered(self, @otel_started, error) if pending
        end
      end

      # Prepended to Consumer. A consumer that belongs to a ManagedConsumer
      # is left alone: there the handler is the processing, not the hand-over.
      module ConsumerSpans
        def each(&block)
          return super unless block && @link.equal?(self)

          consumer = self
          super() { |message| OpenTelemetry.process(consumer, message) { block.call(message) } }
        end
      end

      # Prepended to ManagedConsumer.
      module ManagedConsumerSpans
        def run(&handler)
          return super unless handler

          consumer = self
          super() { |message| OpenTelemetry.process(consumer, message) { handler.call(message) } }
        end
      end

      # Prepended to Client.
      module ClientMetrics
        private

        def establish(connection)
          super
          OpenTelemetry.connection(self, :connections)
        end

        def lost(connection)
          current = @connection.equal?(connection)
          super
          OpenTelemetry.connection(self, :losses) if current
        end
      end
    end
  end
end
