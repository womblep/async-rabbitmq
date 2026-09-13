# frozen_string_literal: true

# Optional OpenTelemetry tracing. Not loaded by default and not a dependency of
# the gem: require it yourself, with opentelemetry-api in your bundle.
#
#   require "async_rabbitmq/telemetry/open_telemetry"
#   AsyncRabbitMQ::Telemetry::OpenTelemetry.install
#
# The spans and attributes match opentelemetry-instrumentation-bunny, so a
# service moving over from Bunny keeps the traces and dashboards it had:
#
#   * publishing produces a PRODUCER span "<exchange>.<routing key> publish" and
#     injects the W3C trace context into the message headers,
#   * basic_get produces a CONSUMER span named for the queue, then renamed to
#     "<exchange>.<routing key> receive" once the delivery arrives,
#   * a consumer handler runs inside a CONSUMER span
#     "<exchange>.<routing key> process", parented by the context extracted from
#     the message headers, so a trace crosses the broker.
#
# One difference from Bunny, forced by the architecture: Bunny creates a receive
# span for a pushed delivery and parents the process span to it, linking to the
# producer. Here a pushed delivery goes straight to the handler, so the process
# span is parented to the producer context directly. Traces join up the same way,
# with one span per delivery instead of two.
#
# Publishing from several fibers on one channel is safe here, and each publish
# gets its own span: the context is captured when basic_publish is called, not
# when the frames reach the wire.

require "opentelemetry"

require_relative "../../async_rabbitmq"

module AsyncRabbitMQ
  module Telemetry
    module OpenTelemetry
      TRACER_NAME      = "async-rabbitmq"
      PROTOCOL         = "AMQP"
      PROTOCOL_VERSION = "0.9.1"
      SYSTEM           = "rabbitmq"

      class << self
        attr_reader :tracer

        # Start tracing. Safe to call more than once: the patch is applied once
        # and does nothing until a tracer is set.
        def install(tracer_provider: ::OpenTelemetry.tracer_provider,
                    tracer_name: TRACER_NAME,
                    tracer_version: AsyncRabbitMQ::VERSION)
          @tracer = tracer_provider.tracer(tracer_name, tracer_version)
          AsyncRabbitMQ::Channel.prepend(ChannelSpans) unless AsyncRabbitMQ::Channel.include?(ChannelSpans)
          true
        end

        # Stop tracing. The prepended module stays in the ancestor chain and
        # falls straight through to the unpatched methods.
        def uninstall
          @tracer = nil
          true
        end

        def installed?
          !@tracer.nil?
        end
      end

      # Attribute and name construction. Pure functions: everything they need is
      # passed in, so the publish, get and consume paths cannot drift apart.
      module Helpers
        module_function

        # Bunny joins the two with a dot and drops whichever is missing.
        def destination(exchange, routing_key)
          [exchange, routing_key].reject { |part| part.nil? || part.to_s.empty? }.join(".")
        end

        # The default exchange is direct, and a direct exchange addresses one
        # queue; everything else is a topic in OpenTelemetry's model. The type is
        # only known for exchanges this client declared.
        def destination_kind(exchange, exchange_type)
          return "queue" if exchange.nil? || exchange.to_s.empty?
          return "queue" if exchange_type.to_s == "direct"

          "topic"
        end

        def basic_attributes(exchange:, routing_key:, peer_host:, peer_port:, exchange_type: nil)
          attributes = {
            "messaging.system"           => SYSTEM,
            "messaging.destination"      => exchange.to_s,
            "messaging.destination_kind" => destination_kind(exchange, exchange_type),
            "messaging.protocol"         => PROTOCOL,
            "messaging.protocol_version" => PROTOCOL_VERSION,
            "net.peer.name"              => peer_host,
            "net.peer.port"              => peer_port
          }
          attributes["messaging.rabbitmq.routing_key"] = routing_key if routing_key && !routing_key.to_s.empty?
          attributes
        end

        def headers_of(header)
          return {} unless header.respond_to?(:properties) && header.properties

          header.properties[:headers] || {}
        end

        def message_id_of(header)
          return nil unless header.respond_to?(:properties) && header.properties

          header.properties[:message_id]
        end
      end

      # Prepended to AsyncRabbitMQ::Channel by .install.
      module ChannelSpans
        def basic_publish(payload, exchange: "", routing_key: "", **opts)
          tracer = OpenTelemetry.tracer
          return super unless tracer

          tracer.in_span("#{Helpers.destination(exchange, routing_key)} publish",
                         attributes: otel_attributes(exchange, routing_key), kind: :producer) do
            headers = (opts[:headers] ||= {})
            ::OpenTelemetry.propagation.inject(headers)
            super(payload, exchange: exchange, routing_key: routing_key, **opts)
          end
        end

        def basic_publish_batch(payloads, exchange: "", routing_key: "", **opts)
          tracer = OpenTelemetry.tracer
          return super unless tracer

          attributes = otel_attributes(exchange, routing_key)
          attributes["messaging.batch.message_count"] = payloads.is_a?(Array) ? payloads.size : 0
          tracer.in_span("#{Helpers.destination(exchange, routing_key)} publish",
                         attributes: attributes, kind: :producer) do
            headers = (opts[:headers] ||= {})
            ::OpenTelemetry.propagation.inject(headers)
            super(payloads, exchange: exchange, routing_key: routing_key, **opts)
          end
        end

        def basic_get(queue_name, manual_ack: true)
          tracer = OpenTelemetry.tracer
          return super unless tracer

          attributes = otel_attributes("", nil)
          attributes["messaging.operation"] = "receive"
          tracer.in_span("#{queue_name} receive", attributes: attributes, kind: :consumer) do |span|
            result = super
            delivery, = result
            if delivery
              span["messaging.destination"] = delivery.exchange.to_s
              span["messaging.destination_kind"] = Helpers.destination_kind(delivery.exchange,
                                                                            otel_exchange_type(delivery.exchange))
              span["messaging.rabbitmq.routing_key"] = delivery.routing_key if delivery.routing_key
              span.name = "#{Helpers.destination(delivery.exchange, delivery.routing_key)} receive"
            end
            result
          end
        end

        def basic_consume(queue_name, **opts, &block)
          tracer = OpenTelemetry.tracer
          return super unless tracer && block
          # Recovery re-registers consumers with the block it stored, which is
          # already wrapped; wrapping again would nest a span per reconnect.
          return super if block.respond_to?(:async_rabbitmq_traced?)

          traced = proc do |delivery, header, body|
            otel_process_span(tracer, delivery, header) { block.call(delivery, header, body) }
          end
          traced.define_singleton_method(:async_rabbitmq_traced?) { true }
          super(queue_name, **opts, &traced)
        end

        private

        def otel_attributes(exchange, routing_key)
          Helpers.basic_attributes(exchange: exchange, routing_key: routing_key,
                                   peer_host: @session.host, peer_port: @session.port,
                                   exchange_type: otel_exchange_type(exchange))
        end

        # Only exchanges this client declared are in the registry; a
        # pre-existing exchange has no recorded type and is treated as a topic.
        def otel_exchange_type(exchange)
          return nil if exchange.nil? || exchange.to_s.empty?

          topology&.exchanges&.find { |recorded| recorded.name == exchange }&.type
        end

        def otel_process_span(tracer, delivery, header, &block)
          attributes = otel_attributes(delivery.exchange, delivery.routing_key)
          attributes["messaging.operation"] = "process"
          if (message_id = Helpers.message_id_of(header))
            attributes["messaging.message_id"] = message_id
          end

          parent = ::OpenTelemetry.propagation.extract(Helpers.headers_of(header))
          ::OpenTelemetry::Context.with_current(parent) do
            tracer.in_span("#{Helpers.destination(delivery.exchange, delivery.routing_key)} process",
                           attributes: attributes, kind: :consumer, &block)
          end
        end
      end
    end
  end
end
