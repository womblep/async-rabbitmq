# frozen_string_literal: true

module AsyncRabbitMQ
  # Structured events for metrics, tracing and debugging.
  #
  # Nothing is emitted until something subscribes. Every call site goes through
  # Instrumented#instrument, which checks for subscribers before running the
  # block that builds the payload, so an unwatched session pays one method call
  # and one array check per event and allocates nothing.
  #
  #   session.on_event { |name, payload| logger.info("#{name} #{payload}") }
  #   session.on_event("message.")     { |name, payload| statsd.increment(name) }
  #   session.on_event("channel.rpc")  { |_, p| histogram.record(p[:duration]) }
  #   session.on_event(/recovery/)     { |name, _| pager.note(name) }
  #
  # A pattern is nil for everything, a String for one event name or, ending in
  # a dot, a prefix, or a Regexp. Subscribe returns a handle for #unsubscribe.
  #
  # A subscriber that raises is logged and skipped: instrumentation must never
  # take the connection down with it.
  class Notifier
    # The events the client emits. Names are API: they do not change without a
    # major version. Payload keys are documented in README.md.
    EVENTS = %w[
      connection.open
      connection.closed
      connection.lost
      connection.blocked
      connection.unblocked
      recovery.attempt
      recovery.succeeded
      recovery.exhausted
      heartbeat.sent
      channel.open
      channel.closed
      channel.rpc
      consumer.registered
      consumer.cancelled
      message.published
      message.confirmed
      message.returned
      message.consumed
    ].freeze

    def initialize(logger: nil)
      @subscribers = []
      @logger      = logger
    end

    def subscribe(pattern = nil, &block)
      raise ArgumentError, "a subscriber block is required" unless block

      @subscribers << [pattern, block]
      block
    end

    # Remove a subscriber using the handle subscribe returned.
    def unsubscribe(handle)
      @subscribers.reject! { |(_, block)| block.equal?(handle) }
      nil
    end

    def subscribed?
      !@subscribers.empty?
    end

    def subscriber_count
      @subscribers.size
    end

    # Called by Instrumented#instrument once the payload exists.
    def publish(name, payload)
      @subscribers.each do |(pattern, block)|
        next unless match?(pattern, name)

        begin
          block.call(name, payload)
        rescue => e
          @logger&.error("Event subscriber for #{name} raised #{e.class}: #{e.message}")
        end
      end
      nil
    end

    private

    def match?(pattern, name)
      case pattern
      when nil    then true
      when String then pattern.end_with?(".") ? name.start_with?(pattern) : name == pattern
      when Regexp then pattern.match?(name)
      else             pattern === name
      end
    end
  end

  # Mixed into Session and Channel, both of which hold a @notifier.
  module Instrumented
    private

    # Emit +name+ with the payload the block returns. The block runs only when
    # something is subscribed, so building the payload costs nothing otherwise.
    def instrument(name)
      notifier = @notifier
      return unless notifier&.subscribed?

      notifier.publish(name, yield)
    end

    # For call sites that need to time the work: returns nil when nobody is
    # listening, so the caller can skip the clock reads too.
    def instrument_clock
      @notifier&.subscribed? ? Process.clock_gettime(Process::CLOCK_MONOTONIC) : nil
    end

    def instrument_elapsed(started)
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    end
  end
end
