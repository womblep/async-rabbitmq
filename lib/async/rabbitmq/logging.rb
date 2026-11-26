# frozen_string_literal: true

require "console"

module Async
  module RabbitMQ
    class << self
      # The least severe level any Client logs at, whatever it logs to:
      # :debug, :info, :warn (the default), :error, :fatal, or :off for
      # nothing at all. It is applied before the logger's own level, so an
      # application that runs its logger at info is not sent this client's
      # info and debug lines unless it asks for them. It can be changed at
      # any time, and ASYNC_RABBITMQ_LOG_LEVEL sets it from the environment.
      # To silence one client, give it logger: nil.
      #
      #   Async::RabbitMQ.log_level = :debug   # troubleshooting a connection
      #   Async::RabbitMQ.log_level = :off     # nothing at all
      attr_reader :log_level

      def log_level=(level)
        level = level.to_sym
        unless %i[debug info warn error fatal off].include?(level)
          raise ArgumentError, "log_level must be :debug, :info, :warn, :error, :fatal or :off (got #{level.inspect})"
        end

        @log_level = level
      end
    end
    self.log_level = ENV.fetch("ASYNC_RABBITMQ_LOG_LEVEL", "warn")

    # Where a Client logs. Every log call in this client has Console's shape,
    # the object doing the logging and then the message, with anything else
    # as keywords:
    #
    #   logger.warn(self, "Connection lost; reconnecting", endpoint: ..., exception: error)
    #
    # and +logger+ is what Logging.resolve made of the +logger:+ the client
    # was given:
    #
    # [Console (the default)] forwarded to Console as it is, so Console's
    #   levels, its per-class filtering and its fiber-local Console.logger
    #   work as for the rest of the async stack. A Console::Logger instance
    #   is forwarded to in the same way.
    # [A Logger-compatible object] (stdlib Logger, Rails.logger, anything
    #   with debug, info, warn and error taking a message) is given one
    #   string: the message, then the keywords, then the exception. The
    #   subject is dropped.
    # [nil] everything is discarded.
    #
    # Before any of that, Async::RabbitMQ.log_level decides whether the call
    # is made at all; then the destination's own level applies as usual.
    module Logging
      LEVELS   = %i[debug info warn error fatal].freeze
      SEVERITY = LEVELS.each_with_index.to_h.merge(off: LEVELS.size).freeze

      # Whether +level+ passes Async::RabbitMQ.log_level. Read on every call,
      # so a change takes effect at once, for every client.
      def self.enabled?(level)
        SEVERITY.fetch(level) >= SEVERITY.fetch(RabbitMQ.log_level)
      end

      # Forwards to Console, or to one Console::Logger.
      class Forward
        # +target+ nil means the Console module, looked up on every call, so
        # that a Console.logger set for one task is the one its fibers use.
        def initialize(target = nil)
          @target = target
        end

        LEVELS.each do |level|
          define_method(level) do |subject, *arguments, **fields, &block|
            (@target || ::Console).public_send(level, subject, *arguments, **fields, &block) if Logging.enabled?(level)
            nil
          end
        end
      end

      # Gives a Logger-compatible object each call as one line of text.
      class Shim
        attr :logger

        def initialize(logger)
          @logger = logger
        end

        LEVELS.each do |level|
          define_method(level) do |_subject, message = nil, *_rest, exception: nil, **fields, &block|
            next nil unless Logging.enabled?(level)

            # Only debug, info, warn and error are asked of the logger.
            to = level == :fatal && !@logger.respond_to?(:fatal) ? :error : level
            @logger.public_send(to, Shim.line(message || block&.call, fields, exception))
            nil
          end
        end

        def self.line(message, fields, exception)
          line = message.to_s.dup
          line << " (" << fields.map { |key, value| "#{key}=#{value}" }.join(", ") << ")" unless fields.empty?
          line << ": #{exception.class}: #{exception.message}" if exception
          line
        end
      end

      # Discards everything: what +logger: nil+ means.
      class Null
        LEVELS.each { |level| define_method(level) { |*, **, &| nil } }
      end

      NULL = Null.new.freeze

      # Turn a +logger:+ argument into something that takes Console's call.
      # Doing it twice does no harm.
      def self.resolve(logger)
        case logger
        when nil                  then NULL
        when Forward, Shim, Null  then logger
        when ::Console::Filter    then Forward.new(logger)
        else
          if logger.equal?(::Console)
            Forward.new
          elsif LEVELS.take(4).all? { |level| logger.respond_to?(level) }
            Shim.new(logger)
          else
            raise ArgumentError, "logger: must respond to debug, info, warn and error " \
                                 "(or be Console, or nil to discard): got #{logger.inspect}"
          end
        end
      end
    end
  end
end
