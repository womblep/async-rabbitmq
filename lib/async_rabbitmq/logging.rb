# frozen_string_literal: true

require "console"

module AsyncRabbitMQ
  # Every log call in this gem has Console's shape: the object doing the
  # logging, then the message —
  #
  #   @logger.warn(self, "Connection lost ...")
  #
  # — and +@logger+ is whatever Logging.resolve made of the +logger:+ the
  # caller passed:
  #
  # [Console (the default)] passed straight through. The subject is the
  #   session, channel, frame layer or cluster that logged, so Console's
  #   per-class filtering, its CONSOLE_LEVEL control and its fiber-local
  #   Console.logger all work as they do for the rest of the socketry stack.
  #   The default is the Console module rather than Console.logger: the module
  #   looks the current logger up on every call, so one set with
  #   Console.logger= before connect reaches every task the session spawns.
  # [A Logger-compatible object] (stdlib Logger, Rails.logger,
  #   AsyncRabbitMQ::Log, anything with debug/info/warn/error taking a
  #   message) wrapped in a shim that drops the subject — one destination per
  #   session, independent of the process-wide Console logger.
  # [nil] every message discarded.
  #
  #   Session.new                              # Console
  #   Session.new(logger: Rails.logger)        # Rails' logger
  #   Session.new(logger: nil)                 # silent
  module Logging
    LEVELS = %i[debug info warn error fatal].freeze

    # Adapts a Logger-compatible object to the (subject, message) call shape
    # by dropping the subject.
    class LoggerShim
      attr_reader :logger

      def initialize(logger)
        @logger = logger
      end

      # Accepts the whole Console call shape - extra arguments and keywords
      # included, e.g. error(self, msg, exception: e) - and passes on only
      # the message, so a call site written for Console never raises for a
      # user who passed a plain Logger.
      (LEVELS - [:fatal]).each do |level|
        define_method(level) do |_subject, message = nil, *_rest, **_fields, &block|
          @logger.public_send(level, message, &block)
          nil
        end
      end

      # resolve only requires debug/info/warn/error, the levels this gem
      # uses; a logger without fatal gets it as error rather than raising
      # NoMethodError from inside an error path.
      def fatal(_subject, message = nil, *_rest, **_fields, &block)
        level = @logger.respond_to?(:fatal) ? :fatal : :error
        @logger.public_send(level, message, &block)
        nil
      end
    end

    # Discards everything: what +logger: nil+ means.
    class Null
      LEVELS.each { |level| define_method(level) { |*, **, &| nil } }
    end

    NULL = Null.new.freeze

    # Turn a +logger:+ argument into an object that takes (subject, message).
    # Idempotent, so every component that accepts a logger can resolve it
    # without caring whether its caller already did.
    def self.resolve(logger)
      # The Console module by identity: `case` would test Console === logger,
      # an is_a? check the module itself does not pass.
      return logger if logger.equal?(::Console)

      case logger
      when nil                       then NULL
      when LoggerShim, Null          then logger
      when ::Console::Filter         then logger # Console.logger, a Console::Logger
      else
        unless LEVELS.take(4).all? { |level| logger.respond_to?(level) }
          raise ArgumentError, "logger: must respond to debug, info, warn and error " \
                               "(or be Console, or nil to discard) - got #{logger.inspect}"
        end
        LoggerShim.new(logger)
      end
    end
  end
end
