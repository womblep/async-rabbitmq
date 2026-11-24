# frozen_string_literal: true

module AsyncRabbitMQ
  # Every log call in this gem has Console's shape: the object doing the
  # logging, then the message —
  #
  #   @logger.warn(self, "Connection lost ...")
  #
  # — and +@logger+ is an adapter Logging.resolve made from the +logger:+ the
  # caller passed. Each session gets its own adapter object; nothing shared is
  # ever handed out, so patching a session's logger cannot reach another.
  #
  # [Console (the default)] forwarded to Console with the subject kept, so
  #   Console's per-class filtering, CONSOLE_LEVEL and its fiber-local
  #   Console.logger work as for the rest of the socketry stack. A
  #   Console::Logger instance is forwarded to as given; the Console module is
  #   looked up on every call, so a Console.logger set before connect reaches
  #   every task the session spawns.
  # [A Logger-compatible object] (stdlib Logger, Rails.logger,
  #   AsyncRabbitMQ::Log, anything with debug/info/warn/error taking a
  #   message) — the subject is dropped. One destination per session,
  #   independent of the process-wide Console logger.
  # [nil] every message discarded.
  #
  # On top of whatever the destination does, AsyncRabbitMQ.log_level caps
  # what this gem emits at all (default :warn) — so an application running
  # Console at info is not flooded with this client's info and debug lines
  # unless it asks for them.
  #
  #   Session.new                              # Console, warnings and up
  #   Session.new(logger: Rails.logger)        # Rails' logger
  #   Session.new(logger: nil)                 # silent
  #   AsyncRabbitMQ.log_level = :debug         # everything, wherever it goes
  #   AsyncRabbitMQ.log_level = :off           # nothing, from any session
  module Logging
    LEVELS = %i[debug info warn error fatal].freeze
    SEVERITY = LEVELS.each_with_index.to_h.merge(off: LEVELS.size).freeze

    # Whether +level+ passes AsyncRabbitMQ.log_level. Read on every call, so
    # changing the level takes effect at once, for every session.
    def self.enabled?(level)
      SEVERITY.fetch(level) >= SEVERITY.fetch(AsyncRabbitMQ.log_level)
    end

    # Shared by the adapters: one method per level that checks the gem-wide
    # level, then hands the call to #emit.
    module Gate
      LEVELS.each do |level|
        define_method(level) do |subject, *arguments, **fields, &block|
          emit(level, subject, *arguments, **fields, &block) if Logging.enabled?(level)
          nil
        end
      end
    end

    # Forwards to Console, keeping the subject.
    class ConsoleAdapter
      include Gate

      # +target+ nil means the Console module, resolved per call.
      def initialize(target = nil)
        @target = target
      end

      def target
        @target || ::Console
      end

      private

      def emit(level, subject, *arguments, **fields, &block)
        target.public_send(level, subject, *arguments, **fields, &block)
      end
    end

    # Adapts a Logger-compatible object to the (subject, message) call shape
    # by dropping the subject. Accepts the whole Console call shape - extra
    # arguments and keywords included, e.g. error(self, msg, exception: e) -
    # and passes on only the message, so a call site written for Console
    # never raises for a user who passed a plain Logger.
    class LoggerShim
      include Gate

      attr_reader :logger

      def initialize(logger)
        @logger = logger
      end

      private

      def emit(level, _subject, message = nil, *_rest, **_fields, &block)
        # resolve only requires debug/info/warn/error, the levels this gem
        # uses; a logger without fatal gets it as error rather than raising
        # NoMethodError from inside an error path.
        level = :error if level == :fatal && !@logger.respond_to?(:fatal)
        @logger.public_send(level, message, &block)
      end
    end

    # Discards everything: what +logger: nil+ means.
    class Null
      LEVELS.each { |level| define_method(level) { |*, **, &| nil } }
    end

    NULL = Null.new.freeze

    # Turn a +logger:+ argument into an adapter that takes (subject, message).
    # Idempotent, so every component that accepts a logger can resolve it
    # without caring whether its caller already did.
    def self.resolve(logger)
      case logger
      when nil                                  then NULL
      when ConsoleAdapter, LoggerShim, Null     then logger
      else
        if console_module?(logger)
          ConsoleAdapter.new
        elsif console_logger?(logger)
          ConsoleAdapter.new(logger)
        elsif LEVELS.take(4).all? { |level| logger.respond_to?(level) }
          LoggerShim.new(logger)
        else
          raise ArgumentError, "logger: must respond to debug, info, warn and error " \
                               "(or be Console, or nil to discard) - got #{logger.inspect}"
        end
      end
    end

    # Console is referenced only when the caller chose it — it is the default
    # Session/Cluster argument, and async loads it anyway — so neither check
    # requires it.
    def self.console_module?(logger)
      defined?(::Console) && logger.equal?(::Console)
    end

    def self.console_logger?(logger)
      defined?(::Console::Filter) && logger.is_a?(::Console::Filter)
    end
    private_class_method :console_module?, :console_logger?
  end
end
