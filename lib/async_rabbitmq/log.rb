# frozen_string_literal: true

module AsyncRabbitMQ
  # The default logger: warnings and errors, one line each, to $stderr.
  #
  # Anything that responds to +debug+, +info+, +warn+ and +error+ can be passed
  # as +logger:+ instead, and that is the expected thing to do in an
  # application: Ruby's Logger, Rails.logger, a Console wrapper, your own
  # object. The gem deliberately does not depend on the logger gem, which
  # stopped being a default gem in Ruby 4.0, so the choice of logging stack
  # stays with the application.
  #
  #   AsyncRabbitMQ::Session.new(logger: Rails.logger)
  #   AsyncRabbitMQ::Session.new(logger: Logger.new($stdout, level: Logger::INFO))
  #   AsyncRabbitMQ::Session.new(logger: AsyncRabbitMQ::Log.silent)
  #   AsyncRabbitMQ::Session.new(logger: AsyncRabbitMQ::Log.new($stdout, level: :debug))
  class Log
    LEVELS = { debug: 0, info: 1, warn: 2, error: 3, fatal: 4 }.freeze
    DEFAULT_LEVEL = :warn

    attr_reader :level

    # +io+ nil discards everything.
    def initialize(io = $stderr, level: DEFAULT_LEVEL)
      @io = io
      self.level = level
    end

    # Accepts a symbol (:debug) or Logger's integer constants (Logger::DEBUG).
    def level=(value)
      @level = value.is_a?(Integer) ? (LEVELS.key(value) || DEFAULT_LEVEL) : value.to_sym
      @threshold = LEVELS.fetch(@level, LEVELS[DEFAULT_LEVEL])
    end

    # Discards everything. Useful in tests and in one-shot scripts.
    def self.silent
      new(nil)
    end

    LEVELS.each_key do |name|
      define_method(name) do |message = nil, &block|
        return nil if @io.nil? || LEVELS.fetch(name) < @threshold

        text = message.nil? && block ? block.call : message
        @io.puts("#{name.to_s.upcase.ljust(5)} async-rabbitmq: #{text}")
        nil
      end

      define_method(:"#{name}?") { !@io.nil? && LEVELS.fetch(name) >= @threshold }
    end
  end
end
