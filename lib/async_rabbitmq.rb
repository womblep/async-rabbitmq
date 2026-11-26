module AsyncRabbitMQ
  class << self
    # Whether a channel warns, once, when it starts a consumer whose backlog
    # nothing bounds - an auto-ack consumer, or a manual-ack one with no
    # prefetch. True by default.
    #
    # Set it false when you have read the warning and accepted the trade, so
    # that silencing it does not mean turning the logger down and losing every
    # other warning a channel raises (stale delivery tags, broker-cancelled
    # consumers, handler exceptions). It is read when a consumer starts, so set
    # it during boot, before the first #basic_consume.
    #
    #   AsyncRabbitMQ.warn_unbounded_consumers = false
    attr_accessor :warn_unbounded_consumers

    # The most this gem logs, whichever logger a session was given: :debug,
    # :info, :warn (the default), :error, :fatal, or :off. Checked before a
    # message reaches the logger, so an application running Console - or
    # Rails' logger - at info is not flooded with this client's info and debug
    # lines; raise it to see them. Process-wide and read on every call, so it
    # can be changed at any time. To silence one session, give it logger: nil.
    #
    #   AsyncRabbitMQ.log_level = :debug   # troubleshooting a connection
    #   AsyncRabbitMQ.log_level = :off     # nothing at all
    attr_reader :log_level

    def log_level=(level)
      level = level.to_sym
      unless %i[debug info warn error fatal off].include?(level)
        raise ArgumentError, "log_level must be :debug, :info, :warn, :error, :fatal or :off (got #{level.inspect})"
      end

      @log_level = level
    end
  end
  self.warn_unbounded_consumers = true
  self.log_level = ENV.fetch("ASYNC_RABBITMQ_LOG_LEVEL", "warn")
end

require_relative "async_rabbitmq/version"
require_relative "async_rabbitmq/log"
require_relative "async_rabbitmq/logging"
require_relative "async_rabbitmq/errors"
require_relative "async_rabbitmq/tls"
require_relative "async_rabbitmq/channel_id_allocator"
require_relative "async_rabbitmq/notifier"
require_relative "async_rabbitmq/versioned_delivery_tag"
require_relative "async_rabbitmq/message"
require_relative "async_rabbitmq/publish_outcome"
require_relative "async_rabbitmq/consumer"
require_relative "async_rabbitmq/managed_consumer"
require_relative "async_rabbitmq/topology_registry"
require_relative "async_rabbitmq/frame_io"
require_relative "async_rabbitmq/queue"
require_relative "async_rabbitmq/exchange"
require_relative "async_rabbitmq/channel"
require_relative "async_rabbitmq/session"
require_relative "async_rabbitmq/cluster"

module AsyncRabbitMQ
  DEFAULT_CONTENT_TYPE = Channel::DEFAULT_CONTENT_TYPE

  # Optional: require "async_rabbitmq/pool" for connection pooling.
end
