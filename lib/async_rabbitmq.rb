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
  end
  self.warn_unbounded_consumers = true
end

require_relative "async_rabbitmq/version"
require_relative "async_rabbitmq/log"
require_relative "async_rabbitmq/errors"
require_relative "async_rabbitmq/tls"
require_relative "async_rabbitmq/channel_id_allocator"
require_relative "async_rabbitmq/notifier"
require_relative "async_rabbitmq/versioned_delivery_tag"
require_relative "async_rabbitmq/topology_registry"
require_relative "async_rabbitmq/frame_io"
require_relative "async_rabbitmq/queue"
require_relative "async_rabbitmq/exchange"
require_relative "async_rabbitmq/channel"
require_relative "async_rabbitmq/session"
require_relative "async_rabbitmq/cluster"

module AsyncRabbitMQ
  # Optional: require "async_rabbitmq/pool" for connection pooling.
end
