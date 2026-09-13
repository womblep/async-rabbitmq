require_relative "async_rabbitmq/version"
require_relative "async_rabbitmq/log"
require_relative "async_rabbitmq/errors"
require_relative "async_rabbitmq/notifier"
require_relative "async_rabbitmq/topology_registry"
require_relative "async_rabbitmq/frame_io"
require_relative "async_rabbitmq/queue"
require_relative "async_rabbitmq/exchange"
require_relative "async_rabbitmq/channel"
require_relative "async_rabbitmq/session"

module AsyncRabbitMQ
  # Optional: require "async_rabbitmq/pool" for connection pooling.
end
