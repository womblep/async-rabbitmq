require "async/pool"
require_relative "session"

module AsyncRabbitMQ
  # Opt-in connection pool backed by Async::Pool::Controller.
  #
  # Usage:
  #   pool = AsyncRabbitMQ::Pool.new(max: 5, host: "localhost")
  #   pool.acquire { |session| session.open_channel.basic_publish(...) }
  class Pool
    def initialize(max: 4, **session_opts)
      @session_opts = session_opts
      @pool = Async::Pool::Controller.new(
        Async::Pool::Resource::Wrapper.new { Session.new(**@session_opts).tap(&:connect) },
        limit: max
      )
    end

    def acquire(&block)
      @pool.acquire(&block)
    end

    def close
      @pool.close
    end
  end
end
