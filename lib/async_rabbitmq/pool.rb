begin
  require "async/pool"
rescue LoadError
  raise LoadError, "AsyncRabbitMQ::Pool needs the async-pool gem, which this gem does not " \
                   "depend on: add gem \"async-pool\" to your Gemfile"
end
require_relative "session"

module AsyncRabbitMQ
  # Opt-in connection pool backed by Async::Pool::Controller.
  #
  # Each pooled resource is a connected Session. A Session advertises its
  # negotiated channel_max as its +concurrency+, so the controller hands the
  # same Session to many fibers at once and only opens another connection
  # when every existing one is saturated or unusable.
  #
  # Usage:
  #   pool = AsyncRabbitMQ::Pool.new(max: 5, host: "localhost")
  #   pool.acquire { |session| session.with_channel { |ch| ch.basic_publish(...) } }
  #   pool.close
  class Pool
    def initialize(max: 4, **session_opts)
      @session_opts = session_opts
      constructor   = -> { Session.new(**@session_opts).tap(&:connect) }
      @pool         = Async::Pool::Controller.new(constructor, limit: max)
    end

    # Acquire a connected Session. With a block, the Session is released when
    # the block returns; without one, the caller must call #release.
    def acquire(&block)
      @pool.acquire(&block)
    end

    def release(session)
      @pool.release(session)
    end

    # Close every pooled Session.
    def close
      @pool.close
    end
  end
end
