module AsyncRabbitMQ
  # Base error class
  class Error < StandardError; end

  # Raised when a TCP connection cannot be established or times out during
  # the AMQP connection.start / tune / open negotiation.
  class ConnectionTimeoutError < Error; end

  # Raised on hard AMQP errors (501-541) — the broker closed the connection.
  # Callers must reconnect.
  class ConnectionError < Error
    attr_reader :code, :text

    def initialize(msg = nil, code: nil, text: nil)
      @code = code
      @text = text
      super(msg || "Connection error #{code}: #{text}")
    end
  end

  # Raised on soft AMQP errors (403/404/405/406) — the broker closed the
  # channel. The connection stays alive; callers may open a new channel.
  class ChannelError < Error
    attr_reader :code, :text, :channel_id

    def initialize(msg = nil, code: nil, text: nil, channel_id: nil)
      @code       = code
      @text       = text
      @channel_id = channel_id
      super(msg || "Channel error #{code}: #{text}")
    end
  end

  # Raised when no heartbeat is received within 2× the negotiated interval.
  class HeartbeatTimeoutError < ConnectionError; end

  # Raised when an operation is attempted on a closed channel or session.
  class NotOpenError < Error; end

  # Raised when SASL negotiation fails (no common mechanism, bad credentials).
  class AuthenticationError < Error; end
end
