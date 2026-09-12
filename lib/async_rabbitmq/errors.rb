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

  # Raised on soft AMQP errors (311/312/313/403/404/405/406) — the broker closed
  # the channel. The connection stays alive; callers may open a new channel.
  #
  # When the error comes from a broker-sent channel.close, +close_method+ is the
  # decoded AMQ::Protocol::Channel::Close and the predicates below identify the
  # common causes without callers having to pattern-match +text+.
  class ChannelError < Error
    attr_reader :code, :text, :channel_id, :close_method

    def initialize(msg = nil, code: nil, text: nil, channel_id: nil, close_method: nil)
      @code         = code
      @text         = text
      @channel_id   = channel_id
      @close_method = close_method
      super(msg || "Channel error #{code}: #{text}")
    end

    # Consumer delivery acknowledgement timeout (406 PRECONDITION_FAILED).
    # See https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout
    def delivery_ack_timeout?
      @close_method.respond_to?(:delivery_ack_timeout?) && @close_method.delivery_ack_timeout?
    end

    # Unknown delivery tag, e.g. a double ack (406 PRECONDITION_FAILED).
    def unknown_delivery_tag?
      @close_method.respond_to?(:unknown_delivery_tag?) && @close_method.unknown_delivery_tag?
    end

    # Message exceeded the broker's configured maximum size (406 PRECONDITION_FAILED).
    def message_too_large?
      @close_method.respond_to?(:message_too_large?) && @close_method.message_too_large?
    end
  end

  # Raised when no heartbeat is received within 2× the negotiated interval.
  class HeartbeatTimeoutError < ConnectionError; end

  # Raised when an operation is attempted on a closed channel or session.
  class NotOpenError < Error; end

  # Raised when authentication fails: no SASL mechanism in common with the
  # broker, or the broker refused the credentials / vhost access with a
  # connection.close 403 ACCESS_REFUSED (then +code+ and +text+ are set).
  class AuthenticationError < Error
    attr_reader :code, :text

    def initialize(msg = nil, code: nil, text: nil)
      @code = code
      @text = text
      super(msg || "Authentication failed (#{code}): #{text}")
    end
  end
end
