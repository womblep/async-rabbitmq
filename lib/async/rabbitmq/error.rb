# frozen_string_literal: true

module Async
  module RabbitMQ
    # Everything this library raises on purpose.
    class Error < StandardError; end

    # The connection is gone: the socket failed, the broker closed it, or it
    # was closed here. +code+ and +text+ are the broker's when it gave any.
    # Whatever was in flight on the connection may or may not have happened.
    class ConnectionError < Error
      attr :code, :text

      def initialize(message = nil, code: nil, text: nil)
        @code = code
        @text = text
        super(message || (code ? "Connection closed by the broker: #{code} #{text}" : "Connection closed"))
      end
    end

    # The connection could not be established in time.
    class ConnectTimeoutError < ConnectionError; end

    # The broker refused the credentials or access to the virtual host.
    class AuthenticationError < ConnectionError; end

    # Nothing was heard from the broker for twice the heartbeat timeout.
    class HeartbeatTimeoutError < ConnectionError; end

    # The broker sent something that is not valid AMQP 0-9-1.
    class ProtocolError < ConnectionError; end

    # The broker closed one channel. The connection is unaffected, and so is
    # everything else on it. +method+ is the decoded channel.close.
    class ChannelError < Error
      attr :code, :text, :method

      def initialize(message = nil, code: nil, text: nil, method: nil)
        @code   = code
        @text   = text
        @method = method
        super(message || "Channel closed by the broker: #{code} #{text}")
      end

      # 404: the queue or exchange named does not exist.
      def not_found?
        @code == 404
      end

      # 403: the user may not do this.
      def access_refused?
        @code == 403
      end

      # 405: the queue is exclusive to another connection.
      def resource_locked?
        @code == 405
      end

      # 406: the request contradicts what is already there.
      def precondition_failed?
        @code == 406
      end
    end

    # Something was asked of a channel or connection that has already closed.
    # +cause+ is why it closed, when that is known.
    class ClosedError < Error; end

    # The broker did not answer a request in time. The channel it was sent on
    # is closed, because a late reply could not be told from the next one's.
    class TimeoutError < Error; end

    # Every channel id the broker allows on this connection is in use.
    class ChannelLimitError < Error; end

    # A published message was not confirmed.
    class PublishError < Error; end

    # The connection was lost after the message was sent and before the broker
    # answered for it, so the broker may or may not have it. Sending it again
    # (Publisher::Outcome#resubmit) risks a second copy; not sending it risks
    # none. Which is right depends on the message, so it is the caller's call.
    # +reason+ is the error that ended the connection.
    class UnconfirmedError < PublishError
      attr :reason

      def initialize(reason)
        @reason = reason
        super("The connection was lost before the broker answered for the message, which it may or may not " \
              "have taken (#{reason.class.name.split('::').last}: #{reason.message})")
      end
    end

    # The broker refused the message: the queue is full and rejects what
    # overflows it, or the broker failed internally. It is in no queue.
    class NackedError < PublishError; end

    # The message was published +mandatory+ and no queue was bound to take it.
    # +code+ and +text+ are the broker's (312 NO_ROUTE).
    class ReturnedError < PublishError
      attr :code, :text, :exchange, :routing_key

      def initialize(code:, text:, exchange:, routing_key:)
        @code        = code
        @text        = text
        @exchange    = exchange
        @routing_key = routing_key
        super("Message returned by the broker: #{code} #{text} " \
              "(exchange #{exchange.inspect}, routing key #{routing_key.inspect})")
      end
    end
  end
end
