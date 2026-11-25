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
  end
end
