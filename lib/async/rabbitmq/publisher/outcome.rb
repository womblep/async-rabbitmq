# frozen_string_literal: true

require "async/promise"

require_relative "../error"

module Async
  module RabbitMQ
    class Publisher
      # What became of one message, returned by Publisher#submit.
      #
      #   outcome = publisher.submit("hello", routing_key: "orders")
      #   outcome.wait   # returns once the broker has it, or raises
      #
      # #wait raises the reason the message was not confirmed:
      #
      # - NackedError: the broker refused it.
      # - ReturnedError: it was mandatory and no queue was bound to take it.
      # - UnconfirmedError: the connection was lost before the broker
      #   answered, so it may or may not have the message. (A publisher
      #   created with +resend: true+ sends the message again itself instead,
      #   and the outcome goes on waiting for that copy.)
      # - ChannelError: the broker closed the publisher's channel before
      #   answering - most often because the exchange does not exist - so
      #   again the message may or may not have been taken.
      # - ClosedError or ConnectionError: the publisher was closed, or its
      #   client gave up reconnecting, before the broker answered.
      #
      # A message that was not confirmed can be sent again with #resubmit.
      class Outcome
        attr :exchange, :routing_key

        # :pending, :acked, :nacked, :returned or :failed.
        attr :status

        # What #wait raises; nil while pending and when acked.
        attr :error

        # Internal: what is needed to send the message again. Dropped once
        # the message is acked.
        attr :body, :properties, :mandatory, :publish_id

        # Internal: the confirm tag the message is waiting under, and whether
        # it has been handed to the connection under that tag.
        attr_accessor :tag, :sent

        def initialize(publisher, body, exchange, routing_key, mandatory, properties, publish_id)
          @publisher   = publisher
          @body        = body
          @exchange    = exchange
          @routing_key = routing_key
          @mandatory   = mandatory
          @properties  = properties
          @publish_id  = publish_id
          @tag         = nil
          @sent        = false
          @status      = :pending
          @error       = nil
          @promise     = Async::Promise.new
        end

        # Wait for the broker's answer. Returns self once the message is
        # acked, and raises otherwise, as described on the class. With
        # +timeout+ (seconds) raises TimeoutError if no answer comes in time;
        # the outcome is still pending then, and can be waited on again.
        def wait(timeout: nil)
          if timeout && @promise.wait?(timeout: timeout).nil?
            raise TimeoutError, "No confirm from the broker within #{timeout}s"
          end

          @promise.wait
          self
        end

        # Send the same message again, through the same publisher. For a
        # message that was not confirmed. The outcome goes back to pending and
        # now answers for the new copy, so it can be waited on again:
        #
        #   begin
        #     outcome.wait
        #   rescue Async::RabbitMQ::UnconfirmedError
        #     outcome.resubmit
        #     retry
        #   end
        #
        # After an UnconfirmedError or a ChannelError the broker may already
        # have the first copy, and a consumer may then see two. The new copy
        # takes its place behind whatever has been published since. Returns
        # self. Raises ClosedError, leaving the outcome failed, if the
        # publisher has been closed.
        def resubmit
          raise Error, "This message is still waiting for the broker's answer" if @status == :pending
          raise Error, "This message was confirmed; there is nothing to send again" if @status == :acked

          @publisher.resubmit(self)
          self
        end

        def pending?
          @status == :pending
        end

        def acked?
          @status == :acked
        end

        def nacked?
          @status == :nacked
        end

        def returned?
          @status == :returned
        end

        # True when the connection, the channel or the publisher went before
        # the broker answered.
        def failed?
          @status == :failed
        end

        def inspect
          "#<#{self.class.name} #{@status} exchange=#{@exchange.inspect} routing_key=#{@routing_key.inspect}>"
        end

        # ---------------------------------------------------------------------
        # Internal: driven by the Publisher. The first answer stands, which
        # matters for a returned message: the broker follows the return with
        # an ordinary ack.
        # ---------------------------------------------------------------------

        def ack!
          return unless @status == :pending

          @status = :acked
          @body   = @properties = nil
          @promise.resolve(true)
        end

        def nack!(error)
          finish(:nacked, error)
        end

        def return!(error)
          finish(:returned, error)
        end

        def fail!(error)
          finish(:failed, error)
        end

        # Back to pending, for another copy of the message. Whoever waited
        # for the last answer has had it; a new promise is for the next.
        def rearm!(properties, publish_id)
          @properties = properties
          @publish_id = publish_id
          @tag        = nil
          @sent       = false
          @status     = :pending
          @error      = nil
          @promise    = Async::Promise.new
        end

        private

        def finish(status, error)
          return unless @status == :pending

          @status = status
          @error  = error
          @promise.reject(error)
        end
      end
    end
  end
end
