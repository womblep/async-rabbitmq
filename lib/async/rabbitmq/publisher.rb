# frozen_string_literal: true

require "securerandom"
require "async"
require "async/promise"
require "async/semaphore"
require "console"
require "amq/protocol"

require_relative "error"
require_relative "channel"
require_relative "publisher/outcome"

module Async
  module RabbitMQ
    # Publishes messages, and knows what became of each one.
    #
    #   publisher = client.publisher("events")
    #   publisher.publish("hello", routing_key: "order.created")
    #
    # #publish returns when the broker has taken the message, and raises if it
    # did not. Many fibers can publish through one publisher at once; each
    # waits only for its own message. #submit sends without waiting and
    # returns an Outcome to ask later.
    #
    # A publisher has a channel to itself, with publisher confirms on. When
    # the connection is lost it waits for the client's next one and opens a
    # channel there, and publishing carries on: a message published while
    # there is no connection waits for one.
    #
    # A message that had been sent, and not answered for, when the connection
    # went is another matter: the broker may have it or may not, and nothing
    # will ever say which. It fails with UnconfirmedError, and the caller
    # decides. Outcome#resubmit sends it again, at the risk of a second copy,
    # and the same outcome then answers for the new one:
    #
    #   begin
    #     outcome.wait
    #   rescue Async::RabbitMQ::UnconfirmedError
    #     outcome.resubmit
    #     retry
    #   end
    #
    # A publisher created with +resend: true+ makes that choice for every
    # message: on the new channel it sends again everything unanswered, in
    # the order it was first sent, and the fibers waiting in #publish go on
    # waiting and return when the second copy is confirmed. That keeps the
    # order, which resubmitting by hand does not, and suits consumers that
    # can stand to see a message twice. A message may then also arrive long
    # after it was published, if the connection was long in coming back.
    #
    # When the broker closes the channel - publishing to an exchange that
    # does not exist does it - there is no telling which message was at fault
    # or which of those in flight were taken, so every one of them fails with
    # the ChannelError, with or without +resend+. The publisher opens a new
    # channel and stays usable.
    #
    # Options, each the default for every message and overridable per publish:
    #
    # [mandatory]  have the broker send back a message no queue takes, which
    #              raises ReturnedError. Default false. A mandatory message
    #              carries one extra header, PUBLISH_ID_HEADER, which is how
    #              its return is matched to it; consumers will see it.
    # [persistent] have the broker write the message to disk. Default true.
    #
    # And two for the publisher alone:
    #
    # [resend]          send again, on the next connection, what the broker
    #                   had not answered for when one is lost. Default false.
    # [max_unconfirmed] how many messages may be waiting for the broker's
    #                   answer before #submit and #publish wait for room.
    #                   Default 1000; nil for no limit.
    class Publisher
      include Channel::Delegate

      P = AMQ::Protocol
      private_constant :P

      # The header a mandatory message carries so that its return can be
      # matched to it.
      PUBLISH_ID_HEADER = "x-async-rabbitmq-publish-id"

      # How long #close waits, by default, for the broker to answer for what
      # has been sent.
      CLOSE_TIMEOUT = 5

      attr :client, :exchange

      def initialize(client, exchange = "", mandatory: false, persistent: true, resend: false,
                     max_unconfirmed: 1000)
        @client      = client
        @exchange    = exchange.to_s
        @mandatory   = mandatory
        @persistent  = persistent
        @resend      = resend
        @limit       = max_unconfirmed
        @token       = SecureRandom.hex(6)
        @sequence    = 0
        @channel     = nil
        # Resolved with the channel once it is open, in confirm mode, and
        # what was unanswered has been sent again on it; replaced when that
        # channel closes; rejected when the publisher ends.
        @ready       = Async::Promise.new
        @lost        = nil # Promise for the current channel closing
        @gate        = Async::Semaphore.new(1)
        @tag         = 0
        @unconfirmed = {} # confirm tag => Outcome, in the order sent
        @returnable  = {} # publish id => Outcome, for mandatory messages
        @room        = nil # Promise a publish waits on when at the limit
        @drained     = nil # Promise #close waits on
        @closed      = false
        @keeper      = nil

        client.register(self)
        begin
          attach(first: true)
        rescue Exception # rubocop:disable Lint/RescueException
          client.unregister(self)
          @closed = true
          raise
        end
        @keeper = client.spawn { keep }
      end

      # Publish +body+ and wait for the broker to take it. Returns true.
      # Raises NackedError if the broker refused it, ReturnedError if it was
      # mandatory and no queue took it, UnconfirmedError if the connection
      # was lost before the broker answered (unless the publisher resends),
      # ChannelError if the broker closed the channel, and ClosedError if the
      # publisher was closed first. While there is no connection it waits
      # for one.
      #
      # +routing_key+ is the queue's name when publishing to the default
      # exchange. The rest are the message's AMQP properties.
      def publish(body, **options)
        submit(body, **options).wait
        true
      end

      # Send +body+ and return an Outcome for it without waiting for the
      # broker's answer. Takes what #publish takes. It does wait while there
      # is no connection, while the broker has publishing blocked, and for
      # room under +max_unconfirmed+.
      def submit(body, routing_key: "", exchange: @exchange, mandatory: @mandatory, persistent: @persistent,
                 content_type: nil, content_encoding: nil, headers: nil, priority: nil, correlation_id: nil,
                 reply_to: nil, expiration: nil, message_id: nil, timestamp: nil, type: nil, user_id: nil,
                 app_id: nil)
        raise ArgumentError, "A message body is a String, not #{body.class}" unless body.is_a?(String)

        properties = { delivery_mode: persistent ? 2 : 1 }
        properties[:content_type]     = content_type     if content_type
        properties[:content_encoding] = content_encoding if content_encoding
        properties[:priority]         = priority         if priority
        properties[:correlation_id]   = correlation_id   if correlation_id
        properties[:reply_to]         = reply_to.to_s    if reply_to
        properties[:expiration]       = expiration.to_s  if expiration
        properties[:message_id]       = message_id       if message_id
        properties[:timestamp]        = timestamp        if timestamp
        properties[:type]             = type             if type
        properties[:user_id]          = user_id          if user_id
        properties[:app_id]           = app_id           if app_id

        publish_id = nil
        if mandatory
          publish_id = "#{@token}.#{@sequence += 1}"
          headers    = headers ? headers.merge(PUBLISH_ID_HEADER => publish_id) : { PUBLISH_ID_HEADER => publish_id }
        elsif headers
          # The caller keeps its hash and may change it; what is sent again
          # after a reconnect must be what was sent the first time.
          headers = headers.dup
        end
        properties[:headers] = headers if headers

        # String#b shares the caller's buffer until either side changes it, so
        # this is a snapshot at the cost of no copy.
        outcome = Outcome.new(self, body.b, exchange.to_s, routing_key.to_s, mandatory ? true : false, properties,
                              publish_id)
        send_message(outcome)
        outcome
      end

      # True if this publisher sends again, by itself, what was unanswered
      # when a connection was lost.
      def resend?
        @resend
      end

      # How many messages are waiting for the broker's answer.
      def unconfirmed_count
        @unconfirmed.size
      end

      # True while the publisher has a channel to publish on.
      def connected?
        !@channel.nil?
      end

      def closed?
        @closed
      end

      # Close the publisher. Waits up to +timeout+ seconds for the broker to
      # answer for what has been sent; whatever is still unanswered then fails
      # with ClosedError. Nothing more can be published.
      def close(timeout: CLOSE_TIMEOUT)
        return if @closed

        @closed = true
        if @channel && !@unconfirmed.empty? && timeout&.positive?
          @drained = Async::Promise.new
          @drained.wait?(timeout: timeout)
        end
        finish(ClosedError.new("This publisher has been closed"))
        nil
      end

      def inspect
        state = @closed ? "closed" : (@channel ? "connected" : "waiting")
        "#<#{self.class.name} exchange=#{@exchange.inspect} #{state} unconfirmed=#{@unconfirmed.size}>"
      end

      # Internal: Outcome#resubmit. The same message as a new publish, which
      # the same Outcome then answers for.
      def resubmit(outcome)
        raise closed_error if @closed

        properties = outcome.properties
        publish_id = nil
        if outcome.mandatory
          # A new id: a late return for the first copy must not be taken for
          # this one's.
          publish_id = "#{@token}.#{@sequence += 1}"
          properties = properties.merge(headers: properties[:headers].merge(PUBLISH_ID_HEADER => publish_id))
        end
        outcome.rearm!(properties, publish_id)
        begin
          send_message(outcome)
        rescue Exception => error # rubocop:disable Lint/RescueException
          # Not sent and never will be: it must not be left pending, with
          # nothing to answer it. (Once sent, the broker's answer will come.)
          unless outcome.sent
            @unconfirmed.delete(outcome.tag) if outcome.tag && @unconfirmed[outcome.tag].equal?(outcome)
            @returnable.delete(publish_id) if publish_id
            outcome.fail!(error.is_a?(StandardError) ? error : closed_error)
          end
          raise
        end
      end

      # -----------------------------------------------------------------------
      # Internal: Channel::Delegate. Called on the connection's reader fiber,
      # so nothing here may wait.
      # -----------------------------------------------------------------------

      def confirmed(method)
        tag   = method.delivery_tag
        acked = method.is_a?(P::Basic::Ack)
        if method.multiple
          # Insertion order is tag order, so the tags covered are at the front.
          loop do
            first = nil
            @unconfirmed.each_key do |key|
              first = key
              break
            end
            break unless first && first <= tag

            settle(@unconfirmed.delete(first), acked)
          end
        elsif (outcome = @unconfirmed.delete(tag))
          settle(outcome, acked)
        end
        made_room
      end

      def returned(method, header, _body)
        id = header.properties&.[](:headers)&.[](PUBLISH_ID_HEADER) or return
        outcome = @returnable.delete(id) or return

        outcome.return!(ReturnedError.new(code: method.reply_code, text: method.reply_text,
                                          exchange: method.exchange, routing_key: method.routing_key))
        # The broker has answered for this message, so it must not be sent
        # again if the connection goes before the ack that follows the return.
        @unconfirmed.delete(outcome.tag) if @unconfirmed[outcome.tag].equal?(outcome)
        made_room
      end

      def closed(error)
        @channel = nil
        @ready   = Async::Promise.new if @ready.resolved? && !@closed
        if error.is_a?(ChannelError)
          fail_sent(error)
        elsif error.is_a?(ConnectionError) && !@resend
          fail_sent(UnconfirmedError.new(error))
        end
        @lost&.resolve(error)
        @drained&.resolve(true)
      end

      private

      # One at a time, so that the order of the confirm tags is the order on
      # the wire: the write can wait, and a second fiber must not get its
      # message out ahead of a first that already has the earlier tag.
      def send_message(outcome)
        @gate.acquire do
          wait_for_room
          channel = attached_channel
          @unconfirmed[@tag += 1] = outcome
          outcome.tag = @tag
          @returnable[outcome.publish_id] = outcome if outcome.publish_id
          begin
            write_message(channel, outcome)
          rescue ConnectionError, ClosedError
            # The channel went under the write. Whoever closed it has dealt
            # with this message as with the rest: failed it if it had gone
            # out, and otherwise kept it for the next channel.
            raise if @closed
          end
        end
      end

      # Hand the message to the connection, waiting first for as long as the
      # broker has publishing blocked. It counts as sent from the moment the
      # connection is given it, and not before: a message still waiting here
      # when the channel closes has certainly not reached the broker.
      def write_message(channel, outcome)
        connection = channel.connection
        connection.wait_unblocked if connection.blocked?
        outcome.sent = true
        channel.write(encode(channel, outcome))
      end

      def wait_for_room
        while @limit && @unconfirmed.size >= @limit
          raise closed_error if @closed

          (@room ||= Async::Promise.new).wait
        end
      end

      def made_room
        if (room = @room) && @unconfirmed.size < @limit
          @room = nil
          room.resolve(true)
        end
        @drained&.resolve(true) if @unconfirmed.empty?
      end

      # The channel, waiting for one while there is none.
      def attached_channel
        loop do
          raise closed_error if @closed

          channel = @channel and return channel
          @ready.wait
        end
      end

      def encode(channel, outcome)
        P::Basic::Publish.encode(channel.id, outcome.body, outcome.properties, outcome.exchange,
                                 outcome.routing_key, outcome.mandatory, false,
                                 channel.connection.frame_max).map(&:encode).join
      end

      def settle(outcome, acked)
        @returnable.delete(outcome.publish_id) if outcome.publish_id
        if acked
          outcome.ack!
        else
          outcome.nack!(NackedError.new("The broker refused the message (exchange #{outcome.exchange.inspect}, " \
                                        "routing key #{outcome.routing_key.inspect})"))
        end
      end

      # The channel has closed. Everything sent on it and unanswered fails; a
      # message that had not gone out yet is kept, to go on the next.
      def fail_sent(error)
        was, @unconfirmed = @unconfirmed, {}
        was.each_value do |outcome|
          if !outcome.sent
            @unconfirmed[outcome.tag] = outcome
          else
            @returnable.delete(outcome.publish_id) if outcome.publish_id
            outcome.fail!(error)
          end
        end
        made_room
      end

      # Runs for the life of the publisher: each time the channel closes,
      # attach to another.
      def keep
        loop do
          error = @lost.wait
          return if @closed

          Console.debug(self, "Publisher lost its channel; attaching again", exception: error)
          attach
        end
      rescue Error => error
        # The client was closed, or gave up reconnecting.
        finish(error)
      end

      # Open a channel, turn confirms on, send what is waiting to go, and only
      # then let publishers onto it. Keeps at it for as long as the
      # client has, or will have, a connection; raises once it will not.
      # +first+ is the attempt made when the publisher is created, where a
      # refusal is the caller's to hear about.
      def attach(first: false)
        delay = 0.05
        loop do
          @lost   = Async::Promise.new
          channel = nil
          begin
            channel = @client.open_channel(self)
            channel.call(P::Confirm::Select.encode(channel.id, false), P::Confirm::SelectOk)
            replay(channel)
          rescue Error => error
            raise if @closed || @client.closed? || @client.failure
            raise if first && !error.is_a?(ConnectionError)

            channel&.close
            Console.debug(self, "Publisher could not attach; trying again", exception: error)
            sleep(delay)
            delay = [delay * 2, 1].min
            next
          end
          # Closed already, by something the replay provoked: start over.
          next unless channel.open?

          @channel = channel
          @ready.resolve(channel)
          return
        end
      end

      # Send, in order and under new tags, what the last channel left behind:
      # the messages that never went out on it and, for a publisher that
      # resends, the ones that did and were not answered for.
      def replay(channel)
        pending      = @unconfirmed.values
        @unconfirmed = {}
        @tag         = 0
        taken        = 0
        pending.each { |outcome| outcome.sent = false }
        pending.each do |outcome|
          taken += 1
          next unless outcome.pending?

          @unconfirmed[@tag += 1] = outcome
          outcome.tag = @tag
          write_message(channel, outcome)
        end
        Console.info(self, "Sent #{@unconfirmed.size} waiting message(s) on the new channel") unless pending.empty?
      ensure
        # Cut short: what was not reached goes back, after what was, for the
        # next attempt. The tags are placeholders until then.
        pending&.drop(taken)&.each { |outcome| @unconfirmed[@tag += 1] = outcome.tap { |o| o.tag = @tag } }
      end

      # The one place a publisher ends.
      def finish(error)
        @closed = true
        keeper, @keeper = @keeper, nil
        channel, @channel = @channel, nil
        pending, @unconfirmed = @unconfirmed, {}
        @returnable = {}
        pending.each_value { |outcome| outcome.fail!(error) }
        @ready.reject(error) unless @ready.resolved?
        room, @room = @room, nil
        room&.reject(error)
        @lost&.resolve(error) unless @lost&.resolved?
        @client.unregister(self)
        begin
          channel&.close
        rescue Error
          # Gone with its connection.
        end
        return unless keeper && !keeper.equal?(Async::Task.current?)

        keeper.respond_to?(:cancel) ? keeper.cancel : keeper.stop
      end

      def closed_error
        ClosedError.new("This publisher has been closed")
      end
    end
  end
end
