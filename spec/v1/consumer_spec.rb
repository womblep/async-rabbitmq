# frozen_string_literal: true

require_relative "helper"

# The consumers against a real broker: Consumer, which is pulled from and
# settles nothing for you, and ManagedConsumer, which runs handlers
# concurrently and settles for them. What matters most here is what only a
# broker can show: where a message is after each way of stopping.
RSpec.describe "Consumers", :integration do
  def with_client(port: RABBITMQ_PORT, endpoint_options: {}, **options)
    with_vhost do |vhost|
      Async::RabbitMQ::Client.open(endpoint(vhost: vhost, port: port, **endpoint_options),
                                   reconnect_interval: 0.1, **options) do |client|
        yield client, vhost
      end
    end
  end

  def current_connection(client)
    client.instance_variable_get(:@connection)
  end

  def reconnected(client, first)
    wait_for { client.connected? && !current_connection(client).equal?(first) }
  end

  # A queue with +count+ messages m0, m1, ... in it.
  def queue_with(client, count, name: "work", **options)
    queue = client.queue(name, **options)
    publish(client, queue, count)
    queue
  end

  def publish(client, queue, count, prefix: "m")
    client.publisher { |publisher| count.times { |i| publisher.publish("#{prefix}#{i}", routing_key: queue) } }
  end

  describe Async::RabbitMQ::Consumer do
    it "yields messages in order in the calling fiber, and returns :stopped after stop" do
      with_client do |client, _|
        queue    = queue_with(client, 5)
        consumer = queue.consumer(prefetch: 2)
        caller_fiber = Fiber.current
        seen = []

        result = consumer.each do |message|
          expect(Fiber.current).to equal(caller_fiber)
          seen << message.body
          message.ack
          consumer.stop if seen.size == 5
        end

        expect(result).to eq(:stopped)
        expect(seen).to eq(%w[m0 m1 m2 m3 m4])
        expect(consumer).not_to be_running
        expect(consumer.consumer_tag).to be_nil
        expect(queue.consumer_count).to eq(0)
        expect(queue.message_count).to eq(0)
      end
    end

    it "gives a message its properties" do
      with_client do |client, _|
        events = client.exchange("events", :topic)
        queue  = client.queue("work").bind(events, routing_key: "order.*")
        events.publisher do |publisher|
          publisher.publish("body", routing_key: "order.created", content_type: "text/plain",
                                    headers: { "tenant" => "acme" }, correlation_id: "c-1", reply_to: "replies")
        end

        queue.consumer do |consumer|
          consumer.each do |message|
            expect(message.body).to eq("body")
            expect(message.exchange).to eq("events")
            expect(message.routing_key).to eq("order.created")
            expect(message.content_type).to eq("text/plain")
            expect(message.headers).to include("tenant" => "acme")
            expect(message.published_at).to be_within(5).of(Time.now)
            expect(message.elapsed).to be_between(0, 5)
            expect(message.correlation_id).to eq("c-1")
            expect(message.reply_to).to eq("replies")
            expect(message.message_id).to be_nil
            expect(message).to be_persistent
            expect(message).not_to be_redelivered
            expect(message.consumer_tag).to eq(consumer.consumer_tag)
            expect(message.ack).to be true
            expect(message.settlement).to eq(:ack)
            expect(message.nack).to be false # the first settlement stands
            consumer.stop
          end
        end
      end
    end

    it "tells how long ago a message was published, as well as its publisher said" do
      with_client do |client, _|
        queue = client.queue("work")
        now   = (Time.now.to_f * 1000).round
        client.publisher do |publisher|
          publisher.publish("ms", routing_key: queue, headers: { "timestamp_in_ms" => now - 1500 })
          publisher.publish("future", routing_key: queue, headers: { "timestamp_in_ms" => now + 60_000 })
        end
        client.publisher(timestamps: false) do |publisher|
          publisher.publish("seconds", routing_key: queue, timestamp: (now / 1000) - 10)
          publisher.publish("none", routing_key: queue)
        end

        seen = {}
        queue.consumer do |consumer|
          consumer.each do |message|
            seen[message.body] = [message.elapsed, message.published_at]
            message.ack
            consumer.stop if seen.size == 4
          end
        end
        expect(seen["ms"].first).to be_between(1.5, 4)
        expect(seen["ms"].last).to be_a(Time)
        expect(seen["future"].first).to eq(0.0) # the publisher's clock is ahead; never negative
        expect(seen["seconds"].first).to be_between(10, 14)
        expect(seen["none"]).to eq([nil, nil])
      end
    end

    it "supports ack, nack and reject, each doing what it says" do
      with_client do |client, _|
        queue      = queue_with(client, 3)
        consumer   = queue.consumer(prefetch: 1)
        deliveries = []

        consumer.each do |message|
          deliveries << [message.body, message.redelivered?]
          case deliveries.size
          when 1 then message.nack(requeue: true)       # m0 comes back
          when 2 then message.reject(requeue: false)    # dropped, whichever it is
          else message.ack
          end
          consumer.stop if deliveries.size == 4
        end

        expect(deliveries.size).to eq(4)
        expect(deliveries.count { |_, redelivered| redelivered }).to eq(1)
        expect(queue.message_count).to eq(0)
      end
    end

    it "leaves a message the block did not settle unacknowledged, to be settled later or released by close" do
      with_client do |client, _|
        queue    = queue_with(client, 2)
        consumer = queue.consumer(prefetch: 1)
        held     = []
        2.times do
          consumer.each do |message|
            held << message
            consumer.stop
          end
        end

        expect(held.map(&:body)).to eq(%w[m0 m1])
        expect(held).to all(satisfy { |message| !message.settled? })
        expect(queue.message_count).to eq(0) # still out with this consumer
        expect(held.first.ack).to be true    # after each has ended

        consumer.close
        wait_for { queue.message_count == 1 }
        expect(held.last.ack).to be false
        expect(held.last.settlement).to eq(:stale)
      end
    end

    it "puts back what had arrived but was not handed out when it stops" do
      with_client do |client, _|
        queue    = queue_with(client, 5)
        consumer = queue.consumer(prefetch: 5)

        consumer.each do |message|
          wait_for { consumer.instance_variable_get(:@buffer).size == 4 }
          message.ack
          consumer.stop
        end

        # Nacked back, not released by a close: the consumer is still open.
        wait_for { queue.message_count == 4 }
        expect(consumer).not_to be_closed
      end
    end

    it "raises the block's exception from each, cancels, and puts the rest back" do
      with_client do |client, _|
        queue    = queue_with(client, 3)
        consumer = queue.consumer(prefetch: 3)

        expect do
          consumer.each do |message|
            message.ack
            raise ArgumentError, "bad order"
          end
        end.to raise_error(ArgumentError, "bad order")

        expect(consumer).not_to be_running
        wait_for { queue.message_count == 2 }
        expect(queue.consumer_count).to eq(0)
      end
    end

    it "ends on a break, cancelling the consumer" do
      with_client do |client, _|
        queue    = queue_with(client, 3)
        consumer = queue.consumer(prefetch: 1)
        consumer.each do |message|
          message.ack
          break
        end

        expect(consumer).not_to be_running
        wait_for { queue.message_count == 2 }
        expect(queue.consumer_count).to eq(0)
      end
    end

    it "returns :cancelled when the queue is deleted" do
      with_client do |client, vhost|
        queue    = queue_with(client, 0)
        consumer = queue.consumer
        running  = Async { consumer.each(&:ack) }
        wait_for { consumer.consumer_tag }

        http_delete("/api/queues/#{URI.encode_www_form_component(vhost)}/work")
        expect(running.wait).to eq(:cancelled)
        expect(consumer).not_to be_running
      end
    end

    it "raises ChannelError for a queue that does not exist, and when the broker closes its channel" do
      with_client do |client, _|
        expect { client.consumer("nowhere").each { nil } }
          .to raise_error(Async::RabbitMQ::ChannelError) { |error| expect(error).to be_not_found }

        queue    = queue_with(client, 1)
        consumer = queue.consumer
        expect do
          consumer.each do |message|
            # Not something Message allows: an ack the broker knows nothing of.
            channel = consumer.instance_variable_get(:@channel)
            channel.write(AMQ::Protocol::Basic::Ack.encode(channel.id, message.delivery_tag + 100, false).encode)
            sleep 0.3
          end
        end.to raise_error(Async::RabbitMQ::ChannelError) { |error| expect(error).to be_precondition_failed }
        expect(consumer).not_to be_running
        wait_for { queue.message_count == 1 }
      end
    end

    it "cancels the consumer when the task calling each is stopped" do
      with_client do |client, _|
        queue    = queue_with(client, 3)
        consumer = queue.consumer(prefetch: 3)
        got      = []
        task     = Async do
          consumer.each do |message|
            got << message
            sleep
          end
        end
        wait_for { got.size == 1 }
        task.stop
        task.wait

        expect(consumer).not_to be_running
        wait_for { queue.consumer_count.zero? }
        wait_for { queue.message_count == 2 } # the one in the block is still held
        expect(got.first.ack).to be true
      end
    end

    it "can be run again after it has ended, and refuses a second each while one is running" do
      with_client do |client, _|
        queue    = queue_with(client, 2)
        consumer = queue.consumer(prefetch: 1)
        bodies   = []
        2.times do
          consumer.each do |message|
            bodies << message.body
            message.ack
            expect { consumer.each { nil } }.to raise_error(Async::RabbitMQ::Error, /already running/)
            consumer.stop
          end
        end
        expect(bodies).to eq(%w[m0 m1])
      end
    end

    it "rejects settings that make no sense" do
      with_client do |client, _|
        expect { client.consumer("q", prefetch: 0) }.to raise_error(ArgumentError, /prefetch/)
        expect { client.consumer("q", on_connection_loss: :panic) }.to raise_error(ArgumentError, /on_connection_loss/)
        expect { client.consumer(nil, exclusive: false) }.to raise_error(ArgumentError, /name/)
        expect { client.consumer("q").bind("x") }.to raise_error(Async::RabbitMQ::Error, /Queue#bind/)
      end
    end

    describe "when the connection is lost" do
      it "keeps consuming on the next connection, and does not yield what the broker took back" do
        with_client do |client, _|
          queue    = queue_with(client, 4)
          consumer = queue.consumer(prefetch: 4)
          first    = current_connection(client)
          seen     = []
          stale    = nil

          result = consumer.each do |message|
            seen << [message.body, message.redelivered?]
            if seen.size == 1
              # Three more are waiting in the buffer when the connection goes.
              wait_for { consumer.instance_variable_get(:@buffer).size == 3 }
              sever!(first)
              reconnected(client, first)
              stale = message
              expect(message).to be_stale
              expect(message.ack).to be false
            else
              message.ack
            end
            consumer.stop if seen.size == 5
          end

          expect(result).to eq(:stopped)
          # m0 once on the old connection, then all four again from the broker.
          expect(seen.first).to eq(["m0", false])
          expect(seen.drop(1).map(&:first)).to eq(%w[m0 m1 m2 m3])
          expect(seen.drop(1).map(&:last)).to all(be true)
          expect(stale.settlement).to eq(:stale)
          expect(queue.message_count).to eq(0)
        end
      end

      it "waits through an outage" do
        skip "toxiproxy not available" unless toxiproxy_available?

        with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
          queue    = queue_with(client, 0)
          consumer = queue.consumer
          seen     = []
          running  = Async do
            consumer.each do |message|
              seen << message.body
              message.ack
              consumer.stop
            end
          end
          wait_for { consumer.consumer_tag }
          toxiproxy_rabbitmq.down do
            wait_for { !client.connected? }
            sleep 0.4
            expect(running).to be_running
          end
          publish(client, queue, 1)

          expect(running.wait).to eq(:stopped)
          expect(seen).to eq(%w[m0])
        end
      end

      it "returns :connection_lost with on_connection_loss: :end, and can be run again" do
        with_client do |client, _|
          queue    = queue_with(client, 1)
          consumer = queue.consumer(on_connection_loss: :end)
          first    = current_connection(client)

          result = consumer.each { |_message| sever!(first) }
          expect(result).to eq(:connection_lost)
          expect(consumer).not_to be_running

          seen = []
          consumer.each do |message|
            seen << [message.body, message.redelivered?]
            message.ack
            consumer.stop
          end
          expect(seen).to eq([["m0", true]])
        end
      end

      it "raises ConnectionError when the client gives up" do
        skip "toxiproxy not available" unless toxiproxy_available?

        with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }, reconnect_attempts: 2) do |client, _|
          consumer = queue_with(client, 0).consumer
          running  = Async do
            consumer.each(&:ack)
          rescue Async::RabbitMQ::Error => error
            error
          end
          wait_for { consumer.consumer_tag }
          toxiproxy_rabbitmq.down do
            expect(running.wait).to be_a(Async::RabbitMQ::ConnectionError)
          end
        end
      end

      it "returns :cancelled if the queue is gone when it comes back" do
        with_client do |client, vhost|
          client.queue("work")
          client.forget("work")
          consumer = client.consumer("work")
          first    = current_connection(client)
          running  = Async do
            consumer.each(&:ack)
          rescue Async::RabbitMQ::Error => error
            error
          end
          wait_for { consumer.consumer_tag }
          # The cancel this sends is overtaken by the cut.
          consumer.define_singleton_method(:cancelled) { |_method| nil }
          http_delete("/api/queues/#{URI.encode_www_form_component(vhost)}/work")
          sever!(first)

          expect(running.wait).to eq(:cancelled)
          expect(consumer).not_to be_running
        end
      end
    end

    describe "when the broker cancels it, and it was asked to resubscribe" do
      # What the broker does when it cancels a consumer and leaves the queue:
      # the consumer is gone at the broker, and the client is told.
      def broker_cancels(consumer)
        channel = consumer.instance_variable_get(:@channel)
        channel.call(AMQ::Protocol::Basic::Cancel.encode(channel.id, consumer.consumer_tag, false),
                     AMQ::Protocol::Basic::CancelOk)
        consumer.cancelled(nil)
      end

      it "consumes again from a queue that is still there, and declares nothing" do
        with_client do |client, _|
          queue    = queue_with(client, 1)
          consumer = queue.consumer(on_cancel: :resubscribe)
          got      = []
          told     = []
          consumer.on_cancelled { |c| told << [:cancelled, c, c.consumer_tag] }
          consumer.on_resubscribed { |c| told << [:resubscribed, c, c.consumer_tag.nil?] }
          running  = Async do
            consumer.each do |message|
              got << message.body
              message.ack
            end
          end
          wait_for { got == %w[m0] }
          first_tag = consumer.consumer_tag
          broker_cancels(consumer)
          wait_for { consumer.consumer_tag && consumer.consumer_tag != first_tag }
          publish(client, queue, 1, prefix: "again")

          wait_for { got == %w[m0 again0] }
          expect(running).to be_running
          expect(told).to eq([[:cancelled, consumer, first_tag], [:resubscribed, consumer, false]])
          expect(consumer.on_cancel).to eq(:resubscribe)
          consumer.stop
          expect(running.wait).to eq(:stopped)
        end
      end

      it "returns :cancelled when the queue has been deleted" do
        with_client do |client, vhost|
          queue    = queue_with(client, 0)
          consumer = queue.consumer(on_cancel: :resubscribe)
          told     = []
          consumer.on_cancelled { told << :cancelled }
          consumer.on_resubscribed { told << :resubscribed }
          running  = Async { consumer.each(&:ack) }
          wait_for { consumer.consumer_tag }

          http_delete("/api/queues/#{URI.encode_www_form_component(vhost)}/work")
          expect(running.wait).to eq(:cancelled)
          expect(told).to eq([:cancelled])
          expect(client.queue?("work")).to be(false)
        end
      end

      it "keeps trying while the broker refuses, and can be stopped while it waits" do
        with_client do |client, _|
          queue    = queue_with(client, 0)
          consumer = queue.consumer(on_cancel: :resubscribe)
          got      = []
          running  = Async do
            consumer.each do |message|
              got << message.body
              message.ack
            end
          end
          wait_for { consumer.consumer_tag }

          # Someone else takes the queue for themselves, so consuming again
          # is refused for as long as they hold it.
          channel = consumer.instance_variable_get(:@channel)
          channel.call(AMQ::Protocol::Basic::Cancel.encode(channel.id, consumer.consumer_tag, false),
                       AMQ::Protocol::Basic::CancelOk)
          other = client.open_channel
          taken = other.call(AMQ::Protocol::Basic::Consume.encode(other.id, "work", "", false, false, true, false, {}),
                             AMQ::Protocol::Basic::ConsumeOk)
          consumer.cancelled(nil)
          sleep 0.3
          expect(running).to be_running
          expect(consumer.consumer_tag).to be_nil

          other.call(AMQ::Protocol::Basic::Cancel.encode(other.id, taken.consumer_tag, false),
                     AMQ::Protocol::Basic::CancelOk)
          publish(client, queue, 1)
          wait_for(timeout: 5) { got == %w[m0] }

          # And again, this time stopped in the middle of the wait.
          channel = consumer.instance_variable_get(:@channel)
          channel.call(AMQ::Protocol::Basic::Cancel.encode(channel.id, consumer.consumer_tag, false),
                       AMQ::Protocol::Basic::CancelOk)
          other.call(AMQ::Protocol::Basic::Consume.encode(other.id, "work", "", false, false, true, false, {}),
                     AMQ::Protocol::Basic::ConsumeOk)
          consumer.cancelled(nil)
          sleep 0.3
          started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          consumer.stop
          expect(running.wait).to eq(:stopped)
          expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 0.5
        end
      end

      it "is an option of a managed consumer too, and only :end or :resubscribe" do
        with_client do |client, _|
          queue   = queue_with(client, 0)
          managed = queue.managed_consumer(on_cancel: :resubscribe)
          got     = []
          told    = []
          managed.on_cancelled { |c| told << c }.on_resubscribed { |c| told << c }
          running = Async { managed.run { |message| got << message.body } }
          wait_for { managed.consumer_tag }
          first_tag = managed.consumer_tag
          broker_cancels(managed.instance_variable_get(:@consumer))
          wait_for { managed.consumer_tag && managed.consumer_tag != first_tag }
          publish(client, queue, 1)

          wait_for { got == %w[m0] }
          expect(managed.on_cancel).to eq(:resubscribe)
          expect(told).to eq([managed, managed])
          managed.stop
          expect(running.wait).to eq(:stopped)
          expect { client.consumer("work", on_cancel: :retry) }.to raise_error(ArgumentError, /on_cancel/)
        end
      end
    end

    describe "#close" do
      it "ends a running each with :closed, and puts back what it held" do
        with_client do |client, _|
          queue    = queue_with(client, 3)
          consumer = queue.consumer(prefetch: 3)
          got      = []
          running  = Async do
            consumer.each do |message|
              got << message.body
              sleep 0.2
            end
          end
          wait_for { got.size == 1 }
          consumer.close

          expect(running.wait).to eq(:closed)
          expect(consumer).to be_closed
          wait_for { queue.message_count == 3 }
          expect { consumer.each { nil } }.to raise_error(Async::RabbitMQ::ClosedError)
          consumer.close # twice is harmless
        end
      end

      it "is closed with its client, and by the block form" do
        held = nil
        running = nil
        with_client do |client, _|
          queue = queue_with(client, 0)
          inner = queue.consumer { |consumer| consumer }
          expect(inner).to be_closed
          held    = queue.consumer
          running = Async { held.each(&:ack) }
          wait_for { held.consumer_tag }
        end
        expect(held).to be_closed
        expect(running.wait).to eq(:closed)
      end

      it "gives its channel back" do
        with_client do |client, _|
          queue    = queue_with(client, 0)
          before   = current_connection(client).channel_count
          consumer = queue.consumer
          expect(current_connection(client).channel_count).to eq(before + 1)
          consumer.close
          expect(current_connection(client).channel_count).to eq(before)
        end
      end

      it "ends an each that is waiting for a connection, once the client closes" do
        skip "toxiproxy not available" unless toxiproxy_available?

        with_client(port: TOXIPROXY_PORT, endpoint_options: { timeout: 1 }) do |client, _|
          consumer = queue_with(client, 0).consumer
          running  = Async { consumer.each(&:ack) }
          wait_for { consumer.consumer_tag }
          toxiproxy_rabbitmq.down do
            wait_for { !client.connected? }
            sleep 0.2
            client.close
            expect(running.wait).to eq(:closed)
          end
        end
      end
    end

    describe "with a queue of its own" do
      it "declares an exclusive queue the broker names, and consumes what is bound to it" do
        with_client do |client, vhost|
          events   = client.exchange("events", :topic)
          consumer = client.consumer(prefetch: 10).bind(events, routing_key: "order.*")
          expect(consumer).to be_exclusive
          expect(consumer.queue_name).to start_with("amq.gen-")
          info = http_get("/api/queues/#{URI.encode_www_form_component(vhost)}").find { |q| q["name"] == consumer.queue_name }
          wait_for { info ||= http_get("/api/queues/#{URI.encode_www_form_component(vhost)}").find { |q| q["name"] == consumer.queue_name } }
          expect(info["exclusive"]).to be true

          events.publisher do |publisher|
            publisher.publish("yes", routing_key: "order.created")
            publisher.publish("no", routing_key: "invoice.created")
            publisher.publish("direct", exchange: "", routing_key: consumer.queue_name)
          end
          seen = []
          consumer.each do |message|
            seen << message.body
            message.ack
            consumer.stop if seen.size == 2
          end
          expect(seen).to eq(%w[yes direct])
          expect(client.topology).not_to be_empty
          expect(client.topology.queues.map(&:name)).not_to include(consumer.queue_name)
        end
      end

      it "takes a name, and arguments for the queue" do
        with_client do |client, vhost|
          consumer = client.consumer("replies.mine", exclusive: true, queue_arguments: { "x-max-length" => 10 })
          expect(consumer.queue_name).to eq("replies.mine")
          info = nil
          wait_for { info = http_get("/api/queues/#{URI.encode_www_form_component(vhost)}").find { |q| q["name"] == "replies.mine" } }
          expect(info["exclusive"]).to be true
          expect(info["arguments"]).to include("x-max-length" => 10)

          # Another client cannot have it.
          Async::RabbitMQ::Client.open(endpoint(vhost: vhost)) do |other|
            expect { other.consumer("replies.mine", exclusive: true) }
              .to raise_error(Async::RabbitMQ::ChannelError) { |error| expect(error).to be_resource_locked }
          end
        end
      end

      it "declares and binds it again on the next connection, and says so" do
        with_client do |client, _|
          events   = client.exchange("events", :fanout)
          consumer = client.consumer.bind(events)
          first    = current_connection(client)
          original = consumer.queue_name
          attached = []
          consumer.on_attach { |c| attached << c.queue_name }
          seen     = []
          running  = Async do
            consumer.each do |message|
              seen << message.body
              message.ack
            end
          end
          wait_for { consumer.consumer_tag }
          sever!(first)
          wait_for { attached.size == 1 }
          wait_for { consumer.consumer_tag }

          expect(consumer.queue_name).not_to eq(original)
          expect(attached).to eq([consumer.queue_name])
          expect(client.queue?(original)).to be false # went with its connection
          events.publisher { |publisher| publisher.publish("after") }
          wait_for { seen == %w[after] }
          consumer.stop
          expect(running.wait).to eq(:stopped)
        end
      end

      it "keeps the queue between runs on one connection" do
        with_client do |client, _|
          consumer = client.consumer
          name     = consumer.queue_name
          client.publisher { |publisher| 2.times { |i| publisher.publish("m#{i}", routing_key: name) } }
          seen = []
          2.times do
            consumer.each do |message|
              seen << message.body
              message.ack
              consumer.stop
            end
          end
          expect(seen).to eq(%w[m0 m1])
          expect(consumer.queue_name).to eq(name)
        end
      end

      it "undoes a binding, and refuses one the broker refuses" do
        with_client do |client, _|
          events   = client.exchange("events", :fanout)
          consumer = client.consumer.bind(events)
          expect { consumer.bind("no-such-exchange") }.to raise_error(Async::RabbitMQ::ChannelError)
          # The refusal closed the channel; the consumer opens another, with its queue and first binding.
          consumer.unbind(events)
          events.publisher { |publisher| publisher.publish("unheard") }
          expect(client.queue_status(consumer.queue_name).message_count).to eq(0)
        end
      end
    end
  end

  describe Async::RabbitMQ::ManagedConsumer do
    it "acks each message when its handler returns" do
      with_client do |client, _|
        queue    = queue_with(client, 5)
        consumer = queue.managed_consumer
        seen     = []
        result   = consumer.run do |message|
          seen << message.body
          consumer.stop if seen.size == 5
        end

        expect(result).to eq(:stopped)
        expect(seen).to eq(%w[m0 m1 m2 m3 m4])
        expect(queue.message_count).to eq(0)
        expect(consumer).not_to be_running
        expect(consumer.prefetch).to eq(1)
        expect(consumer).to be_ack_on_success
      end
    end

    it "runs up to concurrency handlers at once, and no more" do
      with_client do |client, _|
        queue    = queue_with(client, 12)
        consumer = queue.managed_consumer(concurrency: 4)
        running  = 0
        peak     = 0
        done     = 0
        consumer.run do |_message|
          running += 1
          peak = [peak, running].max
          sleep 0.05
          running -= 1
          done += 1
          consumer.stop if done == 12
        end

        expect(peak).to eq(4)
        expect(queue.message_count).to eq(0)
      end
    end

    it "nacks without requeue when the handler raises, and carries on" do
      with_client do |client, _|
        client.queue("dead")
        queue    = queue_with(client, 3, arguments: { "x-dead-letter-exchange" => "",
                                                       "x-dead-letter-routing-key" => "dead" })
        consumer = queue.managed_consumer
        errors   = []
        consumer.on_error { |error, message| errors << [error.message, message.body] }
        seen = 0
        consumer.run do |message|
          seen += 1
          consumer.stop if seen == 3
          raise "boom" if message.body == "m1"
        end

        expect(errors).to eq([%w[boom m1]])
        expect(queue.message_count).to eq(0)
        wait_for { client.queue_status("dead").message_count == 1 }
      end
    end

    it "requeues on an exception when told to, and the message comes back" do
      with_client do |client, _|
        queue    = queue_with(client, 1)
        consumer = queue.managed_consumer(requeue: true)
        attempts = []
        consumer.run do |message|
          attempts << message.redelivered?
          raise "first time" if attempts.size == 1

          consumer.stop
        end
        expect(attempts).to eq([false, true])
        expect(queue.message_count).to eq(0)
      end
    end

    it "acks a message whose handler raised when exception: :ack, and rejects with :reject" do
      with_client do |client, _|
        queue = queue_with(client, 1)
        queue.managed_consumer(exception: :ack) do |consumer|
          consumer.run do |_message|
            consumer.stop
            raise "ignored"
          end
        end
        expect(queue.message_count).to eq(0)

        publish(client, queue, 1)
        settled = nil
        queue.managed_consumer(exception: :reject) do |consumer|
          consumer.on_error { |_error, message| settled = message }
          consumer.run do |_message|
            consumer.stop
            raise "rejected"
          end
        end
        expect(settled.settlement).to eq(:reject)
        expect(queue.message_count).to eq(0)
      end
    end

    it "lets the handler's own settlement, and on_error's, stand" do
      with_client do |client, _|
        queue    = queue_with(client, 2)
        consumer = queue.managed_consumer
        seen     = []
        consumer.on_error { |_error, message| message.ack }
        consumer.run do |message|
          seen << message
          consumer.stop if seen.size == 3
          if message.body == "m0" && !message.redelivered?
            message.nack(requeue: true)
          elsif message.body == "m1"
            raise "acked by on_error"
          end
        end

        # With a prefetch of one, the message put back is the next one sent.
        expect(seen.map(&:body)).to eq(%w[m0 m0 m1])
        expect(seen.map(&:settlement)).to eq(%i[nack ack ack])
        expect(queue.message_count).to eq(0)
      end
    end

    it "leaves the message unacknowledged with ack_on_success: false" do
      with_client do |client, _|
        queue    = queue_with(client, 1)
        consumer = queue.managed_consumer(ack_on_success: false)
        held     = nil
        consumer.run do |message|
          held = message
          consumer.stop
        end

        expect(held).not_to be_settled
        expect(queue.message_count).to eq(0)
        expect(held.ack).to be true
      end
    end

    it "drains on stop: running handlers finish and are acked, the rest go back" do
      with_client do |client, _|
        queue    = queue_with(client, 6)
        consumer = queue.managed_consumer(concurrency: 2, prefetch: 6)
        started  = []
        finished = []
        result   = consumer.run do |message|
          started << message.body
          consumer.stop if started.size == 2
          sleep 0.2
          finished << message.body
        end

        expect(result).to eq(:stopped)
        expect(finished).to match_array(%w[m0 m1])
        expect(started.size).to eq(2)
        wait_for { queue.message_count == 4 }
      end
    end

    it "stops handlers that outlast drain_timeout and puts their messages back" do
      with_client do |client, _|
        queue    = queue_with(client, 1)
        consumer = queue.managed_consumer(drain_timeout: 0.2)
        began    = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        consumer.run do |_message|
          consumer.stop
          sleep 30
        end

        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - began).to be < 5
        wait_for { queue.message_count == 1 }
      end
    end

    it "drains when the task running it is stopped" do
      with_client do |client, _|
        queue    = queue_with(client, 4)
        consumer = queue.managed_consumer(concurrency: 2, prefetch: 4)
        started  = []
        finished = []
        task     = Async do
          consumer.run do |message|
            started << message.body
            sleep 0.2
            finished << message.body
          end
        end
        wait_for { started.size == 2 }
        task.stop
        task.wait

        expect(finished).to match_array(%w[m0 m1])
        wait_for { queue.message_count == 2 }
        expect(queue.consumer_count).to eq(0)
      end
    end

    it "drains when it is closed, and when its client is" do
      with_client do |client, _|
        queue    = queue_with(client, 2)
        consumer = queue.managed_consumer(prefetch: 2)
        finished = []
        running  = Async do
          consumer.run do |message|
            sleep 0.2
            finished << message.body
          end
        end
        wait_for { consumer.consumer_tag }
        sleep 0.05
        consumer.close

        expect(finished).to eq(%w[m0])
        expect(running.wait).to eq(:closed)
        wait_for { queue.message_count == 1 }
        expect { consumer.run { nil } }.to raise_error(Async::RabbitMQ::ClosedError)

        other    = queue.managed_consumer
        finished = []
        running  = Async do
          other.run do |message|
            sleep 0.2
            finished << message.body
          end
        end
        wait_for { other.consumer_tag }
        sleep 0.05
        client.close
        expect(finished).to eq(%w[m1])
        expect(running.wait).to eq(:closed)
      end
    end

    it "keeps running across a lost connection, and does not run handlers for what was waiting" do
      with_client do |client, _|
        queue    = queue_with(client, 4)
        consumer = queue.managed_consumer(prefetch: 4)
        first    = current_connection(client)
        runs     = []
        result   = consumer.run do |message|
          runs << [message.body, message.redelivered?]
          if runs.size == 1
            sever!(first)
            reconnected(client, first)
          end
          consumer.stop if runs.size == 5
        end

        expect(result).to eq(:stopped)
        # m0 ran on the old connection; m1..m3 were waiting there and did not.
        expect(runs.first).to eq(["m0", false])
        expect(runs.drop(1)).to eq([["m0", true], ["m1", true], ["m2", true], ["m3", true]])
        expect(queue.message_count).to eq(0)
      end
    end

    it "reuses its handler fibers and never holds more than concurrency of them" do
      with_client do |client, _|
        queue    = queue_with(client, 40)
        consumer = queue.managed_consumer(concurrency: 3)
        fibers   = []
        count    = 0
        consumer.run do |_message|
          fibers << Fiber.current
          count += 1
          consumer.stop if count == 40
        end
        expect(fibers.uniq.size).to be <= 3
        expect(queue.message_count).to eq(0)
      end
    end

    it "refuses a second run while one is running, without disturbing it" do
      with_client do |client, _|
        queue    = queue_with(client, 1)
        consumer = queue.managed_consumer
        result   = consumer.run do |_message|
          expect { consumer.run { nil } }.to raise_error(Async::RabbitMQ::Error, /already running/)
          expect(consumer).to be_running
          consumer.stop
        end
        expect(result).to eq(:stopped)
        expect(queue.message_count).to eq(0)
      end
    end

    it "consumes a queue of its own" do
      with_client do |client, _|
        events   = client.exchange("events", :fanout)
        consumer = client.managed_consumer(concurrency: 2).bind(events)
        expect(consumer).to be_exclusive
        events.publisher { |publisher| 3.times { |i| publisher.publish("e#{i}") } }
        seen = []
        consumer.run do |message|
          seen << message.body
          consumer.stop if seen.size == 3
        end
        expect(seen).to match_array(%w[e0 e1 e2])
      end
    end

    it "rejects settings that make no sense" do
      with_client do |client, _|
        expect { client.managed_consumer("q", concurrency: 0) }.to raise_error(ArgumentError, /concurrency/)
        expect { client.managed_consumer("q", exception: :explode) }.to raise_error(ArgumentError, /exception/)
        expect { client.managed_consumer("q", drain_timeout: -1) }.to raise_error(ArgumentError, /drain_timeout/)
      end
    end
  end
end
