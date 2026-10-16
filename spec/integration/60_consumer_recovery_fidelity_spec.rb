require "spec_helper"

# Re-registering a consumer after a reconnect or a channel reopen has to
# reproduce the consumer it replaces, not a default-valued lookalike. Whatever
# basic_consume does not keep is lost on the first recovery, and lost quietly:
# the consumer comes back and keeps delivering, so nothing announces that it is
# no longer the consumer that was asked for.
RSpec.describe "consumer recovery fidelity", :integration do

  def wait_until(timeout = 15)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  def esc(name)
    URI.encode_www_form_component(name)
  end

  # The broker's own record of a consumer: the only place the arguments it was
  # registered with can be read back.
  def broker_consumer(vhost, tag)
    http_get("/api/consumers/#{esc(vhost)}").find { |c| c["consumer_tag"] == tag }
  end

  # nil while the broker has not published the queue's stats yet, which is not
  # the same as a count of zero: compare rather than coerce, so an unpublished
  # count keeps the caller waiting instead of reading as "no consumers".
  def consumers_on(vhost, queue, count)
    http_get("/api/queues/#{esc(vhost)}/#{esc(queue)}")["consumers"] == count
  end

  def close_by_broker(ch)
    ch.basic_ack(4242)            # unknown delivery tag: 406, the broker closes the channel
    wait_until { ch.closed? }
  end

  def consume_with(ch, queue, **opts)
    ch.basic_consume(queue, **opts) { |_di, _h, _b| }
  end

  describe "consumer arguments" do
    it "survive a reconnect" do
      isolated_session do |session, vhost|
        ch  = session.open_channel
        q   = ch.queue("fidelity.args.#{SecureRandom.hex(4)}", durable: true)
        tag = consume_with(ch, q.name, arguments: { "x-priority" => 10 })
        wait_until { broker_consumer(vhost, tag) }
        before = broker_consumer(vhost, tag)["channel_details"]["connection_name"]

        recover_connection!(session)

        # Insist on the re-registered consumer's record rather than the dead
        # connection's, which can still be draining out of the stats and would
        # report the arguments of the consumer that is already gone.
        record = nil
        wait_until do
          record = broker_consumer(vhost, tag)
          record && record["channel_details"]["connection_name"] != before
        end
        expect(record["arguments"]).to eq("x-priority" => 10)
        ch.close
      end
    end

    it "survive a channel reopen" do
      isolated_session do |session, vhost|
        ch  = session.open_channel
        q   = ch.queue("fidelity.args.#{SecureRandom.hex(4)}", durable: true)
        tag = consume_with(ch, q.name, arguments: { "x-priority" => 10 })
        wait_until { broker_consumer(vhost, tag) }

        close_by_broker(ch)
        wait_until { broker_consumer(vhost, tag).nil? }   # the old record is gone first
        ch.reopen(recover_consumers: true)

        wait_until { broker_consumer(vhost, tag) }
        expect(broker_consumer(vhost, tag)["arguments"]).to eq("x-priority" => 10)
        ch.close
      end
    end
  end

  describe "an exclusive consumer" do
    it "is still exclusive after a reconnect" do
      isolated_session do |session, vhost|
        ch = session.open_channel
        q  = ch.queue("fidelity.excl.#{SecureRandom.hex(4)}", durable: true)
        consume_with(ch, q.name, exclusive: true)

        recover_connection!(session)

        # One consumer on the queue, so a refusal below is the re-registered
        # consumer holding it and not the dead connection's still being reaped.
        wait_until { consumers_on(vhost, q.name, 1) }
        other = session.open_channel
        expect {
          consume_with(other, q.name)
        }.to raise_error(AsyncRabbitMQ::ChannelError) do |err|
          expect(err.code).to eq(403)
        end
        ch.close
      end
    end

    it "is still exclusive after a channel reopen" do
      isolated_session do |session, vhost|
        ch = session.open_channel
        q  = ch.queue("fidelity.excl.#{SecureRandom.hex(4)}", durable: true)
        consume_with(ch, q.name, exclusive: true)

        close_by_broker(ch)
        wait_until { consumers_on(vhost, q.name, 0) }
        ch.reopen(recover_consumers: true)
        wait_until { consumers_on(vhost, q.name, 1) }

        other = session.open_channel
        expect {
          consume_with(other, q.name)
        }.to raise_error(AsyncRabbitMQ::ChannelError) do |err|
          expect(err.code).to eq(403)
        end
        ch.close
      end
    end
  end
end
