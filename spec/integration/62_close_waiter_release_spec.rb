require "spec_helper"

# Step 62: every way a channel can close must release every parked waiter.
# A confirm waiter with no timeout (the default) and a publisher parked on an
# outstanding slot have no clock of their own: if the close path forgets to
# signal them, they wait forever.
RSpec.describe "Waiter release on channel close", :integration do

  it "wait_for_confirms raises instead of hanging when the broker closes the channel" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.confirm_select

      # Publish to an exchange that does not exist: the broker answers with a
      # 404 channel.close instead of an ack, so without the fix the untimed
      # wait below parks forever.
      ch.basic_publish("doomed", exchange: "no.such.exchange.#{SecureRandom.hex(4)}", routing_key: "x")

      error = nil
      Async::Task.current.with_timeout(10) do
        begin
          ch.wait_for_confirms
        rescue AsyncRabbitMQ::ChannelError, AsyncRabbitMQ::ConnectionError => e
          error = e
        end
      end

      expect(error).not_to be_nil
      expect(ch).to be_closed
    end
  end

  it "wait_for_confirms raises instead of hanging when another fiber closes the channel" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.close.waiters", durable: true)
      ch.confirm_select

      ch.basic_publish("payload", routing_key: q.name)
      ch.wait_for_confirms # drain the genuine confirm first

      # Park a waiter with nothing left to confirm it, then close under it.
      # Fabricate one outstanding confirm so the wait actually parks.
      ch.instance_variable_get(:@pending_confirms)[999_999] = [+"", "payload", nil]

      error = nil
      waiter = Async::Task.current.async do
        ch.wait_for_confirms
      rescue => e
        error = e
      end

      Async::Task.current.yield until waiter.running?
      ch.close

      Async::Task.current.with_timeout(10) { waiter.wait }
      expect(error).to be_a(AsyncRabbitMQ::NotOpenError)
    ensure
      ch.queue_delete("test.close.waiters") rescue nil
    end
  end

  it "a publisher parked on an outstanding slot is released by a broker channel close" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.close.slots", durable: true)
      ch.confirm_select(tracking: true, outstanding_limit: 1)

      # Fill the single slot with a confirm that will never resolve, so the
      # next publish parks waiting for room.
      ch.instance_variable_get(:@pending_confirms)[999_999] = [+"", "payload", nil]

      error = nil
      publisher = Async::Task.current.async do
        ch.basic_publish("parked", routing_key: q.name)
      rescue => e
        error = e
      end

      Async::Task.current.yield until publisher.running?

      # 404 on a passive declare of a missing queue closes the channel.
      begin
        ch.queue("no.such.queue.#{SecureRandom.hex(4)}", passive: true)
      rescue AsyncRabbitMQ::ChannelError
        # expected
      end

      Async::Task.current.with_timeout(10) { publisher.wait }
      expect(error).to be_a(AsyncRabbitMQ::Error)
    ensure
      begin
        cleanup = session.open_channel
        cleanup.queue_delete("test.close.slots")
        cleanup.close
      rescue StandardError
        nil
      end
    end
  end
end
