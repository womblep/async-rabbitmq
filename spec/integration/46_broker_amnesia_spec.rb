require "spec_helper"

# Severing a connection leaves the broker's state intact, so topology recovery
# only ever re-declares things that still exist and a broken replay would pass
# unnoticed. These tests make the broker actually forget: the exchange, queue
# and binding are deleted through the management API while the client is
# connected, and only then is the connection severed. That is what a broker
# restart looks like to a client, without the cost of restarting a container.
RSpec.describe "recovery against a broker that lost the topology", :integration do

  def delete_queue(vhost, name)
    http_delete("/api/queues/#{URI.encode_www_form_component(vhost)}/#{URI.encode_www_form_component(name)}")
  end

  def delete_exchange(vhost, name)
    http_delete("/api/exchanges/#{URI.encode_www_form_component(vhost)}/#{URI.encode_www_form_component(name)}")
  end

  it "re-declares the exchange, queue and binding, and routes again" do
    isolated_session do |session, vhost|
      suffix   = SecureRandom.hex(4)
      exchange = "test.amnesia.x.#{suffix}"
      queue    = "test.amnesia.q.#{suffix}"
      channel  = session.open_channel
      channel.exchange(exchange, type: :topic, durable: true)
      channel.durable_queue(queue)
      channel.queue_bind(queue, exchange: exchange, routing_key: "rk.#")

      delete_queue(vhost, queue)
      delete_exchange(vhost, exchange)
      expect { channel.queue(queue, passive: true) }.to raise_error(AsyncRabbitMQ::ChannelError)
      channel.reopen # the 404 above closed it

      recover_connection!(session)

      # The topology is back and still wired together.
      expect(channel.queue(queue, passive: true).message_count).to eq(0)
      channel.confirm_select
      channel.basic_publish("after amnesia", exchange: exchange, routing_key: "rk.one")
      channel.wait_for_confirms
      expect(channel.queue(queue, passive: true).message_count).to eq(1)
    end
  end

  it "re-publishes unconfirmed messages after the topology is back, not before" do
    isolated_session do |session, vhost|
      suffix   = SecureRandom.hex(4)
      exchange = "test.amnesia.pending.x.#{suffix}"
      queue    = "test.amnesia.pending.q.#{suffix}"
      channel  = session.open_channel
      channel.exchange(exchange, type: :topic, durable: true)
      channel.durable_queue(queue)
      channel.queue_bind(queue, exchange: exchange, routing_key: "rk")
      channel.confirm_select

      # A message the broker never acked before the connection died. Injected
      # rather than raced: publishing and severing at the right instant is not
      # reproducible, and the point here is the ordering, not the timing.
      encoded = channel.send(:encode_publish, "unconfirmed at drop", exchange: exchange, routing_key: "rk")
      channel.instance_variable_set(:@pending_confirms, {})
      channel.instance_variable_set(:@delivery_tag, 0)
      # Through the channel's own path, so this does not depend on the shape of
      # what it keeps per outstanding publish.
      channel.send(:reserve_confirm_tag, encoded, "unconfirmed at drop",
                   channel.send(:publish_context, exchange, "rk", {}))

      delete_queue(vhost, queue)
      delete_exchange(vhost, exchange)
      recover_connection!(session)

      # Re-published before the replay, this message would have hit a missing
      # exchange (404, closing the channel again) or a missing binding (dropped
      # silently while the broker still acks it).
      expect(channel.queue(queue, passive: true).message_count).to eq(1)
      _delivery, _header, body = channel.basic_get(queue, manual_ack: false)
      expect(body).to eq("unconfirmed at drop")
      expect(channel.unconfirmed_tags).to be_empty
    end
  end

  # A queue deleted under a live connection cancels its consumer, and a
  # cancelled consumer is gone for good: recovery re-declares the queue but does
  # not resurrect a subscription the broker explicitly ended. (A real broker
  # restart takes the connection with it, so the cancel never arrives and the
  # consumer IS re-registered — that path is covered by the recovery specs.)
  it "does not resurrect a consumer the broker cancelled, but the queue comes back" do
    isolated_session do |session, vhost|
      suffix    = SecureRandom.hex(4)
      queue     = "test.amnesia.consumer.#{suffix}"
      channel   = session.open_channel
      channel.durable_queue(queue)
      cancelled = []
      channel.on_cancel { |tag| cancelled << tag }
      tag = channel.basic_consume(queue, manual_ack: false) { |_d, _h, _b| nil }

      delete_queue(vhost, queue)
      50.times { break unless cancelled.empty?; sleep 0.1 }
      expect(cancelled).to eq([tag])

      recover_connection!(session)

      expect(channel.queue(queue, passive: true).consumer_count).to eq(0)

      # Subscribing again works on the re-declared queue.
      delivered = []
      channel.basic_consume(queue, manual_ack: false) { |_d, _h, body| delivered << body }
      channel.basic_publish("after the queue came back", routing_key: queue)
      50.times { break unless delivered.empty?; sleep 0.1 }
      expect(delivered).to eq(["after the queue came back"])
    end
  end
end
