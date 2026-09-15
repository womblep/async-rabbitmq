require "spec_helper"

# The topology registry mirrors what the broker will do, not which channel
# declared what. The broker deletes an auto-delete queue when its last consumer
# goes, so the registry forgets the queue (and its bindings, and an auto-delete
# exchange those bindings were keeping alive) at that moment: on cancel, on a
# broker-initiated cancel, and when the channel closes. Durable and exclusive
# queues outlive the channel that declared them and are kept, so recovery
# still re-declares what the application asked for.
RSpec.describe "auto-delete topology follows the consumers", :integration do

  def queue_names(vhost)
    http_get("/api/queues/#{URI.encode_www_form_component(vhost)}").map { |q| q["name"] }
  end

  it "forgets a temporary queue when the channel that consumed it closes" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
    session.connect

    # One channel per unit of work, each with its own temporary queue, as an
    # RPC client or a per-stream worker does.
    3.times do
      channel = session.open_channel
      queue   = channel.temporary_queue
      channel.basic_consume(queue.name) { |_d, _h, _b| nil }
      channel.close
    end

    expect(session.topology.queues).to be_empty
    expect(session.topology.consumers).to be_empty

    recover_connection!(session)
    sleep 0.2
    # Nothing was re-declared: no dead amq.gen-* queues piling up per reconnect.
    expect(queue_names(vhost).grep(/^amq\.gen-/)).to be_empty
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "keeps an auto-delete queue while another channel still consumes it" do
    isolated_session do |session, _|
      name = "test.prune.shared.#{SecureRandom.hex(4)}"
      a = session.open_channel
      b = session.open_channel
      a.queue(name, durable: true, auto_delete: true)
      a.basic_consume(name) { |_d, _h, _b| nil }
      b.basic_consume(name) { |_d, _h, _b| nil }

      a.close
      expect(session.topology.queues.map(&:name)).to include(name)

      b.close
      expect(session.topology.queues.map(&:name)).not_to include(name)
    end
  end

  it "forgets it on basic_cancel, together with its bindings and an auto-delete exchange" do
    isolated_session do |session, _|
      suffix   = SecureRandom.hex(4)
      channel  = session.open_channel
      queue    = channel.temporary_queue
      transient = "test.prune.x.transient.#{suffix}"
      durable   = "test.prune.x.durable.#{suffix}"
      channel.exchange(transient, type: :fanout, auto_delete: true)
      channel.exchange(durable, type: :fanout, durable: true)
      channel.queue_bind(queue.name, exchange: transient)
      channel.queue_bind(queue.name, exchange: durable)
      tag = channel.basic_consume(queue.name) { |_d, _h, _b| nil }

      expect(session.topology.consumers).to eq(tag => queue.name)

      channel.basic_cancel(tag)

      expect(session.topology.queues).to be_empty
      expect(session.topology.queue_bindings).to be_empty
      expect(session.topology.exchanges.map(&:name)).to eq([durable])
    end
  end

  it "forgets it when the broker cancels the consumer" do
    isolated_session do |session, vhost|
      name    = "test.prune.broker.#{SecureRandom.hex(4)}"
      channel = session.open_channel
      channel.queue(name, durable: true, auto_delete: true)
      cancelled = []
      channel.on_cancel { |tag| cancelled << tag }
      channel.basic_consume(name) { |_d, _h, _b| nil }

      http_delete("/api/queues/#{URI.encode_www_form_component(vhost)}/#{URI.encode_www_form_component(name)}")
      50.times { break unless cancelled.empty?; sleep 0.1 }

      expect(cancelled.size).to eq(1)
      expect(session.topology.queues.map(&:name)).not_to include(name)
    end
  end

  it "still recovers a durable queue whose declaring channel was closed" do
    isolated_session do |session, vhost|
      name  = "test.prune.durable.#{SecureRandom.hex(4)}"
      setup = session.open_channel
      setup.durable_queue(name)
      setup.close

      expect(session.topology.queues.map(&:name)).to include(name)

      http_delete("/api/queues/#{URI.encode_www_form_component(vhost)}/#{URI.encode_www_form_component(name)}")
      recover_connection!(session)

      expect(session.open_channel.queue(name, passive: true).message_count).to eq(0)
    end
  end

  it "still recovers an exclusive queue consumed on a channel other than the one that declared it" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
    session.connect

    declarer = session.open_channel
    queue    = declarer.queue("", exclusive: true) # exclusive, NOT auto-delete: lives with the connection
    consumer = session.open_channel
    delivered = []
    consumer.basic_consume(queue.name, manual_ack: false) { |_d, _h, body| delivered << body }
    declarer.close

    expect(session.topology.queues.map(&:name)).to include(queue.name)

    recover_connection!(session)

    # Re-declared under a new server name, consumer re-registered onto it.
    consumer.basic_publish("after reconnect", routing_key: queue.name)
    50.times { break unless delivered.empty?; sleep 0.1 }
    expect(delivered).to eq(["after reconnect"])
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "renames consumer records with a server-named queue" do
    registry = AsyncRabbitMQ::TopologyRegistry.new
    registry.record_queue(1, "amq.gen-old", durable: false, exclusive: true, auto_delete: true,
                             arguments: {}, server_named: true)
    registry.record_consumer("ctag-1", "amq.gen-old")

    registry.rename_queue("amq.gen-old", "amq.gen-new")
    expect(registry.consumers).to eq("ctag-1" => "amq.gen-new")

    registry.delete_consumer("ctag-1")
    expect(registry.queues).to be_empty
  end
end
