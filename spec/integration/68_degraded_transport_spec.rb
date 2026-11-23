require "spec_helper"

# Degraded transport rather than severed: frames arriving in tiny TCP fragments,
# and a connection ending partway through a multi-frame message body. Both
# exercise read_exactly's reassembly loop and the discard of half-assembled
# content, which every clean-FIN test skips because frames arrive whole.
RSpec.describe "degraded transport", :integration, :toxiproxy do

  def wait_until(what, timeout: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for #{what}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  def with_proxied_session(**opts)
    skip "Toxiproxy not available" unless toxiproxy_available?
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST, port: TOXIPROXY_PORT, vhost: vhost,
      recovery_interval: 0.3, recovery_max_interval: 0.5, **opts
    )
    session.connect
    yield session, vhost
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  # A deterministic pattern long enough for several body frames at the default
  # 128KiB frame_max; any reassembly slip changes the digest.
  def patterned_payload(bytes, seed)
    base = "#{seed}:0123456789abcdefghijklmnopqrstuvwxyz-"
    (base * (bytes / base.bytesize + 1))[0, bytes]
  end

  it "reassembles a multi-frame message delivered in tiny TCP fragments" do
    with_proxied_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.sliced", durable: true)
      payload = patterned_payload(300_000, "sliced")

      toxiproxy_rabbitmq.upstream(:slicer, average_size: 1024, size_variation: 512, delay: 10)
                        .downstream(:slicer, average_size: 1024, size_variation: 512, delay: 10)
                        .apply do
        ch.basic_publish(payload, routing_key: q.name)
        got = nil
        wait_until("the sliced message") do
          _di, _h, body = ch.basic_get(q.name, manual_ack: false)
          got = body
        end
        expect(got.bytesize).to eq(payload.bytesize)
        expect(got).to eq(payload)
      end
    end
  end

  it "discards a half-delivered body when the connection ends mid-message, and redelivers it whole" do
    with_proxied_session do |session, vhost|
      q_name  = "test.midbody.#{SecureRandom.hex(4)}"
      payload = patterned_payload(400_000, "midbody") # several body frames

      # Stage the message from a direct (un-proxied) connection, so the proxy's
      # byte budget below is spent only on this message's delivery to us.
      direct = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
      direct.connect
      direct.with_channel do |dch|
        dch.queue(q_name, durable: true)
        dch.basic_publish(payload, routing_key: q_name)
      end
      direct.close

      ch = session.open_channel
      ch.basic_qos(prefetch_count: 1)
      received = []
      lost = false
      session.on_event("connection.lost") { |_n, _p| lost = true }

      # Budget the downstream pipe to less than one message, THEN start
      # consuming: the broker pushes the body, and the pipe dies partway
      # through it. read_exactly's 'Socket closed mid-frame' must fire and the
      # half-assembled content be discarded — never handed to the handler.
      toxiproxy_rabbitmq.downstream(:limit_data, bytes: 200_000).apply do
        ch.basic_consume(q_name, manual_ack: true) do |di, _h, body|
          received << body
          ch.basic_ack(di.delivery_tag)
        end
        wait_until("the mid-body cut") { lost }
      end

      # After recovery the broker requeues the un-acked delivery and sends it
      # again, whole, on the reopened channel.
      wait_until("the message redelivered intact") { received.any? }
      expect(received.map(&:bytesize)).to all(eq(payload.bytesize))
      received.each { |body| expect(body).to eq(payload) }
    end
  end
end
