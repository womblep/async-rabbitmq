require "spec_helper"

# Smaller reliability fixes that do not each warrant a file: the TLS socket
# closing its transport, the unbounded-prefetch warning, a bounded
# wait_for_confirms, and background loops that outlive the task that started
# them.
RSpec.describe "reliability gaps", :integration do

  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  # True only when the port completes a real TLS handshake; an open port that
  # is not serving TLS would fail later with SSLError instead of skipping.
  def tls_serving?
    require "openssl"
    raw = TCPSocket.new(RABBITMQ_HOST, RABBITMQ_TLS_PORT)
    ctx = OpenSSL::SSL::SSLContext.new
    ctx.set_params(verify_mode: OpenSSL::SSL::VERIFY_NONE)
    ssl = OpenSSL::SSL::SSLSocket.new(raw, ctx)
    ssl.connect
    ssl.close rescue nil
    raw.close rescue nil
    true
  rescue StandardError
    false
  end

  describe "TLS transport" do
    it "closes the underlying TCP socket with the SSL socket" do
      skip "TLS port not available" unless tls_serving?

      session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_TLS_PORT,
                                           tls: true, verify_peer: false)
      session.connect
      ssl = session.instance_variable_get(:@frame_io).instance_variable_get(:@socket)
      expect(ssl).to be_a(OpenSSL::SSL::SSLSocket)
      expect(ssl.sync_close).to be true

      raw = ssl.io
      session.close
      # Without sync_close the TCP socket would stay open until the GC ran.
      wait_until(5) { raw.closed? }
      expect(raw.closed?).to be true
    end
  end

  describe "consuming without basic_qos" do
    it "warns once, naming the queue" do
      isolated_session do |session, _|
        warnings = []
        logger = session.instance_variable_get(:@logger)
        logger.define_singleton_method(:warn) { |msg| warnings << msg.to_s }

        ch = session.open_channel
        ch.instance_variable_set(:@logger, logger)
        qname = "test.noqos.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: false) { |_d, _h, _b| }

        expect(warnings.grep(/without basic_qos/).size).to eq(1)
        expect(warnings.join).to include(qname)

        # Only once per channel, however many consumers.
        other = "test.noqos2.#{SecureRandom.hex(4)}"
        ch.queue(other, durable: true)
        ch.basic_consume(other, manual_ack: false) { |_d, _h, _b| }
        expect(warnings.grep(/without basic_qos/).size).to eq(1)
        ch.close
      end
    end

    it "stays quiet when prefetch is set" do
      isolated_session do |session, _|
        warnings = []
        ch = session.open_channel
        logger = ch.instance_variable_get(:@logger)
        logger.define_singleton_method(:warn) { |msg| warnings << msg.to_s }

        ch.basic_qos(prefetch_count: 10)
        qname = "test.qos.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: false) { |_d, _h, _b| }

        expect(warnings.grep(/without basic_qos/)).to be_empty
        ch.close
      end
    end
  end

  describe "wait_for_confirms" do
    it "still waits forever by default" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.confirm.default.#{SecureRandom.hex(4)}", durable: true)
        ch.confirm_select
        ch.basic_publish("x", routing_key: q.name)
        expect(ch.wait_for_confirms).to be true
        ch.close
      end
    end

    it "raises ConfirmTimeoutError instead of parking forever" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.confirm.timeout.#{SecureRandom.hex(4)}", durable: true)
        ch.confirm_select
        ch.basic_publish("x", routing_key: q.name)

        # Drop the broker's confirm so nothing ever resolves the publish.
        ch.instance_variable_set(:@pending_confirms, { 99_999 => "never confirmed" })

        expect { ch.wait_for_confirms(timeout: 0.5) }
          .to raise_error(AsyncRabbitMQ::ConfirmTimeoutError) { |e|
            expect(e.unconfirmed_tags).to eq([99_999])
            expect(e.channel_id).to eq(ch.channel_id)
          }
      end
    end
  end

  describe "background loops" do
    it "keeps consuming after the task that opened the channel has finished" do
      isolated_session do |session, _|
        qname = "test.taskparent.#{SecureRandom.hex(4)}"
        got   = []
        ch    = nil

        # Open the channel and its consumer inside a task that then ends. The
        # dispatch loop must not be a child of it.
        # Nested inside a reactor, Async{} returns without blocking, so wait on
        # it explicitly: the point is that the spawning task has *finished*.
        Async do |task|
          inner = task.async do
            ch = session.open_channel
            ch.basic_qos(prefetch_count: 5)
            ch.queue(qname, durable: true)
            ch.basic_consume(qname, manual_ack: false) { |_d, _h, b| got << b }
          end
          inner.wait
        end.wait

        expect(ch).not_to be_nil
        expect(ch.open?).to be true

        ch.basic_publish("after parent ended", routing_key: qname)
        wait_until { !got.empty? }
        expect(got).to eq(["after parent ended"])
        ch.close
      end
    end
  end
end
