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

  # The warning has to be ack-mode aware, because the advice is. RabbitMQ
  # applies prefetch only to unacknowledged messages, so basic_qos bounds a
  # manual-ack consumer and does nothing whatsoever for an auto-ack one.
  # Pointing an auto-ack caller at basic_qos would send them to a no-op.
  describe "consuming without a bound" do
    # AsyncRabbitMQ.warn_unbounded_consumers is process-wide, and the suite runs
    # in random order, so put it back whatever an example did to it.
    around do |example|
      previous = AsyncRabbitMQ.warn_unbounded_consumers
      example.run
    ensure
      AsyncRabbitMQ.warn_unbounded_consumers = previous
    end

    # Collect what the channel logs as warnings.
    def warnings_from(channel)
      [].tap do |collected|
        logger = channel.instance_variable_get(:@logger)
        logger.define_singleton_method(:warn) { |msg| collected << msg.to_s }
      end
    end

    it "warns a manual-ack consumer once per channel, naming the queue" do
      isolated_session do |session, _|
        ch       = session.open_channel
        warnings = warnings_from(ch)

        qname = "test.noqos.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: true) { |_d, _h, _b| }

        expect(warnings.grep(/without basic_qos/).size).to eq(1)
        expect(warnings.join).to include(qname)

        # Only once per channel, however many consumers.
        other = "test.noqos2.#{SecureRandom.hex(4)}"
        ch.queue(other, durable: true)
        ch.basic_consume(other, manual_ack: true) { |_d, _h, _b| }
        expect(warnings.grep(/without basic_qos/).size).to eq(1)
        ch.close
      end
    end

    it "tells an auto-ack consumer that basic_qos cannot bound it" do
      isolated_session do |session, _|
        ch       = session.open_channel
        warnings = warnings_from(ch)

        qname = "test.autoack.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: false) { |_d, _h, _b| }

        expect(warnings.grep(/basic_qos cannot bound/).size).to eq(1)
        expect(warnings.join).to include(qname)
        # It must not send them to the no-op.
        expect(warnings.grep(/Call basic_qos/)).to be_empty
        ch.close
      end
    end

    it "stays quiet for a manual-ack consumer once prefetch is set" do
      isolated_session do |session, _|
        ch       = session.open_channel
        warnings = warnings_from(ch)

        ch.basic_qos(prefetch_count: 10)
        qname = "test.qos.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: true) { |_d, _h, _b| }

        expect(warnings.grep(/basic_qos/)).to be_empty
        ch.close
      end
    end

    # Silencing is a boot-time decision, not a per-channel one: an application
    # either accepts unbounded consumers or it does not. It needs to be possible
    # without turning the logger down, which would lose every other warning a
    # channel raises.
    it "stays quiet everywhere once switched off" do
      AsyncRabbitMQ.warn_unbounded_consumers = false

      isolated_session do |session, _|
        ch       = session.open_channel
        warnings = warnings_from(ch)

        qname = "test.optout.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: false) { |_d, _h, _b| }

        # A second channel too: the setting is not per-channel state captured
        # when one happened to be opened.
        other      = session.open_channel
        other_warn = warnings_from(other)
        oname      = "test.optout2.#{SecureRandom.hex(4)}"
        other.queue(oname, durable: true)
        other.basic_consume(oname, manual_ack: true) { |_d, _h, _b| }

        expect(warnings).to be_empty
        expect(other_warn).to be_empty
        ch.close
        other.close
      end
    end

    it "defaults to warning, and warns again once switched back on" do
      expect(AsyncRabbitMQ.warn_unbounded_consumers).to be(true)
      AsyncRabbitMQ.warn_unbounded_consumers = false
      AsyncRabbitMQ.warn_unbounded_consumers = true

      isolated_session do |session, _|
        ch       = session.open_channel
        warnings = warnings_from(ch)

        qname = "test.optin.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: false) { |_d, _h, _b| }

        expect(warnings.grep(/basic_qos cannot bound/).size).to eq(1)
        ch.close
      end
    end

    it "still warns an auto-ack consumer that has set a prefetch" do
      isolated_session do |session, _|
        ch       = session.open_channel
        warnings = warnings_from(ch)

        ch.basic_qos(prefetch_count: 10)
        qname = "test.qos.autoack.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.basic_consume(qname, manual_ack: false) { |_d, _h, _b| }

        # Having set a prefetch is the case where the caller most believes the
        # consumer is bounded, so silence here would be the worst outcome.
        expect(warnings.grep(/basic_qos cannot bound/).size).to eq(1)
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
        ch.instance_variable_set(:@pending_confirms, { 99_999 => ["never confirmed", nil, nil] })

        expect { ch.wait_for_confirms(timeout: 0.5) }
          .to raise_error(AsyncRabbitMQ::ConfirmTimeoutError) { |e|
            expect(e.unconfirmed_tags).to eq([99_999])
            expect(e.channel_id).to eq(ch.channel_id)
          }
      end
    end
  end

  describe "unconfirmed messages" do
    it "keeps the publish options so they can be sent again elsewhere" do
      isolated_session do |session, _|
        ch = session.open_channel
        qname = "test.unconfirmed.opts.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.confirm_select

        # Stop confirms resolving so the record stays outstanding to inspect.
        allow(ch).to receive(:handle_confirm_ack)

        ch.basic_publish("body", routing_key: qname, persistent: true,
                         headers: { "tenant" => "acme" }, message_id: "m-1",
                         correlation_id: "c-1")
        sleep 0.3

        msg = ch.unconfirmed_messages.first
        expect(msg).not_to be_nil
        expect(msg.payload).to eq("body")
        expect(msg.routing_key).to eq(qname)
        # Without these a republish on another node silently downgrades the
        # message: no persistence, no headers, no correlation.
        expect(msg.options[:persistent]).to be true
        expect(msg.options[:headers]).to eq("tenant" => "acme")
        expect(msg.options[:message_id]).to eq("m-1")
        expect(msg.options[:correlation_id]).to eq("c-1")
      end
    end

    it "copies the caller's headers hash rather than holding a reference" do
      isolated_session do |session, _|
        ch = session.open_channel
        qname = "test.unconfirmed.copy.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.confirm_select
        allow(ch).to receive(:handle_confirm_ack)

        headers = { "tenant" => "acme" }
        ch.basic_publish("body", routing_key: qname, headers: headers)
        sleep 0.3

        headers["tenant"] = "mutated"       # the caller reuses its hash
        headers["added"]  = "later"

        stored = ch.unconfirmed_messages.first.options[:headers]
        expect(stored).to eq("tenant" => "acme")
      end
    end
  end

  describe "reopening a channel" do
    it "replays unconfirmed messages before it accepts new publishes" do
      isolated_session do |session, _|
        ch = session.open_channel
        qname = "test.replay.order.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)
        ch.confirm_select

        # Hold a publish unconfirmed, then have the broker close the channel.
        allow(ch).to receive(:handle_confirm_ack)
        ch.basic_publish("first", routing_key: qname)
        sleep 0.2
        expect(ch.unconfirmed_messages.size).to eq(1)

        observed = []
        allow(ch).to receive(:republish_unconfirmed).and_wrap_original do |orig, *args|
          observed << ch.instance_variable_get(:@state)
          orig.call(*args)
        end

        ch.basic_ack(4242)                 # unknown tag: 406, broker closes it
        wait_until { ch.closed? }
        ch.reopen

        # The replay must run before the channel is open, or a publish arriving
        # mid-replay takes a tag that no longer matches its place on the wire.
        expect(observed).to eq([:resyncing])
        expect(ch.open?).to be true
      end
    end
  end

  describe "recovery retry after a drop mid-reopen" do
    it "tears the half-built connection down before going round again" do
      isolated_session(recovery_interval: 0.2, recovery_max_interval: 0.3) do |session, _|
        session.open_channel

        states = []
        stopped = []
        first = true

        # Fail the first reopen the way a drop mid-reopen does.
        allow(session).to receive(:reopen_channels).and_wrap_original do |orig, *args|
          if first
            first = false
            io = session.instance_variable_get(:@frame_io)
            allow(io).to receive(:stop).and_wrap_original { |m, *a| stopped << io; m.call(*a) }
            false
          else
            orig.call(*args)
          end
        end

        watcher = Async do
          200.times { states << session.instance_variable_get(:@state); sleep 0.02 }
        end

        session.trigger_recovery(AsyncRabbitMQ::ConnectionError.new(code: 0, text: "forced"))
        wait_until(20) { session.open? }
        watcher.stop

        # While retrying it must not advertise itself as open: a Cluster would
        # place new channels on a connection that is already dead.
        expect(states).to include(:recovering)
        expect(stopped).not_to be_empty
        expect(session.instance_variable_get(:@channel0_task)).not_to be_nil
      end
    end
  end

  describe "background loops" do
    it "keeps the connection alive after the task that called connect has finished" do
      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)
      session = nil

      # connect inside a task that is then STOPPED. Async stops a task's
      # children with it, so if the reader and writer hang off the connect
      # caller they die here and the connection is silently dead while the
      # session still reports itself open.
      Async do |task|
        inner = task.async do
          session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)
          session.connect
          sleep 30                    # keep the task alive until it is stopped
        end
        wait_until(10) { session&.open? }
        inner.stop
      end.wait
      sleep 0.2

      expect(session.open?).to be true

      # The connection still works, which means the reader and writer survived.
      ch = session.open_channel
      qname = "test.connparent.#{SecureRandom.hex(4)}"
      ch.queue(qname, durable: true)
      ch.basic_publish("still alive", routing_key: qname)
      sleep 0.3
      expect(ch.basic_get(qname, manual_ack: false)[2]).to eq("still alive")
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end

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
