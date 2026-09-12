require "spec_helper"

# Step 25: P2 Bunny API parity — convenience methods, predicates, and named properties.
RSpec.describe "Bunny API parity (P2)", :integration do

  # ---------------------------------------------------------------------------
  # Channel#exchange internal: flag
  # ---------------------------------------------------------------------------

  describe "Channel#exchange internal: flag" do
    it "declares an internal exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.exchange("test.internal.#{SecureRandom.hex(4)}", type: :fanout, internal: true)
        expect(ex.name).to include("test.internal.")
        expect(ex.internal?).to be true
        ch.close
      end
    end

    it "rejects direct publish to an internal exchange with a channel error" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex_name = "test.internal.reject.#{SecureRandom.hex(4)}"
        ch.exchange(ex_name, type: :fanout, internal: true)

        # Publishing directly to an internal exchange should trigger 403 ACCESS_REFUSED
        ch.basic_publish("hello", exchange: ex_name, routing_key: "")
        sleep 0.3

        # The channel should be closed by the broker
        expect(ch.closed?).to be true
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Convenience exchange type helpers
  # ---------------------------------------------------------------------------

  describe "Convenience exchange helpers" do
    it "Channel#direct declares a direct exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.direct("test.conv.direct.#{SecureRandom.hex(4)}")
        expect(ex.type).to eq(:direct)
        ch.close
      end
    end

    it "Channel#fanout declares a fanout exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.fanout("test.conv.fanout.#{SecureRandom.hex(4)}")
        expect(ex.type).to eq(:fanout)
        ch.close
      end
    end

    it "Channel#topic declares a topic exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.topic("test.conv.topic.#{SecureRandom.hex(4)}")
        expect(ex.type).to eq(:topic)
        ch.close
      end
    end

    it "Channel#headers declares a headers exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.headers("test.conv.headers.#{SecureRandom.hex(4)}")
        expect(ex.type).to eq(:headers)
        ch.close
      end
    end

    it "Channel#default_exchange returns the default '' exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.default_exchange
        expect(ex.name).to eq("")
        expect(ex.type).to eq(:direct)
        expect(ex.durable?).to be true
        expect(ex.predefined?).to be true
        ch.close
      end
    end

    it "convenience exchange helpers forward keyword arguments" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.fanout("test.conv.durable.#{SecureRandom.hex(4)}", durable: true)
        expect(ex.durable?).to be true
        ch.close
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Convenience queue helpers
  # ---------------------------------------------------------------------------

  describe "Convenience queue helpers" do
    it "Channel#temporary_queue creates an exclusive auto-delete queue" do
      isolated_session do |session, _|
        ch = session.open_channel
        q = ch.temporary_queue
        expect(q.name).to match(/\Aamq\.gen-/)
        expect(q.exclusive?).to be true
        expect(q.auto_delete?).to be true
        expect(q.server_named?).to be true
        ch.close
      end
    end

    it "Channel#quorum_queue creates a durable quorum queue" do
      isolated_session do |session, _|
        ch = session.open_channel
        q_name = "test.quorum.#{SecureRandom.hex(4)}"
        q = ch.quorum_queue(q_name)
        expect(q.name).to eq(q_name)
        expect(q.durable?).to be true
        q.delete
        ch.close
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Queue predicates
  # ---------------------------------------------------------------------------

  describe "Queue predicate methods" do
    it "exposes durable?" do
      isolated_session do |session, _|
        ch = session.open_channel
        q = ch.queue("test.pred.dur.#{SecureRandom.hex(4)}", durable: true)
        expect(q.durable?).to be true
        q.delete
        ch.close
      end
    end

    it "exposes exclusive?" do
      isolated_session do |session, _|
        ch = session.open_channel
        q = ch.queue("", exclusive: true)
        expect(q.exclusive?).to be true
        ch.close
      end
    end

    it "exposes auto_delete?" do
      isolated_session do |session, _|
        ch = session.open_channel
        q = ch.queue("test.pred.ad.#{SecureRandom.hex(4)}", durable: true, auto_delete: true)
        expect(q.auto_delete?).to be true
        q.delete
        ch.close
      end
    end

    it "server_named? is true for broker-generated names" do
      isolated_session do |session, _|
        ch = session.open_channel
        q = ch.queue("", exclusive: true)
        expect(q.server_named?).to be true
        ch.close
      end
    end

    it "server_named? is false for user-named queues" do
      isolated_session do |session, _|
        ch = session.open_channel
        q = ch.queue("test.pred.named.#{SecureRandom.hex(4)}", durable: true)
        expect(q.server_named?).to be false
        ch.close
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Queue#status
  # ---------------------------------------------------------------------------

  describe "Queue#status" do
    it "returns current message_count and consumer_count" do
      isolated_session do |session, _|
        ch = session.open_channel
        q_name = "test.status.#{SecureRandom.hex(4)}"
        q = ch.queue(q_name, durable: true)

        3.times { ch.basic_publish("msg", routing_key: q_name) }
        sleep 0.1

        status = q.status
        expect(status).to be_a(Hash)
        expect(status[:message_count]).to be >= 3
        expect(status[:consumer_count]).to eq(0)

        # message_count and consumer_count on the Queue object should also be updated
        expect(q.message_count).to be >= 3
        ch.close
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Exchange predicates
  # ---------------------------------------------------------------------------

  describe "Exchange predicate methods" do
    it "exposes durable?" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.exchange("test.pred.dur.ex.#{SecureRandom.hex(4)}", type: :direct, durable: true)
        expect(ex.durable?).to be true
        ex.delete
        ch.close
      end
    end

    it "exposes auto_delete?" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.exchange("test.pred.ad.ex.#{SecureRandom.hex(4)}", type: :direct, auto_delete: true)
        expect(ex.auto_delete?).to be true
        ex.delete
        ch.close
      end
    end

    it "exposes internal?" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.exchange("test.pred.int.ex.#{SecureRandom.hex(4)}", type: :fanout, internal: true)
        expect(ex.internal?).to be true
        ex.delete
        ch.close
      end
    end

    it "predefined? is true for amq.* exchanges" do
      ex = AsyncRabbitMQ::Exchange.new("amq.direct", :direct, nil, durable: true)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for the default exchange" do
      ex = AsyncRabbitMQ::Exchange.new("", :direct, nil, durable: true)
      expect(ex.predefined?).to be true
    end

    it "predefined? is false for user-declared exchanges" do
      ex = AsyncRabbitMQ::Exchange.new("my.custom.ex", :direct, nil)
      expect(ex.predefined?).to be false
    end
  end

  # ---------------------------------------------------------------------------
  # Exchange#on_return
  # ---------------------------------------------------------------------------

  describe "Exchange#on_return" do
    it "delegates to the channel's on_return handler" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex = ch.default_exchange

        returned = []
        ex.on_return { |_ret, _headers, body| returned << body.to_s }

        ch.basic_publish(
          "unroutable-via-ex",
          exchange: "",
          routing_key: "no.such.queue.#{SecureRandom.hex(8)}",
          mandatory: true
        )
        sleep 0.3

        expect(returned).not_to be_empty
        expect(returned.first).to include("unroutable-via-ex")
        ch.close
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Session#with_channel
  # ---------------------------------------------------------------------------

  describe "Session#with_channel" do
    it "yields an open channel and closes it after the block" do
      isolated_session do |session, _|
        captured_ch = nil
        session.with_channel do |ch|
          captured_ch = ch
          expect(ch.open?).to be true

          q = ch.queue("test.with_channel.#{SecureRandom.hex(4)}", durable: true)
          expect(q.name).to include("test.with_channel.")
        end
        expect(captured_ch.closed?).to be true
      end
    end

    it "closes the channel even when an exception is raised" do
      isolated_session do |session, _|
        captured_ch = nil
        begin
          session.with_channel do |ch|
            captured_ch = ch
            raise "boom"
          end
        rescue RuntimeError
          # expected
        end
        expect(captured_ch.closed?).to be true
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Session#queue_exists? / #exchange_exists?
  # ---------------------------------------------------------------------------

  describe "Session#queue_exists?" do
    it "returns true for an existing queue" do
      isolated_session do |session, _|
        ch = session.open_channel
        q_name = "test.exists.q.#{SecureRandom.hex(4)}"
        ch.queue(q_name, durable: true)
        ch.close

        expect(session.queue_exists?(q_name)).to be true
      end
    end

    it "returns false for a non-existent queue" do
      isolated_session do |session, _|
        expect(session.queue_exists?("no.such.queue.#{SecureRandom.hex(8)}")).to be false
      end
    end
  end

  describe "Session#exchange_exists?" do
    it "returns true for an existing exchange" do
      isolated_session do |session, _|
        ch = session.open_channel
        ex_name = "test.exists.ex.#{SecureRandom.hex(4)}"
        ch.exchange(ex_name, type: :direct)
        ch.close

        expect(session.exchange_exists?(ex_name)).to be true
      end
    end

    it "returns true for the built-in amq.direct exchange" do
      isolated_session do |session, _|
        expect(session.exchange_exists?("amq.direct")).to be true
      end
    end

    it "returns false for a non-existent exchange" do
      isolated_session do |session, _|
        expect(session.exchange_exists?("no.such.exchange.#{SecureRandom.hex(8)}")).to be false
      end
    end
  end

  # ---------------------------------------------------------------------------
  # basic_publish named message properties
  # ---------------------------------------------------------------------------

  describe "basic_publish named message properties" do
    # AMQ::Protocol::HeaderFrame exposes properties via #properties (a Hash
    # with symbol keys), not as top-level accessor methods.

    it "forwards content_type and content_encoding" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.ct.#{SecureRandom.hex(4)}", durable: true)

        ch.basic_publish(
          '{"key":"value"}',
          routing_key: q.name,
          content_type: "application/json",
          content_encoding: "utf-8"
        )

        _di, header, _body = ch.basic_get(q.name)
        props = header.properties
        expect(props[:content_type]).to eq("application/json")
        expect(props[:content_encoding]).to eq("utf-8")
        ch.close
      end
    end

    it "forwards correlation_id and reply_to" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.rpc.#{SecureRandom.hex(4)}", durable: true)

        ch.basic_publish(
          "rpc-request",
          routing_key: q.name,
          correlation_id: "abc-123",
          reply_to: "reply.queue"
        )

        _di, header, _body = ch.basic_get(q.name)
        props = header.properties
        expect(props[:correlation_id]).to eq("abc-123")
        expect(props[:reply_to]).to eq("reply.queue")
        ch.close
      end
    end

    it "forwards message_id, timestamp, type, and app_id" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.meta.#{SecureRandom.hex(4)}", durable: true)
        ts = Time.now.to_i

        ch.basic_publish(
          "metadata",
          routing_key: q.name,
          message_id: "msg-456",
          timestamp: ts,
          type: "user.created",
          app_id: "test-suite"
        )

        _di, header, _body = ch.basic_get(q.name)
        props = header.properties
        expect(props[:message_id]).to eq("msg-456")
        expect(props[:timestamp].to_i).to eq(ts)
        expect(props[:type]).to eq("user.created")
        expect(props[:app_id]).to eq("test-suite")
        ch.close
      end
    end

    it "forwards priority" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.pri.#{SecureRandom.hex(4)}", durable: true, arguments: { "x-max-priority" => 10 })

        ch.basic_publish("high-pri", routing_key: q.name, priority: 5)

        _di, header, _body = ch.basic_get(q.name)
        expect(header.properties[:priority]).to eq(5)
        ch.close
      end
    end

    it "forwards expiration" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.exp.#{SecureRandom.hex(4)}", durable: true)

        ch.basic_publish("ttl-msg", routing_key: q.name, expiration: "60000")

        _di, header, _body = ch.basic_get(q.name)
        expect(header.properties[:expiration]).to eq("60000")
        ch.close
      end
    end

    it "forwards headers" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.hdr.#{SecureRandom.hex(4)}", durable: true)

        ch.basic_publish(
          "with-headers",
          routing_key: q.name,
          headers: { "x-retry-count" => 3, "x-source" => "test" }
        )

        _di, header, _body = ch.basic_get(q.name)
        expect(header.properties[:headers]).to include("x-retry-count" => 3, "x-source" => "test")
        ch.close
      end
    end

    it "named kwargs merge with properties: hash (properties: hash wins on overlap)" do
      isolated_session do |session, _|
        ch = session.open_channel
        q  = ch.queue("test.props.merge.#{SecureRandom.hex(4)}", durable: true)

        ch.basic_publish(
          "merge",
          routing_key: q.name,
          content_type: "text/plain",
          properties: { content_type: "application/octet-stream", app_id: "from-hash" }
        )

        _di, header, _body = ch.basic_get(q.name)
        props = header.properties
        # properties: hash merges on top of named kwargs
        expect(props[:content_type]).to eq("application/octet-stream")
        expect(props[:app_id]).to eq("from-hash")
        ch.close
      end
    end
  end
end
