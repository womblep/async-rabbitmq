require "spec_helper"

# Tests for on_blocked/on_unblocked callbacks, Channel#on_error callback,
# and connection_name client property.
RSpec.describe "callbacks and client properties", :integration do

  # ---------------------------------------------------------------------------
  # Session#on_blocked / #on_unblocked callbacks
  # ---------------------------------------------------------------------------

  describe "Session#on_blocked / #on_unblocked" do
    it "invokes on_blocked callback with reason when connection.blocked is received" do
      isolated_session do |session, _|
        received_reason = nil
        session.on_blocked { |reason| received_reason = reason }

        frame_io = session.instance_variable_get(:@frame_io)
        queue0 = frame_io.channel_queue(0)
        queue0.push([:method, AMQ::Protocol::Connection::Blocked.new("low on memory")])
        sleep 0.1

        expect(received_reason).to eq("low on memory")

        # Unblock so close doesn't hang
        queue0.push([:method, AMQ::Protocol::Connection::Unblocked.new])
        sleep 0.05
      end
    end

    it "invokes on_unblocked callback when connection.unblocked is received" do
      isolated_session do |session, _|
        unblocked_called = false
        session.on_unblocked { unblocked_called = true }

        frame_io = session.instance_variable_get(:@frame_io)
        queue0 = frame_io.channel_queue(0)
        queue0.push([:method, AMQ::Protocol::Connection::Blocked.new("test")])
        sleep 0.05
        queue0.push([:method, AMQ::Protocol::Connection::Unblocked.new])
        sleep 0.1

        expect(unblocked_called).to be true
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Channel#on_error callback
  # ---------------------------------------------------------------------------

  describe "Channel#on_error" do
    it "invokes on_error callback when broker closes the channel" do
      isolated_session do |session, _|
        ch = session.open_channel
        callback_args = nil
        ch.on_error { |channel, method| callback_args = [channel, method] }

        # Inject a server-initiated channel.close on channel's queue
        frame_io = session.instance_variable_get(:@frame_io)
        ch_queue = frame_io.channel_queue(ch.instance_variable_get(:@channel_id))
        close_method = AMQ::Protocol::Channel::Close.new(404, "NOT_FOUND - no queue 'nonexistent'", 0, 0)
        ch_queue.push([:method, close_method])

        sleep 0.2

        expect(callback_args).not_to be_nil
        channel, method = callback_args
        expect(channel).to eq(ch)
        expect(method.reply_code).to eq(404)
        expect(method.reply_text).to include("NOT_FOUND")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # connection_name in client properties
  # ---------------------------------------------------------------------------

  describe "connection_name client property" do
    it "connects successfully with connection_name and sends client properties" do
      name = "test-conn-#{SecureRandom.hex(4)}"
      isolated_session(connection_name: name) do |session, _|
        expect(session.open?).to be true
        expect(session.instance_variable_get(:@connection_name)).to eq(name)

        # Verify we can operate normally (open channel, declare queue)
        ch = session.open_channel
        q = ch.queue("test.props.#{SecureRandom.hex(4)}", durable: false)
        expect(q.name).not_to be_empty
        ch.close
      end
    end

    it "connects successfully without connection_name and sends default client properties" do
      isolated_session do |session, _|
        expect(session.open?).to be true
        expect(session.instance_variable_get(:@connection_name)).to be_nil
      end
    end

    it "client properties are visible in management API" do
      # Verify client properties appear in the RabbitMQ management API.
      # The management stats DB has a collection interval, so we poll with
      # retries. We use a separate Thread (with its own sleep) to make the
      # HTTP call outside the Async IO scheduler.
      name = "test-conn-#{SecureRandom.hex(4)}"
      isolated_session(connection_name: name) do |session, _|
        require "json"

        our_conn = nil
        5.times do
          # Async sleep keeps reactor alive (heartbeats, etc.)
          sleep 1
          raw = Thread.new {
            require "net/http"
            uri = URI("http://#{RABBITMQ_HOST}:#{RABBITMQ_MGMT}/api/connections")
            req = Net::HTTP::Get.new(uri)
            req.basic_auth("guest", "guest")
            Net::HTTP.start(uri.host, uri.port) { |http| http.request(req) }.body
          }.value
          conns = JSON.parse(raw)
          our_conn = conns.find { |c|
            (c.dig("client_properties") || {})["connection_name"] == name
          }
          break if our_conn
        end

        expect(our_conn).not_to be_nil, "Expected management API to show connection_name=#{name}"
        expect(our_conn.dig("client_properties", "product")).to eq("async-rabbitmq")
        expect(our_conn.dig("client_properties", "version")).to eq(AsyncRabbitMQ::VERSION)
      end
    end
  end
end
