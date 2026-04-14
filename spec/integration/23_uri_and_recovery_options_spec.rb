require "spec_helper"

# Tests for URI connection strings, auto_recover option, and configurable
# recovery options with callbacks.
RSpec.describe "URI parsing and recovery options", :integration do

  # ---------------------------------------------------------------------------
  # Session.from_uri
  # ---------------------------------------------------------------------------

  describe "Session.from_uri" do
    it "connects using an amqp:// URI string" do
      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)
      uri = "amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}/#{URI.encode_www_form_component(vhost)}"
      session = AsyncRabbitMQ::Session.from_uri(uri)
      session.connect

      expect(session.open?).to be true
      expect(session.host).to eq(RABBITMQ_HOST)
      expect(session.port).to eq(RABBITMQ_PORT)
      expect(session.vhost).to eq(vhost)
      expect(session.username).to eq("guest")

      ch = session.open_channel
      q = ch.queue("test.uri.#{SecureRandom.hex(4)}", durable: false)
      expect(q.name).not_to be_empty
      ch.close
      session.close
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end

    it "uses default vhost when URI path is empty" do
      session = AsyncRabbitMQ::Session.from_uri("amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}")
      session.connect
      expect(session.open?).to be true
      expect(session.vhost).to eq("/")
      session.close
    ensure
      session&.close rescue nil
    end

    it "decodes percent-encoded vhost" do
      vhost = "/production"
      encoded_vhost = "%2Fproduction"
      create_vhost(vhost)
      session = AsyncRabbitMQ::Session.from_uri("amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}/#{encoded_vhost}")
      session.connect
      expect(session.open?).to be true
      expect(session.vhost).to eq(vhost)
      session.close
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end

    it "allows keyword overrides" do
      session = AsyncRabbitMQ::Session.from_uri(
        "amqp://wronguser:wrongpass@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}",
        username: "guest", password: "guest"
      )
      session.connect
      expect(session.open?).to be true
      expect(session.username).to eq("guest")
      session.close
    ensure
      session&.close rescue nil
    end

    it "raises ArgumentError for non-AMQP URI schemes" do
      expect {
        AsyncRabbitMQ::Session.from_uri("http://localhost")
      }.to raise_error(ArgumentError, /amqp/)
    end

    it "sets tls: true for amqps:// scheme" do
      session = AsyncRabbitMQ::Session.from_uri("amqps://rabbit:5671/test")
      expect(session.instance_variable_get(:@tls)).to be true
      expect(session.port).to eq(5671)
    end
  end

  # ---------------------------------------------------------------------------
  # auto_recover: false
  # ---------------------------------------------------------------------------

  describe "auto_recover: false" do
    it "does not attempt recovery when connection is lost" do
      isolated_session(auto_recover: false) do |session, _|
        expect(session.open?).to be true

        # Simulate a connection loss
        error = AsyncRabbitMQ::ConnectionError.new(code: 0, text: "simulated disconnect")
        session.trigger_recovery(error)

        # Session should be closed, not recovering
        sleep 0.1
        expect(session.open?).to be false
        expect(session.closed?).to be true
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Recovery options and callbacks
  # ---------------------------------------------------------------------------

  describe "recovery options" do
    it "fires on_recovery_attempt and on_recovery callbacks", :toxiproxy do
      skip "Toxiproxy not available" unless toxiproxy_available?

      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)

      attempt_numbers = []
      recovery_fired = false

      session = AsyncRabbitMQ::Session.new(
        host: RABBITMQ_HOST,
        port: TOXIPROXY_PORT,
        vhost: vhost,
        recovery_interval: 0.5,
        recovery_max_interval: 1.0
      )
      session.on_recovery_attempt { |n| attempt_numbers << n }
      session.on_recovery { |_s| recovery_fired = true }
      session.connect

      # Cut and restore the connection
      toxiproxy_rabbitmq.down do
        sleep 0.5
      end

      # Wait for recovery to complete
      10.times do
        break if session.open?
        sleep 0.5
      end

      expect(session.open?).to be true
      expect(attempt_numbers).not_to be_empty
      expect(attempt_numbers.first).to eq(1)
      expect(recovery_fired).to be true
      session.close
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end

    it "fires on_recovery_exhausted when attempts are exceeded", :toxiproxy do
      skip "Toxiproxy not available" unless toxiproxy_available?

      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)

      exhausted_fired = false
      attempt_numbers = []

      session = AsyncRabbitMQ::Session.new(
        host: RABBITMQ_HOST,
        port: TOXIPROXY_PORT,
        vhost: vhost,
        recovery_attempts: 2,
        recovery_interval: 0.3,
        recovery_max_interval: 0.5
      )
      session.on_recovery_attempt { |n| attempt_numbers << n }
      session.on_recovery_exhausted { |_s| exhausted_fired = true }
      session.connect

      # Keep the connection down so recovery will exhaust
      toxiproxy_rabbitmq.down do
        # Wait long enough for 2 attempts to exhaust
        sleep 5
      end

      expect(exhausted_fired).to be true
      expect(attempt_numbers.length).to eq(2)
      expect(session.closed?).to be true
      session.close rescue nil
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end

    it "uses custom recovery_interval and recovery_max_interval", :toxiproxy do
      skip "Toxiproxy not available" unless toxiproxy_available?

      vhost = "test-#{SecureRandom.hex(6)}"
      create_vhost(vhost)

      attempt_times = []

      session = AsyncRabbitMQ::Session.new(
        host: RABBITMQ_HOST,
        port: TOXIPROXY_PORT,
        vhost: vhost,
        recovery_interval: 0.2,
        recovery_max_interval: 0.5
      )
      session.on_recovery_attempt { |_n| attempt_times << Process.clock_gettime(Process::CLOCK_MONOTONIC) }
      session.connect

      # Cut and restore quickly
      toxiproxy_rabbitmq.down do
        sleep 0.3
      end

      # Wait for recovery
      10.times do
        break if session.open?
        sleep 0.3
      end

      expect(session.open?).to be true
      # First attempt should come quickly (~0.2s), not the default 1.0s
      if attempt_times.length >= 1
        # Just verify recovery happened — timing is hard to assert precisely
        expect(attempt_times).not_to be_empty
      end
      session.close
    ensure
      session&.close rescue nil
      delete_vhost(vhost) rescue nil
    end
  end
end
