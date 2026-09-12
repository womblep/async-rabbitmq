require "spec_helper"
require "async_rabbitmq/pool"

# AsyncRabbitMQ::Pool — opt-in connection pool on Async::Pool::Controller.
RSpec.describe AsyncRabbitMQ::Pool, :integration do

  it "hands out a connected Session and reuses it for subsequent acquisitions" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    pool = described_class.new(max: 2, host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)

    first = nil
    pool.acquire do |session|
      expect(session).to be_a(AsyncRabbitMQ::Session)
      expect(session.open?).to be true
      session.with_channel { |ch| ch.queue("test.pool.#{SecureRandom.hex(4)}") }
      first = session
    end

    # A Session's concurrency is its channel_max, so the pool reuses it rather
    # than opening a second connection.
    pool.acquire do |session|
      expect(session).to equal(first)
      expect(session.open?).to be true
    end

    pool.close
    expect(first.closed?).to be true
  ensure
    pool&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  it "supports acquire/release without a block" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    pool = described_class.new(max: 1, host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost)

    session = pool.acquire
    expect(session.open?).to be true
    pool.release(session)
    pool.close
    expect(session.closed?).to be true
  ensure
    pool&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
