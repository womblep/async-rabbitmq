require "spec_helper"

# Step 20: passive flag on queue.declare and exchange.declare.
RSpec.describe "Passive declare", :integration do

  it "passive queue declare succeeds when the queue exists" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.queue("test.passive.q", durable: true)

      q = ch.queue("test.passive.q", passive: true)
      expect(q.name).to eq("test.passive.q")
      ch.close
    end
  end

  it "passive queue declare raises ChannelError(404) when the queue does not exist" do
    isolated_session do |session, _|
      ch = session.open_channel

      expect {
        ch.queue("test.passive.nonexistent.#{rand(10_000)}", passive: true)
      }.to raise_error(AsyncRabbitMQ::ChannelError) do |err|
        expect(err.code).to eq(404)
      end
    end
  end

  it "passive exchange declare succeeds when the exchange exists" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.exchange("test.passive.ex", type: :direct, durable: false)

      ex = ch.exchange("test.passive.ex", type: :direct, passive: true)
      expect(ex.name).to eq("test.passive.ex")
      ch.close
    end
  end

  it "passive exchange declare raises ChannelError(404) when the exchange does not exist" do
    isolated_session do |session, _|
      ch = session.open_channel

      expect {
        ch.exchange("test.passive.nonexistent.#{rand(10_000)}", type: :direct, passive: true)
      }.to raise_error(AsyncRabbitMQ::ChannelError) do |err|
        expect(err.code).to eq(404)
      end
    end
  end
end
