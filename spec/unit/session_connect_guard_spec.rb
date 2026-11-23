require "spec_helper"

RSpec.describe AsyncRabbitMQ::Session do
  let(:session) { described_class.new(logger: AsyncRabbitMQ::Log.silent) }

  describe "#connect while not cleanly closed" do
    it "refuses while the session is recovering" do
      session.instance_variable_set(:@state, :recovering)
      expect { session.connect }.to raise_error(AsyncRabbitMQ::NotOpenError, /recovering/)
    end

    it "refuses while a recovery task is in flight even if the state has moved on" do
      session.instance_variable_set(:@recovery_in_progress, true)
      expect { session.connect }.to raise_error(AsyncRabbitMQ::NotOpenError, /recovering/)
    end

    it "refuses a concurrent connect" do
      session.instance_variable_set(:@connecting, true)
      expect { session.connect }.to raise_error(AsyncRabbitMQ::NotOpenError, /connecting/)
    end

    it "still reports an already-open session as connected" do
      session.instance_variable_set(:@state, :open)
      expect { session.connect }.to raise_error(AsyncRabbitMQ::NotOpenError, /already connected/)
    end
  end
end
