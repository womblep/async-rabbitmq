require "spec_helper"

# Step 10: basic.return handling — mandatory flag, unroutable messages.
RSpec.describe "basic.return handling", :integration do

  it "delivers basic.return to on_return handler for unroutable mandatory message" do
    isolated_session do |session, _|
      ch = session.open_channel

      returned = []
      ch.on_return { |_ret, _headers, body| returned << body.to_s }

      # Publish to a non-existent routing key with mandatory: true
      ch.basic_publish("unroutable", exchange: "", routing_key: "no.such.queue.#{SecureRandom.hex(8)}", mandatory: true)

      # Give the broker time to return the message
      sleep 0.3

      expect(returned).not_to be_empty
      expect(returned.first).to include("unroutable")
      ch.close
    end
  end

  it "logs a warning when basic.return is received with no handler" do
    isolated_session do |session, _|
      ch = session.open_channel

      logger = session.instance_variable_get(:@logger)
      # Only the unhandled-return warning is asserted; any other warning the
      # session logs must not fail the example.
      allow(logger).to receive(:warn).and_call_original
      expect(logger).to receive(:warn).with(/Unhandled basic\.return/).at_least(:once)

      ch.basic_publish("unroutable", exchange: "", routing_key: "no.such.#{SecureRandom.hex(8)}", mandatory: true)
      sleep 0.3
      ch.close
    end
  end
end
