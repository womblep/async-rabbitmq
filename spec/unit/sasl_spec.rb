require "spec_helper"
require "async_rabbitmq/sasl"

RSpec.describe AsyncRabbitMQ::SASL do

  describe AsyncRabbitMQ::SASL::Plain do
    subject { described_class.new("user", "pass") }

    it "returns PLAIN as mechanism name" do
      expect(subject.mechanism_name).to eq("PLAIN")
    end

    it "formats initial response as NUL-delimited triple" do
      expect(subject.initial_response).to eq("\x00user\x00pass")
    end

    it "echoes the initial response on challenge" do
      expect(subject.challenge_response("anything")).to eq("\x00user\x00pass")
    end
  end

  describe AsyncRabbitMQ::SASL::External do
    subject { described_class.new }

    it "returns EXTERNAL as mechanism name" do
      expect(subject.mechanism_name).to eq("EXTERNAL")
    end

    it "sends empty initial response" do
      expect(subject.initial_response).to eq("")
    end

    it "sends empty challenge response" do
      expect(subject.challenge_response("anything")).to eq("")
    end
  end

  describe AsyncRabbitMQ::SASL::CRRabbitMQDemo do
    subject { described_class.new("demo_user", "demo_pass") }

    it "returns RABBIT-CR-DEMO as mechanism name" do
      expect(subject.mechanism_name).to eq("RABBIT-CR-DEMO")
    end

    it "sends username as initial response" do
      expect(subject.initial_response).to eq("demo_user")
    end

    it "sends password as challenge response" do
      expect(subject.challenge_response("Please tell me your password")).to eq("demo_pass")
    end
  end

  describe ".negotiate" do
    it "selects PLAIN when broker offers PLAIN AMQPLAIN" do
      mech = described_class.negotiate("PLAIN AMQPLAIN", username: "u", password: "p")
      expect(mech).to be_a(AsyncRabbitMQ::SASL::Plain)
    end

    it "prefers EXTERNAL over PLAIN when both offered" do
      mech = described_class.negotiate("PLAIN EXTERNAL", username: "u", password: "p")
      expect(mech).to be_a(AsyncRabbitMQ::SASL::External)
    end

    it "honors preferred: override" do
      mech = described_class.negotiate("PLAIN EXTERNAL", preferred: "PLAIN", username: "u", password: "p")
      expect(mech).to be_a(AsyncRabbitMQ::SASL::Plain)
    end

    it "raises AuthenticationError when preferred mechanism not offered" do
      expect {
        described_class.negotiate("PLAIN", preferred: "EXTERNAL", username: "u", password: "p")
      }.to raise_error(AsyncRabbitMQ::AuthenticationError, /EXTERNAL not offered/)
    end

    it "raises AuthenticationError when no common mechanism" do
      expect {
        described_class.negotiate("GSSAPI DIGEST-MD5", username: "u", password: "p")
      }.to raise_error(AsyncRabbitMQ::AuthenticationError, /No supported SASL mechanism/)
    end

    it "selects RABBIT-CR-DEMO when explicitly preferred" do
      mech = described_class.negotiate("PLAIN RABBIT-CR-DEMO", preferred: "RABBIT-CR-DEMO", username: "u", password: "p")
      expect(mech).to be_a(AsyncRabbitMQ::SASL::CRRabbitMQDemo)
    end
  end
end
