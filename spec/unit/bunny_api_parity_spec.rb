require "spec_helper"

# Unit tests for P2 Bunny API parity features that don't need a live broker.
RSpec.describe "Bunny API parity — unit tests" do

  # ---------------------------------------------------------------------------
  # Queue predicates
  # ---------------------------------------------------------------------------

  describe AsyncRabbitMQ::Queue do
    let(:channel) { double("Channel") }

    it "durable? reflects the construction parameter" do
      q = described_class.new("q1", 0, 0, channel, durable: true)
      expect(q.durable?).to be true

      q2 = described_class.new("q2", 0, 0, channel)
      expect(q2.durable?).to be false
    end

    it "exclusive? reflects the construction parameter" do
      q = described_class.new("q1", 0, 0, channel, exclusive: true)
      expect(q.exclusive?).to be true
    end

    it "auto_delete? reflects the construction parameter" do
      q = described_class.new("q1", 0, 0, channel, auto_delete: true)
      expect(q.auto_delete?).to be true
    end

    it "server_named? is true for amq.gen- prefixed names" do
      q = described_class.new("amq.gen-abc123", 0, 0, channel)
      expect(q.server_named?).to be true
    end

    it "server_named? is false for user-provided names" do
      q = described_class.new("my-queue", 0, 0, channel)
      expect(q.server_named?).to be false
    end

    it "status calls passive declare and returns refreshed counts" do
      refreshed_q = described_class.new("q1", 42, 3, channel)
      allow(channel).to receive(:queue).with("q1", passive: true).and_return(refreshed_q)

      q = described_class.new("q1", 0, 0, channel)
      result = q.status

      expect(result).to eq({ message_count: 42, consumer_count: 3 })
      expect(q.message_count).to eq(42)
      expect(q.consumer_count).to eq(3)
    end
  end

  # ---------------------------------------------------------------------------
  # Exchange predicates
  # ---------------------------------------------------------------------------

  describe AsyncRabbitMQ::Exchange do
    it "durable? reflects the construction parameter" do
      ex = described_class.new("ex1", :direct, nil, durable: true)
      expect(ex.durable?).to be true

      ex2 = described_class.new("ex2", :direct, nil)
      expect(ex2.durable?).to be false
    end

    it "auto_delete? reflects the construction parameter" do
      ex = described_class.new("ex1", :direct, nil, auto_delete: true)
      expect(ex.auto_delete?).to be true
    end

    it "internal? reflects the construction parameter" do
      ex = described_class.new("ex1", :fanout, nil, internal: true)
      expect(ex.internal?).to be true

      ex2 = described_class.new("ex2", :fanout, nil)
      expect(ex2.internal?).to be false
    end

    it "predefined? is true for the default exchange" do
      ex = described_class.new("", :direct, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for amq.direct" do
      ex = described_class.new("amq.direct", :direct, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for amq.fanout" do
      ex = described_class.new("amq.fanout", :fanout, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for amq.topic" do
      ex = described_class.new("amq.topic", :topic, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for amq.headers" do
      ex = described_class.new("amq.headers", :headers, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for amq.match" do
      ex = described_class.new("amq.match", :headers, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is true for amq.rabbitmq.trace" do
      ex = described_class.new("amq.rabbitmq.trace", :topic, nil)
      expect(ex.predefined?).to be true
    end

    it "predefined? is false for user-declared exchanges" do
      ex = described_class.new("my.custom.exchange", :direct, nil)
      expect(ex.predefined?).to be false
    end

    it "on_return delegates to the channel" do
      channel = double("Channel")
      block = proc { |r, h, b| }
      expect(channel).to receive(:on_return) { |&blk| expect(blk).to eq(block) }

      ex = described_class.new("ex1", :direct, channel)
      ex.on_return(&block)
    end
  end
end
