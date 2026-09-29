require "spec_helper"

# A zero-length body has a content header and no body frames at all: neither
# RabbitMQ nor amq-protocol emits one. The frame reader must route on the
# header, or the delivery never completes and the next method frame on that
# channel is mistaken for its content.
RSpec.describe "empty message bodies", :integration do

  it "delivers an empty body to basic_get" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.empty.get.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("", routing_key: q.name)
      sleep 0.2

      msg = ch.basic_get(q.name, manual_ack: false)
      expect(msg).not_to be_nil
      expect(msg[2]).to eq("")
      ch.close
    end
  end

  it "delivers an empty body to a consumer" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 10)
      qname = "test.empty.consume.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)

      got = []
      ch.basic_consume(qname, manual_ack: false) { |_di, _h, body| got << body }

      ch.basic_publish("", routing_key: q.name)
      30.times { break unless got.empty?; sleep 0.1 }

      expect(got).to eq([""])
      ch.close
    end
  end

  # The original failure mode: the empty message never routes, so the *next*
  # message's method frame is consumed as its body and everything after it is
  # skewed. Interleaving empty and non-empty bodies catches that.
  it "keeps the channel in step when empty and non-empty bodies interleave" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.basic_qos(prefetch_count: 20)
      qname = "test.empty.mixed.#{SecureRandom.hex(4)}"
      q = ch.queue(qname, durable: true)

      sent = ["", "one", "", "", "two", "three", ""]
      got  = []
      ch.basic_consume(qname, manual_ack: false) { |_di, _h, body| got << body }

      sent.each { |b| ch.basic_publish(b, routing_key: q.name) }
      50.times { break if got.size >= sent.size; sleep 0.1 }

      expect(got).to eq(sent)
      ch.close
    end
  end

  it "round-trips an empty body with headers intact" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.empty.props.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("", routing_key: q.name, headers: { "kind" => "tombstone" }, message_id: "m-1")
      sleep 0.2

      _di, header, body = ch.basic_get(q.name, manual_ack: false)
      expect(body).to eq("")
      expect(header.properties[:message_id]).to eq("m-1")
      expect(header.properties[:headers]).to include("kind" => "tombstone")
      ch.close
    end
  end
end
