require "spec_helper"
require "net/http"
require "json"

# Bunny-style declaration helpers: durable_queue, quorum_queue, stream, plus
# the queue and exchange type constants.
RSpec.describe "queue helpers and type constants", :integration do

  def queue_info(vhost, name)
    uri = URI("http://#{RABBITMQ_HOST}:#{RABBITMQ_MGMT}/api/queues/#{URI.encode_www_form_component(vhost)}/#{URI.encode_www_form_component(name)}")
    req = Net::HTTP::Get.new(uri)
    req.basic_auth("guest", "guest")
    res = Net::HTTP.start(uri.host, uri.port) { |http| http.request(req) }
    JSON.parse(res.body)
  end

  it "durable_queue declares a durable, non-exclusive, non-auto-delete classic queue" do
    isolated_session do |session, vhost|
      ch = session.open_channel
      q  = ch.durable_queue("test.durable.#{SecureRandom.hex(4)}")
      expect(q.durable?).to be true
      expect(q.exclusive?).to be false
      expect(q.auto_delete?).to be false
      sleep 0.3
      expect(queue_info(vhost, q.name)["type"]).to eq("classic")
      ch.close
    end
  end

  it "quorum_queue declares a quorum queue" do
    isolated_session do |session, vhost|
      ch = session.open_channel
      q  = ch.quorum_queue("test.quorum.#{SecureRandom.hex(4)}")
      expect(q.durable?).to be true
      sleep 0.5
      expect(queue_info(vhost, q.name)["type"]).to eq("quorum")
      ch.close
    end
  end

  it "stream declares a stream" do
    isolated_session do |session, vhost|
      ch = session.open_channel
      q  = ch.stream("test.stream.#{SecureRandom.hex(4)}")
      expect(q.durable?).to be true
      sleep 0.5
      expect(queue_info(vhost, q.name)["type"]).to eq("stream")
      expect(session.queue_exists?(q.name)).to be true
      ch.close
    end
  end

  it "rejects nil or empty names where a server-named queue makes no sense" do
    isolated_session do |session, _|
      ch = session.open_channel
      expect { ch.quorum_queue("") }.to raise_error(ArgumentError, /name/)
      expect { ch.quorum_queue(nil) }.to raise_error(ArgumentError, /name/)
      expect { ch.stream("") }.to raise_error(ArgumentError, /name/)
      expect { ch.durable_queue("") }.to raise_error(ArgumentError, /name/)
      expect(ch.open?).to be true
      ch.close
    end
  end

  it "exposes queue and exchange type constants" do
    expect(AsyncRabbitMQ::Queue::Types::CLASSIC).to eq("classic")
    expect(AsyncRabbitMQ::Queue::Types::QUORUM).to eq("quorum")
    expect(AsyncRabbitMQ::Queue::Types::STREAM).to eq("stream")
    expect(AsyncRabbitMQ::Exchange::TYPE_DIRECT).to eq("direct")
    expect(AsyncRabbitMQ::Exchange::TYPE_FANOUT).to eq("fanout")
    expect(AsyncRabbitMQ::Exchange::TYPE_TOPIC).to eq("topic")
    expect(AsyncRabbitMQ::Exchange::TYPE_HEADERS).to eq("headers")
    expect(AsyncRabbitMQ::Exchange::TYPE_CONSISTENT_HASH).to eq("x-consistent-hash")
  end
end
