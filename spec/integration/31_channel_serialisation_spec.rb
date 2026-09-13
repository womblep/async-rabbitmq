require "spec_helper"

# A channel may be shared by many fibers. Request/reply operations must be
# serialised (AMQP replies carry no correlation id) and one publish's frames
# must reach the wire contiguously even when write_frame yields under
# backpressure.
RSpec.describe "same-channel concurrency", :integration do

  it "returns the right reply to each of many concurrent RPCs on one channel" do
    isolated_session do |session, _|
      ch = session.open_channel

      tasks = 20.times.map do |i|
        Async::Task.current.async do
          q = ch.queue("test.rpc.#{i}", durable: true)
          [i, q.name, q.message_count]
        end
      end
      results = tasks.map(&:wait)

      results.each do |i, name, count|
        expect(name).to eq("test.rpc.#{i}")
        expect(count).to eq(0)
      end
      expect(ch.open?).to be true
      expect(session.open?).to be true
      ch.close
    end
  end

  it "does not interleave frames of concurrent multi-frame publishes under backpressure" do
    # Small frame_max => many body frames per message; enough messages to fill
    # the 1024-frame write queue so that write_frame yields mid-message (8192 is the
    # smallest frame_max this broker accepts).
    isolated_session(frame_max: 8192) do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.interleave.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select

      publishers = 12
      payloads   = publishers.times.map { |i| (("a".ord + i).chr * (1024 * 1024)).b }   # 1 MiB, 128 frames each at 8 KiB

      tasks = payloads.map do |payload|
        Async::Task.current.async { ch.basic_publish(payload, routing_key: q.name) }
      end
      tasks.each(&:wait)
      expect(ch.wait_for_confirms).to be true
      expect(session.open?).to be true   # an interleave would have produced a 505 connection close

      received = []
      while (msg = ch.basic_get(q.name, manual_ack: false))
        received << msg[2]
      end
      expect(received.size).to eq(publishers)
      expect(received.map(&:bytesize).uniq).to eq([1024 * 1024])
      received.each { |body| expect(body.chars.uniq.size).to eq(1) }   # each body is one letter repeated
      ch.close
    end
  end

  it "assigns confirm tags in wire order for concurrent publishers" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tags.#{SecureRandom.hex(4)}", durable: true)
      ch.confirm_select

      tags = 10.times.map { |i| Async::Task.current.async { ch.basic_publish("m#{i}", routing_key: q.name) } }.map(&:wait)
      expect(tags.sort).to eq((1..10).to_a)
      expect(ch.wait_for_confirms).to be true
      expect(ch.unconfirmed_tags).to be_empty
      ch.close
    end
  end
end
