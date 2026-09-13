require "spec_helper"

# basic_publish hands frames to the writer fiber; it does not write them to the
# socket. If the socket stalls, the close handshake gives up after 5s and
# whatever is still queued goes with the connection. That is fire-and-forget
# publishing behaving as specified (confirms are how a publisher gets
# certainty), but the client must not do it silently: a publisher without
# confirms has no other way to learn that its last messages never left.
RSpec.describe "closing with frames still queued", :integration do

  it "reports how many queued writes the close discarded" do
    log = StringIO.new
    isolated_session(logger: Logger.new(log)) do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.close.unwritten.#{SecureRandom.hex(4)}")
      channel.basic_publish("queued", routing_key: queue.name)

      frame_io = session.instance_variable_get(:@frame_io)
      allow(frame_io).to receive(:pending_writes).and_return(7)
      # A broker that never answers connection.close, or a socket too backed up
      # to carry the frame within the 5s handshake budget.
      allow(session).to receive(:wait_channel0_method).and_raise(Async::TimeoutError)

      session.close

      expect(log.string).to match(/Close discarded 7 queued write\(s\) \(Async::TimeoutError\)/)
      expect(session.closed?).to be true
    end
  end

  it "says nothing when the writer had drained before the close" do
    log = StringIO.new
    isolated_session(logger: Logger.new(log)) do |session, _|
      channel = session.open_channel
      queue   = channel.durable_queue("test.close.drained.#{SecureRandom.hex(4)}")
      channel.confirm_select
      100.times { |i| channel.basic_publish("m#{i}", routing_key: queue.name) }
      channel.wait_for_confirms

      expect(session.instance_variable_get(:@frame_io).pending_writes).to eq(0)

      session.close

      expect(log.string).not_to include("Close discarded")
    end
  end

  it "counts one queued write per publish, whatever the batch carries" do
    isolated_session do |session, _|
      channel  = session.open_channel
      queue    = channel.durable_queue("test.close.counting.#{SecureRandom.hex(4)}")
      frame_io = session.instance_variable_get(:@frame_io)

      expect(frame_io.pending_writes).to be_a(Integer)
      channel.basic_publish_batch(Array.new(50, "batched"), routing_key: queue.name)
      channel.confirm_select
      channel.basic_publish("done", routing_key: queue.name)
      channel.wait_for_confirms

      # Everything written: the queue is a hand-off point, not a buffer of record.
      expect(frame_io.pending_writes).to eq(0)
      expect(channel.queue(queue.name, passive: true).message_count).to eq(51)
    end
  end
end
