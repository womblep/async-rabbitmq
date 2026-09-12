require "spec_helper"

# Step 17: Concurrent channel stress test.
# 100 channels publishing simultaneously — no frame interleaving, no delivery corruption.
RSpec.describe "Concurrent channel stress test", :integration do

  it "handles 100 concurrent channels publishing without frame interleaving" do
    isolated_session do |session, _|
      channel_count = 100
      channels      = channel_count.times.map { session.open_channel }
      queues        = channels.each_with_index.map do |ch, i|
        ch.queue("test.stress.#{i}", durable: true)
      end

      # Publish from all channels concurrently
      tasks = channels.each_with_index.map do |ch, i|
        Async { ch.basic_publish("msg-#{i}", routing_key: queues[i].name) }
      end
      tasks.each(&:wait)

      # Verify each queue has exactly its message
      channels.each_with_index do |ch, i|
        _di, _h, body = ch.basic_get(queues[i].name)
        expect(body).to eq("msg-#{i}".b), "Queue #{i} got wrong message: #{body.inspect}"
      end

      channels.each(&:close)
    end
  end

  it "10 channels each publishing 100 messages — no corruption" do
    isolated_session do |session, _|
      channels = 10.times.map { session.open_channel }
      queues   = channels.each_with_index.map { |ch, i| ch.queue("test.stress10.#{i}", durable: true) }

      tasks = channels.each_with_index.map do |ch, i|
        Async do
          100.times { |j| ch.basic_publish("#{i}-#{j}", routing_key: queues[i].name) }
        end
      end
      tasks.each(&:wait)

      channels.each_with_index do |ch, i|
        received = []
        while (msg = ch.basic_get(queues[i].name))
          received << msg[2].to_s
        end
        expect(received.size).to eq(100)
      end

      channels.each(&:close)
    end
  end
end
