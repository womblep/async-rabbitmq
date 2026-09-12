require "spec_helper"

# Step 16: Multi-frame body reassembly.
# AMQP requires large payloads (> frame_max) to be split across multiple body frames.
# The Reader Task must reassemble them correctly — silent truncation is data corruption.
RSpec.describe "Multi-frame body reassembly", :integration do

  it "sends and receives a payload spanning 3 body frames" do
    isolated_session do |session, _|
      frame_max = session.frame_max
      # 3× frame_max ensures at least 3 body frames
      payload = ("A" * frame_max) + ("B" * frame_max) + ("C" * (frame_max / 2))

      ch  = session.open_channel
      q   = ch.queue("test.multiframe", durable: true)

      ch.basic_publish(payload, routing_key: q.name)

      _di, _headers, received = ch.basic_get(q.name)

      expect(received.bytesize).to eq(payload.bytesize)
      expect(received).to eq(payload.b)
      ch.close
    end
  end

  it "reassembles body correctly when frame_max is very small" do
    # Use frame_max: 8192 (the smallest value this broker accepts) so a 200 KB
    # payload spans ~25 body frames, exercising the multi-frame reassembly path.
    isolated_session(frame_max: 8192) do |session, _|
      payload = "X" * 200_000

      ch = session.open_channel
      q  = ch.queue("test.smallframe", durable: true)

      ch.basic_publish(payload, routing_key: q.name)
      _di, _headers, received = ch.basic_get(q.name)

      expect(received.bytesize).to eq(payload.bytesize)
      ch.close
    end
  end
end
