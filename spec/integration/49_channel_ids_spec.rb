require "spec_helper"

# Channel ids come back when a channel closes, and the client refuses to open
# more channels than the negotiated channel_max. Before, the id was a bare
# counter: a process that opened and closed a channel per unit of work walked
# it to 65535, after which the id wrapped to 0 and the broker dropped the
# connection; and opening one channel too many was left to the broker, which
# answers with a 530 that also drops the connection. channel_max: 4 makes both
# arrive in a handful of opens instead of thousands.
RSpec.describe "channel id allocation", :integration do

  it "reuses the ids of closed channels instead of counting past channel_max" do
    isolated_session(channel_max: 4) do |session, _|
      expect(session.concurrency).to eq(4)

      ids = 12.times.map do
        channel = session.open_channel
        channel.close
        channel.channel_id
      end

      expect(ids).to all(eq(1)) # nothing else is open, so the lowest free id every time
      expect(session.open?).to be true
    end
  end

  it "hands out the lowest free id" do
    isolated_session(channel_max: 8) do |session, _|
      a, b, c = 3.times.map { session.open_channel }
      expect([a, b, c].map(&:channel_id)).to eq([1, 2, 3])

      b.close
      expect(session.open_channel.channel_id).to eq(2)
      expect(session.open_channel.channel_id).to eq(4)
    end
  end

  it "refuses to exceed channel_max rather than letting the broker close the connection" do
    isolated_session(channel_max: 4) do |session, _|
      held = 4.times.map { session.open_channel }

      expect { session.open_channel }.to raise_error(AsyncRabbitMQ::ChannelLimitError, /channel_max 4 reached/)
      expect(session.open?).to be true
      expect(held).to all(be_open)

      held.first.close
      expect(session.open_channel.channel_id).to eq(1)
    end
  end

  it "frees the id of a channel the broker closed" do
    isolated_session(channel_max: 4) do |session, _|
      doomed = session.open_channel
      expect { doomed.queue("no.such.queue.#{SecureRandom.hex(4)}", passive: true) }
        .to raise_error(AsyncRabbitMQ::ChannelError)
      expect(doomed.closed?).to be true

      replacement = session.open_channel
      expect(replacement.channel_id).to eq(doomed.channel_id)

      # The old object cannot be reopened in place while its id is taken.
      expect { doomed.reopen }.to raise_error(AsyncRabbitMQ::Error, /in use by another channel/)
    end
  end

  it "keeps ids across a reconnect" do
    vhost = "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    session = AsyncRabbitMQ::Session.new(host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost, channel_max: 4)
    session.connect
    kept = session.open_channel
    kept.durable_queue("test.ids.recovery.#{SecureRandom.hex(4)}")
    gone = session.open_channel
    gone.close

    recover_connection!(session)

    expect(kept.open?).to be true
    expect(kept.channel_id).to eq(1)
    expect(session.open_channel.channel_id).to eq(2)
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end
end
