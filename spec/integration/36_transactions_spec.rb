require "spec_helper"

# AMQP 0-9-1 transactions: tx.select / tx.commit / tx.rollback.
RSpec.describe "transactions", :integration do

  def ready_count(session, name)
    session.with_channel { |ch| ch.queue(name, passive: true).message_count }
  end

  it "discards publishes on rollback and delivers them on commit" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tx.#{SecureRandom.hex(4)}", durable: true)
      expect(ch.using_tx?).to be false
      ch.tx_select
      expect(ch.using_tx?).to be true

      ch.basic_publish("rolled back", routing_key: q.name)
      ch.tx_rollback
      expect(ready_count(session, q.name)).to eq(0)

      ch.basic_publish("committed", routing_key: q.name)
      expect(ready_count(session, q.name)).to eq(0)   # held until commit
      ch.tx_commit
      expect(ready_count(session, q.name)).to eq(1)
      _di, _h, body = ch.basic_get(q.name, manual_ack: false)
      expect(body).to eq("committed")
      ch.close
    end
  end

  it "holds acks until commit" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tx.ack.#{SecureRandom.hex(4)}", durable: true)
      ch.basic_publish("ack in tx", routing_key: q.name)
      sleep 0.1
      ch.tx_select

      di, _h, _body = ch.basic_get(q.name)   # manual ack
      ch.basic_ack(di.delivery_tag)
      ch.tx_rollback                          # ack discarded -> message still unacked on this channel
      ch.close                                # requeued on close
      sleep 0.2
      expect(ready_count(session, q.name)).to eq(1)
    end
  end

  it "restores transactional mode after connection recovery" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tx.recover.#{SecureRandom.hex(4)}", durable: true)
      ch.tx_select

      recover_connection!(session)
      expect(ch.open?).to be true

      # Without tx.select on the new channel, tx.commit would be a 406 channel error.
      ch.basic_publish("after recovery", routing_key: q.name)
      expect { ch.tx_commit }.not_to raise_error
      expect(ready_count(session, q.name)).to eq(1)
      expect(ch.using_tx?).to be true
      ch.close
    end
  end

  it "rejects switching a confirm-mode channel to transactions with a channel error" do
    isolated_session do |session, _|
      ch = session.open_channel
      ch.confirm_select
      expect { ch.tx_select }.to raise_error(AsyncRabbitMQ::ChannelError) { |e| expect(e.code).to eq(406) }
      expect(ch.closed?).to be true
    end
  end

  it "re-selects tx mode after a connection drop; uncommitted work is gone, the next transaction works" do
    isolated_session do |session, _|
      ch = session.open_channel
      q  = ch.queue("test.tx.recover.#{SecureRandom.hex(4)}", durable: true)
      ch.tx_select

      # Buffered broker-side, never committed — it dies with the old channel.
      # That loss is the documented contract ("work that was uncommitted when
      # the connection dropped is lost, as with any client"); this pins it.
      ch.basic_publish("uncommitted, lost with the channel", routing_key: q.name)

      recover_connection!(session)

      expect(ch.using_tx?).to be true
      # Committing on the recovered channel must not raise — and must not
      # resurrect the pre-drop publish: the new channel's transaction is empty.
      ch.tx_commit
      expect(ready_count(session, q.name)).to eq(0)

      # The recovered channel is genuinely transactional, not just flagged so.
      ch.basic_publish("committed after recovery", routing_key: q.name)
      expect(ready_count(session, q.name)).to eq(0)   # held until commit
      ch.tx_commit
      expect(ready_count(session, q.name)).to eq(1)
    end
  end
end
