require "spec_helper"

# Session#recover_topology_on: a hard failure mid-replay (an RPC timeout from a
# broker that stopped answering) stops the replay — each remaining entity would
# eat its own full timeout — but must be REPORTED: returned to the caller,
# emitted as recovery.topology_incomplete, and reflected as
# topology_complete: false in the recovery.succeeded payload.
RSpec.describe "topology replay failure reporting" do
  let(:session) { AsyncRabbitMQ::Session.new(logger: AsyncRabbitMQ::Log.silent) }

  def record_topology
    session.topology.record_exchange(1, "ex.one", "direct", durable: true, auto_delete: false,
                                                            internal: false, arguments: {})
    session.topology.record_queue(1, "q.one", durable: true, exclusive: false, auto_delete: false,
                                              arguments: {}, server_named: false)
    session.topology.record_queue(1, "q.two", durable: true, exclusive: false, auto_delete: false,
                                              arguments: {}, server_named: false)
  end

  it "returns nil and emits nothing when every entity replays" do
    record_topology
    events = []
    session.on_event("recovery.topology_incomplete") { |n, p| events << [n, p] }

    channel = double("Channel", channel_id: 1)
    allow(channel).to receive(:recover_exchange)
    allow(channel).to receive(:recover_queue)

    expect(session.send(:recover_topology_on, [channel])).to be_nil
    expect(events).to be_empty
  end

  it "stops at a hard failure, returns it, and emits recovery.topology_incomplete" do
    record_topology
    events = []
    session.on_event("recovery.topology_incomplete") { |_n, p| events << p }

    channel = double("Channel", channel_id: 1)
    allow(channel).to receive(:recover_exchange)
    # First queue replays; the second times out — a broker that stopped answering.
    allow(channel).to receive(:recover_queue) do |q|
      raise AsyncRabbitMQ::RpcTimeoutError, "no reply" if q.name == "q.two"
    end

    failure = session.send(:recover_topology_on, [channel])

    expect(failure).to include(kind: :queue, entity: "q.two", error: "AsyncRabbitMQ::RpcTimeoutError")
    expect(failure[:replayed]).to eq(2)  # ex.one and q.one made it
    expect(failure[:skipped]).to eq(0)   # q.two was the last entity
    expect(events).to eq([failure])
  end

  it "counts the entities skipped after the failure" do
    record_topology
    channel = double("Channel", channel_id: 1)
    # The exchange itself fails: both queues after it are skipped.
    allow(channel).to receive(:recover_exchange).and_raise(AsyncRabbitMQ::RpcTimeoutError, "no reply")

    failure = session.send(:recover_topology_on, [channel])

    expect(failure).to include(kind: :exchange, entity: "ex.one", replayed: 0, skipped: 2)
  end
end
