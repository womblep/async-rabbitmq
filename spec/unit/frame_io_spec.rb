require "spec_helper"

RSpec.describe AsyncRabbitMQ::FrameIO do
  # A socket double that accepts writes and blocks reads until closed, like a
  # healthy connection with nothing to say.
  class FakeSocket
    def initialize
      @reader, @writer = IO.pipe
      @written = +"".b
    end

    attr_reader :written

    def write(data)
      @written << data
      data.bytesize
    end

    def read(n)
      @reader.read(n) # parks until close
    end

    def close
      @reader.close unless @reader.closed?
      @writer.close unless @writer.closed?
    end
  end

  let(:logger) { AsyncRabbitMQ::Log.silent }

  it "releases a publisher parked on a full write queue when stopped" do
    stub_const("AsyncRabbitMQ::FrameIO::WRITE_QUEUE_LIMIT", 2)

    Sync do |task|
      io = described_class.new(FakeSocket.new, logger: logger)
      # No writer task running, so the queue fills and the third write parks.
      io.write_frame("a")
      io.write_frame("b")

      parked_done = false
      publisher = task.async do
        io.write_frame("c") # parks: queue is full and nothing pops
        parked_done = true
      end

      task.yield until publisher.running? # let it reach the parked push
      expect(parked_done).to be false

      io.stop

      publisher.wait
      expect(parked_done).to be true
    end
  end

  it "discards writes that arrive after stop instead of blocking" do
    Sync do
      io = described_class.new(FakeSocket.new, logger: logger)
      io.stop
      expect { io.write_frame("late") }.not_to raise_error
    end
  end

  it "ends the writer task when the queue is closed by stop" do
    Sync do |task|
      socket = FakeSocket.new
      io = described_class.new(socket, logger: logger)
      io.start(task: task)
      io.write_frame("hello")
      task.yield while socket.written.empty? # writer drains the frame
      expect(socket.written).to eq("hello")
      io.stop
      # Reaching here without a hang is the assertion: stop returned with the
      # writer parked on an empty queue, which only a queue close can wake.
    end
  end
end
