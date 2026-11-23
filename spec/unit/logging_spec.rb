require "spec_helper"
require "console/capture"

# Log calls take Console's shape, (subject, message); Logging.resolve adapts
# whatever +logger:+ was given to it.
RSpec.describe AsyncRabbitMQ::Logging do
  # A Console logger writing into a capture, installed for this fiber only
  # (Console.logger is fiber-local), so the module default routes into it.
  def with_console_capture
    capture = Console::Capture.new
    previous = Console.logger
    Console.logger = Console::Logger.new(capture, level: :debug)
    yield capture
  ensure
    Console.logger = previous
  end

  describe ".resolve" do
    it "passes the Console module straight through, keeping the subject" do
      with_console_capture do |capture|
        logger = described_class.resolve(Console)
        expect(logger).to equal(Console)

        subject_object = Object.new
        logger.warn(subject_object, "through console")

        record = capture.last
        expect(record[:severity]).to eq(:warn)
        expect(record[:subject]).to equal(subject_object)
        expect(record[:arguments]).to eq(["through console"])
      end
    end

    it "passes a Console::Logger instance straight through" do
      console_logger = Console::Logger.new(Console::Capture.new)
      expect(described_class.resolve(console_logger)).to equal(console_logger)
    end

    it "wraps a Logger-compatible object in a shim that drops the subject" do
      io = StringIO.new
      plain = AsyncRabbitMQ::Log.new(io, level: :debug)
      logger = described_class.resolve(plain)

      expect(logger).to be_a(AsyncRabbitMQ::Logging::LoggerShim)
      logger.info(Object.new, "only the message")
      expect(io.string).to include("only the message")
      expect(io.string).not_to include("#<Object")
    end

    it "accepts any duck-typed logger, Rails.logger style" do
      received = []
      duck = Object.new
      %i[debug info warn error fatal].each { |l| duck.define_singleton_method(l) { |m| received << [l, m] } }

      described_class.resolve(duck).error(:anything, "boom")
      expect(received).to eq([[:error, "boom"]])
    end

    it "takes the full Console call shape on a shim, passing on only the message" do
      received = []
      duck = Object.new
      %i[debug info warn error].each { |l| duck.define_singleton_method(l) { |m| received << [l, m] } }

      described_class.resolve(duck).error(self, "with extras", RuntimeError.new("x"), exception: RuntimeError.new("y"))
      expect(received).to eq([[:error, "with extras"]])
    end

    it "maps fatal to error for a logger that has no fatal" do
      received = []
      duck = Object.new
      %i[debug info warn error].each { |l| duck.define_singleton_method(l) { |m| received << [l, m] } }

      described_class.resolve(duck).fatal(self, "very bad")
      expect(received).to eq([[:error, "very bad"]])
    end

    it "discards everything for nil" do
      logger = described_class.resolve(nil)
      expect { logger.warn(self, "ignored") }.not_to raise_error
      expect(logger.error(self, "ignored")).to be_nil
    end

    it "is idempotent, so every component can resolve what it is handed" do
      shim = described_class.resolve(AsyncRabbitMQ::Log.silent)
      expect(described_class.resolve(shim)).to equal(shim)
      null = described_class.resolve(nil)
      expect(described_class.resolve(null)).to equal(null)
    end

    it "rejects something that is not a logger, with a pointer to the options" do
      expect { described_class.resolve(:stdout) }.to raise_error(ArgumentError, /debug, info, warn and error.*Console.*nil/)
    end
  end

  describe "wired through the components" do
    it "defaults a Session to Console, with the logging component as the subject" do
      with_console_capture do |capture|
        session = AsyncRabbitMQ::Session.new(host: "127.0.0.1", port: 1)
        session.instance_variable_get(:@logger).warn(session, "from the session")

        expect(capture.last[:subject]).to equal(session)
      end
    end

    it "lets logger: nil silence a Session entirely" do
      session = AsyncRabbitMQ::Session.new(host: "127.0.0.1", port: 1, logger: nil)
      expect(session.instance_variable_get(:@logger)).to be_a(AsyncRabbitMQ::Logging::Null)
    end

    it "hands a Session's resolved logger on to the channels it opens unchanged" do
      session  = double("Session", frame_max: 131_072)
      shim     = described_class.resolve(AsyncRabbitMQ::Log.silent)
      channel  = AsyncRabbitMQ::Channel.new(1, session, double("FrameIO"), frame_max: 131_072, logger: shim)
      expect(channel.instance_variable_get(:@logger)).to equal(shim)
    end
  end
end
