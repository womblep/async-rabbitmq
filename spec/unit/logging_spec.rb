require "spec_helper"
require "console/capture"

# Log calls take Console's shape, (subject, message); Logging.resolve adapts
# whatever +logger:+ was given to it, and AsyncRabbitMQ.log_level caps what
# reaches any destination.
RSpec.describe AsyncRabbitMQ::Logging do
  # log_level is process-wide and the suite runs in random order.
  around do |example|
    previous = AsyncRabbitMQ.log_level
    example.run
  ensure
    AsyncRabbitMQ.log_level = previous
  end

  # A Console logger writing into a capture, installed for this fiber only
  # (Console.logger is fiber-local), so the Console default routes into it.
  def with_console_capture
    capture = Console::Capture.new
    previous = Console.logger
    Console.logger = Console::Logger.new(capture, level: :debug)
    yield capture
  ensure
    Console.logger = previous
  end

  # A Logger-style object recording what it is given.
  def recording_logger(levels = %i[debug info warn error fatal])
    received = []
    logger = Object.new
    levels.each { |l| logger.define_singleton_method(l) { |m| received << [l, m] } }
    [logger, received]
  end

  describe ".resolve" do
    it "adapts the Console module, keeping the subject" do
      with_console_capture do |capture|
        logger = described_class.resolve(Console)
        expect(logger).to be_a(AsyncRabbitMQ::Logging::ConsoleAdapter)

        subject_object = Object.new
        logger.warn(subject_object, "through console")

        record = capture.last
        expect(record[:severity]).to eq(:warn)
        expect(record[:subject]).to equal(subject_object)
        expect(record[:arguments]).to eq(["through console"])
      end
    end

    it "forwards to a given Console::Logger instance rather than the current one" do
      capture = Console::Capture.new
      logger  = described_class.resolve(Console::Logger.new(capture, level: :debug))
      logger.error(:subject, "to that instance")
      expect(capture.last[:arguments]).to eq(["to that instance"])
    end

    it "gives every caller its own adapter, so patching one reaches no other" do
      a = described_class.resolve(Console)
      b = described_class.resolve(Console)
      expect(a).not_to equal(b)

      seen = []
      a.define_singleton_method(:warn) { |*args| seen << args }
      with_console_capture do |capture|
        b.warn(:other, "unaffected")
        expect(capture.last[:arguments]).to eq(["unaffected"])
      end
      expect(seen).to be_empty
    end

    it "wraps a Logger-compatible object in a shim that drops the subject" do
      io = StringIO.new
      logger = described_class.resolve(AsyncRabbitMQ::Log.new(io, level: :debug))

      expect(logger).to be_a(AsyncRabbitMQ::Logging::LoggerShim)
      logger.warn(Object.new, "only the message")
      expect(io.string).to include("only the message")
      expect(io.string).not_to include("#<Object")
    end

    it "takes the full Console call shape on a shim, passing on only the message" do
      duck, received = recording_logger(%i[debug info warn error])
      described_class.resolve(duck).error(self, "with extras", RuntimeError.new("x"), exception: RuntimeError.new("y"))
      expect(received).to eq([[:error, "with extras"]])
    end

    it "maps fatal to error for a logger that has no fatal" do
      duck, received = recording_logger(%i[debug info warn error])
      described_class.resolve(duck).fatal(self, "very bad")
      expect(received).to eq([[:error, "very bad"]])
    end

    it "discards everything for nil" do
      logger = described_class.resolve(nil)
      expect(logger).to be_a(AsyncRabbitMQ::Logging::Null)
      expect(logger.error(self, "ignored")).to be_nil
    end

    it "is idempotent, so every component can resolve what it is handed" do
      [Console, AsyncRabbitMQ::Log.silent, nil].each do |given|
        adapter = described_class.resolve(given)
        expect(described_class.resolve(adapter)).to equal(adapter)
      end
    end

    it "rejects something that is not a logger, with a pointer to the options" do
      expect { described_class.resolve(:stdout) }.to raise_error(ArgumentError, /debug, info, warn and error.*Console.*nil/)
    end
  end

  describe "AsyncRabbitMQ.log_level" do
    it "defaults to warnings, so info and debug never reach a Console set to info" do
      AsyncRabbitMQ.log_level = :warn
      with_console_capture do |capture|
        logger = described_class.resolve(Console)
        logger.info(:s, "recovery chatter")
        logger.debug(:s, "frame detail")
        logger.warn(:s, "connection lost")
        expect(capture.map { |r| r[:arguments].first }).to eq(["connection lost"])
      end
    end

    it "caps a Logger-style destination the same way" do
      AsyncRabbitMQ.log_level = :warn
      duck, received = recording_logger
      shim = described_class.resolve(duck)
      shim.info(:s, "dropped")
      shim.error(:s, "kept")
      expect(received).to eq([[:error, "kept"]])
    end

    it "lets more through when raised, and is read on every call" do
      shim_target, received = recording_logger
      shim = described_class.resolve(shim_target)

      AsyncRabbitMQ.log_level = :warn
      shim.debug(:s, "before")
      AsyncRabbitMQ.log_level = :debug
      shim.debug(:s, "after")
      expect(received).to eq([[:debug, "after"]])
    end

    it "silences every session at :off" do
      AsyncRabbitMQ.log_level = :off
      duck, received = recording_logger
      described_class.resolve(duck).fatal(:s, "even this")
      expect(received).to be_empty
    end

    it "refuses an unknown level when it is set, not at the first log call" do
      expect { AsyncRabbitMQ.log_level = :verbose }.to raise_error(ArgumentError, /:debug, :info, :warn/)
      expect { AsyncRabbitMQ.log_level = "info" }.not_to raise_error
      expect(AsyncRabbitMQ.log_level).to eq(:info)
    end
  end

  describe "wired through the components" do
    it "defaults a Session to its own Console adapter, with the logging component as the subject" do
      with_console_capture do |capture|
        session = AsyncRabbitMQ::Session.new(host: "127.0.0.1", port: 1)
        logger  = session.instance_variable_get(:@logger)
        expect(logger).to be_a(AsyncRabbitMQ::Logging::ConsoleAdapter)

        logger.warn(session, "from the session")
        expect(capture.last[:subject]).to equal(session)
      end
    end

    it "lets logger: nil silence a Session entirely" do
      session = AsyncRabbitMQ::Session.new(host: "127.0.0.1", port: 1, logger: nil)
      expect(session.instance_variable_get(:@logger)).to be_a(AsyncRabbitMQ::Logging::Null)
    end

    it "hands a Session's resolved logger on to the channels it opens unchanged" do
      session = double("Session", frame_max: 131_072)
      shim    = described_class.resolve(AsyncRabbitMQ::Log.silent)
      channel = AsyncRabbitMQ::Channel.new(1, session, double("FrameIO"), frame_max: 131_072, logger: shim)
      expect(channel.instance_variable_get(:@logger)).to equal(shim)
    end
  end
end
