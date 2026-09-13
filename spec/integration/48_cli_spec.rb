require "spec_helper"
require "tmpdir"
require "async_rabbitmq/cli"

# The `async-rabbitmq` command, driven through CLI.start so the exit status and
# both streams can be asserted. Every example talks to the real broker in its
# own vhost, because the command is only worth having if it works end to end.
RSpec.describe AsyncRabbitMQ::CLI, :integration do
  let(:out) { StringIO.new }
  let(:err) { StringIO.new }

  def run(*argv, url:)
    described_class.start(argv + ["--url", url], out: out, err: err)
  end

  def url_for(vhost)
    "amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}/#{URI.encode_www_form_component(vhost)}"
  end

  # A vhost with one durable queue, plus the URL the CLI should use for it.
  def with_queue
    isolated_session do |session, vhost|
      channel = session.open_channel
      queue   = channel.durable_queue("cli.#{SecureRandom.hex(4)}")
      yield(queue.name, url_for(vhost), channel)
    end
  end

  it "publishes, counts and purges" do
    with_queue do |queue, url|
      expect(run("publish", queue, "hello", "--count", "3", url: url)).to eq(0)
      expect(out.string).to include("Published 3 messages to queue #{queue}, all confirmed")

      out.truncate(out.rewind)
      expect(run("inspect", queue, url: url)).to eq(0)
      expect(out.string).to include("#{queue}: 3 messages, 0 consumers")

      out.truncate(out.rewind)
      expect(run("purge", queue, url: url)).to eq(0)
      expect(out.string).to include("3 messages discarded")

      out.truncate(out.rewind)
      run("inspect", queue, url: url)
      expect(out.string).to include("0 messages")
    end
  end

  it "consumes exactly the number asked for and leaves the rest" do
    with_queue do |queue, url|
      run("publish", queue, "payload", "--count", "5", url: url)
      out.truncate(out.rewind)

      expect(run("consume", queue, "--count", "2", "--timeout", "5", url: url)).to eq(0)
      expect(out.string.scan("payload").size).to eq(2)
      expect(out.string).to include("Received 2 messages")

      out.truncate(out.rewind)
      run("inspect", queue, url: url)
      expect(out.string).to include("3 messages")
    end
  end

  it "leaves everything on the queue when peeking" do
    with_queue do |queue, url|
      run("publish", queue, "kept", "--count", "2", url: url)
      out.truncate(out.rewind)

      expect(run("consume", queue, "--peek", "--timeout", "2", url: url)).to eq(0)
      expect(out.string).to include("nothing is acknowledged")
      expect(out.string.scan("kept").size).to eq(2)

      out.truncate(out.rewind)
      run("inspect", queue, url: url)
      expect(out.string).to include("2 messages")
    end
  end

  it "publishes through an exchange with the queue name as the routing key" do
    with_queue do |queue, url, channel|
      exchange = "cli.x.#{SecureRandom.hex(4)}"
      channel.exchange(exchange, type: :topic, durable: true)
      channel.queue_bind(queue, exchange: exchange, routing_key: queue)

      expect(run("publish", queue, "routed", "--exchange", exchange, url: url)).to eq(0)
      expect(out.string).to include("to #{exchange} with routing key #{queue}")

      out.truncate(out.rewind)
      run("inspect", queue, url: url)
      expect(out.string).to include("1 message,")
    end
  end

  it "reads the body from a file and from stdin" do
    with_queue do |queue, url|
      file = File.join(Dir.tmpdir, "cli-body-#{SecureRandom.hex(4)}.txt")
      File.write(file, "from a file")
      begin
        expect(run("publish", queue, "--file", file, url: url)).to eq(0)
      ensure
        File.delete(file)
      end

      allow($stdin).to receive(:tty?).and_return(false)
      allow($stdin).to receive(:binmode).and_return(StringIO.new("from stdin"))
      expect(run("publish", queue, url: url)).to eq(0)

      out.truncate(out.rewind)
      run("consume", queue, "--count", "2", "--timeout", "5", url: url)
      expect(out.string).to include("from a file").and include("from stdin")
    end
  end

  it "prints only messages with --quiet" do
    with_queue do |queue, url|
      run("publish", queue, "quiet please", url: url)
      out.truncate(out.rewind)

      run("consume", queue, "--count", "1", "--timeout", "5", "--quiet", url: url)
      expect(out.string).to eq("#{queue} (12 bytes): quiet please\n")
    end
  end

  it "fails with the broker's own words when the queue does not exist" do
    isolated_session do |_session, vhost|
      expect(run("inspect", "cli.missing.#{SecureRandom.hex(4)}", url: url_for(vhost))).to eq(1)
      expect(err.string).to include("404", "NOT_FOUND")
    end
  end

  it "fails when a published message is unroutable" do
    isolated_session do |_session, vhost|
      expect(run("publish", "cli.nowhere.#{SecureRandom.hex(4)}", "lost", url: url_for(vhost))).to eq(1)
      expect(err.string).to include("came back unroutable")
    end
  end

  it "fails when the broker cannot be reached" do
    expect(run("inspect", "anything", url: "amqp://guest:guest@127.0.0.1:1")).to eq(1)
    expect(err.string).not_to be_empty
  end

  it "explains itself without arguments, and reports an unknown command" do
    expect(described_class.start([], out: out, err: err)).to eq(1)
    expect(out.string).to include("Usage: async-rabbitmq")

    expect(described_class.start(%w[frobnicate], out: out, err: err)).to eq(1)
    expect(err.string).to include('Unknown command "frobnicate"')

    expect(described_class.start(%w[--url amqp://x inspect q], out: out, err: err)).to eq(1)
    expect(err.string).to include("Options come after the command")
  end

  it "prints the version" do
    expect(described_class.start(%w[--version], out: out, err: err)).to eq(0)
    expect(out.string.strip).to eq(AsyncRabbitMQ::VERSION)
  end
end
