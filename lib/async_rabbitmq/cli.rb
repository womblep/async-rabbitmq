# frozen_string_literal: true

require "optparse"

require_relative "../async_rabbitmq"

module AsyncRabbitMQ
  # The `async-rabbitmq` command: publish, consume, inspect and purge from a
  # terminal, written on the gem's own API.
  #
  # Everything goes through CLI.start, which returns an exit status instead of
  # calling exit, so the specs can drive it like any other object.
  class CLI
    COMMANDS = %w[publish consume inspect purge].freeze
    DEFAULT_URL = "amqp://guest:guest@localhost:5672"

    Options = Struct.new(:url, :exchange, :routing_key, :persistent, :count, :timeout,
                         :peek, :prefetch, :file, :quiet, keyword_init: true)

    def self.start(argv, out: $stdout, err: $stderr)
      new(out: out, err: err).run(argv)
    end

    def initialize(out: $stdout, err: $stderr)
      @out = out
      @err = err
    end

    def run(argv)
      argv = argv.dup
      command = argv.shift

      case command
      when "publish" then publish(argv)
      when "consume" then consume(argv)
      when "inspect" then inspect_queue(argv)
      when "purge"   then purge(argv)
      when "--version", "-v"
        @out.puts(AsyncRabbitMQ::VERSION)
        0
      when nil, "help", "--help", "-h"
        @out.puts(usage)
        command.nil? ? 1 : 0
      when /\A-/
        @err.puts("Options come after the command: async-rabbitmq <command> QUEUE #{command} ...")
        @err.puts(usage)
        1
      else
        @err.puts("Unknown command #{command.inspect}")
        @err.puts(usage)
        1
      end
    rescue AsyncRabbitMQ::ChannelError => e
      @err.puts("#{e.code} #{e.text}")
      1
    rescue AsyncRabbitMQ::AuthenticationError => e
      @err.puts("Authentication failed: #{e.message}")
      1
    rescue AsyncRabbitMQ::Error, SystemCallError, SocketError => e
      @err.puts("#{e.class.name.split('::').last}: #{e.message}")
      1
    rescue OptionParser::ParseError => e
      @err.puts(e.message)
      1
    end

    def usage
      <<~TEXT
        Usage: async-rabbitmq <command> [options]

        Commands:
          publish QUEUE [BODY]   publish a message (BODY defaults to stdin)
          consume QUEUE          print messages as they arrive
          inspect QUEUE          message and consumer counts
          purge QUEUE            discard everything on a queue

        Options go after the command and its queue:
          --url URL              broker URL (env RABBITMQ_URL, default #{DEFAULT_URL})
          --quiet                print only the messages, not the status lines
          -h, --help             this text, or per-command help after a command
              --version          print the gem version

        Run "async-rabbitmq <command> --help" for the options of one command.
      TEXT
    end

    private

    # ------------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------------

    def publish(argv)
      options = parse(argv, "publish QUEUE [BODY]") do |parser, opts|
        parser.on("--exchange NAME", "publish to this exchange; QUEUE becomes the routing key") { |v| opts.exchange = v }
        parser.on("--persistent", "mark the message persistent (delivery mode 2)") { opts.persistent = true }
        parser.on("--count N", Integer, "publish the same body N times (default 1)") { |v| opts.count = v }
        parser.on("--file PATH", "read the body from a file") { |v| opts.file = v }
      end
      return 1 unless options

      target = argv.shift
      return missing("QUEUE") unless target

      body = body_for(argv, options)
      return 1 unless body

      count = options.count || 1
      exchange = options.exchange || ""
      routing_key = target

      with_session(options) do |session|
        channel  = session.open_channel
        returned = 0
        # Registered before publishing: a return can arrive before its ack.
        channel.on_return { |_ret, _header, _body| returned += 1 }
        channel.confirm_select
        count.times do
          channel.basic_publish(body, exchange: exchange, routing_key: routing_key,
                                      persistent: options.persistent, mandatory: true)
        end
        confirmed = channel.wait_for_confirms
        sleep 0.1 # a basic.return arrives before its ack, but give the reader a tick

        destination = exchange.empty? ? "queue #{routing_key}" : "#{exchange} with routing key #{routing_key}"
        if !confirmed
          @err.puts("Broker rejected #{channel.nacked_tags.size} of #{count} message(s)")
          1
        elsif returned.positive?
          @err.puts("#{returned} of #{count} message(s) came back unroutable from #{destination}")
          1
        else
          say("Published #{count} message#{'s' if count != 1} to #{destination}, all confirmed")
          0
        end
      end
    end

    def consume(argv)
      options = parse(argv, "consume QUEUE") do |parser, opts|
        parser.on("--count N", Integer, "stop after N messages") { |v| opts.count = v }
        parser.on("--timeout SECONDS", Float, "stop after this long without a message") { |v| opts.timeout = v }
        parser.on("--peek", "leave the messages on the queue (never acknowledge)") { opts.peek = true }
        parser.on("--prefetch N", Integer, "unacked messages to allow (default 10)") { |v| opts.prefetch = v }
      end
      return 1 unless options

      queue = argv.shift
      return missing("QUEUE") unless queue

      wanted   = options.count
      idle     = options.timeout
      received = 0

      with_session(options) do |session|
        channel = session.open_channel
        # With a limit, ask for no more than that many at a time, so the broker
        # does not push a batch we are about to walk away from.
        channel.basic_qos(prefetch_count: options.prefetch || (wanted ? [wanted, 1].max : 10))
        last_at = monotonic

        channel.basic_consume(queue, manual_ack: true) do |delivery, header, body|
          # Already have what was asked for: leave this one on the queue by not
          # acknowledging it, so --count 2 takes two and no more.
          next if wanted && received >= wanted

          received += 1
          last_at = monotonic
          @out.puts(format_delivery(delivery, header, body))
          channel.basic_ack(delivery.delivery_tag) unless options.peek
        end

        say("Consuming from #{queue}#{options.peek ? ' (peeking, nothing is acknowledged)' : ''}. Ctrl-C to stop.")

        interrupted = false
        Signal.trap("INT") { interrupted = true }
        until interrupted
          sleep 0.1
          break if wanted && received >= wanted
          break if idle && (monotonic - last_at) > idle
        end

        say("Received #{received} message#{'s' if received != 1}#{options.peek ? ', left on the queue' : ''}")
        0
      end
    end

    def inspect_queue(argv)
      options = parse(argv, "inspect QUEUE") { |_parser, _opts| }
      return 1 unless options

      queue = argv.shift
      return missing("QUEUE") unless queue

      with_session(options) do |session|
        channel = session.open_channel
        status  = channel.queue(queue, passive: true)
        @out.puts("#{queue}: #{status.message_count} message#{'s' if status.message_count != 1}, " \
                  "#{status.consumer_count} consumer#{'s' if status.consumer_count != 1}")
        0
      end
    end

    def purge(argv)
      options = parse(argv, "purge QUEUE") { |_parser, _opts| }
      return 1 unless options

      queue = argv.shift
      return missing("QUEUE") unless queue

      with_session(options) do |session|
        channel = session.open_channel
        purged  = channel.queue_purge(queue)
        count   = purged.respond_to?(:message_count) ? purged.message_count : nil
        @out.puts("Purged #{queue}#{count ? ": #{count} message#{'s' if count != 1} discarded" : ''}")
        0
      end
    end

    # ------------------------------------------------------------------------
    # Plumbing
    # ------------------------------------------------------------------------

    # Parses the shared options plus whatever the command adds. Returns nil when
    # the parse failed or --help was asked for, having printed the reason.
    def parse(argv, banner)
      opts = Options.new(url: ENV["RABBITMQ_URL"] || DEFAULT_URL)
      parser = OptionParser.new do |p|
        p.banner = "Usage: async-rabbitmq #{banner} [options]"
        p.on("--url URL", "broker URL (env RABBITMQ_URL)") { |v| opts.url = v }
        p.on("--quiet", "print only the messages, no status lines") { opts.quiet = true }
        yield(p, opts)
        p.on("-h", "--help", "this text") do
          @out.puts(p)
          return nil
        end
      end
      parser.parse!(argv)
      @quiet = opts.quiet
      opts
    end

    def with_session(options)
      status = 1
      Sync do
        session = AsyncRabbitMQ::Session.from_uri(options.url, connection_name: "async-rabbitmq cli",
                                                               logger: quiet_logger)
        begin
          session.connect
          status = yield(session)
        ensure
          session.close rescue nil
        end
      end
      status
    end

    def body_for(argv, options)
      if options.file
        return File.binread(options.file) if File.exist?(options.file)

        @err.puts("No such file: #{options.file}")
        nil
      elsif !argv.empty?
        argv.join(" ")
      elsif !$stdin.tty?
        $stdin.binmode.read
      else
        missing("BODY")
        nil
      end
    end

    def format_delivery(delivery, header, body)
      properties = header.respond_to?(:properties) ? (header.properties || {}) : {}
      source = delivery.exchange.to_s.empty? ? delivery.routing_key : "#{delivery.exchange}/#{delivery.routing_key}"
      details = ["#{body.to_s.bytesize} bytes"]
      details << "redelivered" if delivery.redelivered
      details << "id #{properties[:message_id]}" if properties[:message_id]
      "#{source} (#{details.join(', ')}): #{body}"
    end

    def missing(what)
      @err.puts("Missing #{what}")
      1
    end

    def say(message)
      @out.puts(message) unless @quiet
    end

    def quiet_logger
      logger = Logger.new(@err)
      logger.level = Logger::WARN
      logger.formatter = ->(severity, _time, _progname, msg) { "#{severity.downcase}: #{msg}\n" }
      logger
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
