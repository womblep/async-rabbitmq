# frozen_string_literal: true

require "logger"
require "stringio"
require_relative "helper"

RSpec.describe Async::RabbitMQ::Logging do
  around do |example|
    was = Async::RabbitMQ.log_level
    Async::RabbitMQ.log_level = :debug
    example.run
  ensure
    Async::RabbitMQ.log_level = was
  end

  describe "Async::RabbitMQ.log_level" do
    it "is :warn unless set, and holds back what is below it whatever the logger would take" do
      out    = StringIO.new
      logger = described_class.resolve(Logger.new(out, level: :debug, formatter: ->(severity, *, message) { "#{severity} #{message}
" }))
      Async::RabbitMQ.log_level = :warn
      logger.info(Object.new, "Reconnected")
      logger.warn(Object.new, "Connection lost")
      Async::RabbitMQ.log_level = "info"
      logger.info(Object.new, "Reconnected")
      Async::RabbitMQ.log_level = :off
      logger.error(Object.new, "Gave up")

      expect(out.string).to eq("WARN Connection lost
INFO Reconnected
")
      expect { Async::RabbitMQ.log_level = :loud }.to raise_error(ArgumentError, /log_level/)
    end

    it "holds back what goes to Console too" do
      seen   = []
      target = Console.logger.dup
      target.define_singleton_method(:info) { |*arguments, **| seen << arguments.last }
      logger = described_class.resolve(target)
      Async::RabbitMQ.log_level = :warn
      logger.info(Object.new, "held back")
      Async::RabbitMQ.log_level = :info
      logger.info(Object.new, "let through")

      expect(seen).to eq(["let through"])
    end
  end

  describe ".resolve" do
    it "forwards to Console by default, and to a Console logger it is given" do
      expect(described_class.resolve(Console)).to be_a(described_class::Forward)
      expect(described_class.resolve(Console.logger)).to be_a(described_class::Forward)
    end

    it "gives a Logger-compatible object the message, the fields and the exception as one line" do
      out    = StringIO.new
      logger = described_class.resolve(Logger.new(out, formatter: ->(severity, _, _, message) { "#{severity} #{message}\n" }))
      logger.warn(Object.new, "Connection lost", endpoint: "amqp://here", exception: IOError.new("gone"))
      logger.info(Object.new, "Reconnected")

      expect(out.string).to eq("WARN Connection lost (endpoint=amqp://here): IOError: gone\nINFO Reconnected\n")
    end

    it "discards everything for nil, is harmless to repeat, and refuses what cannot log" do
      silent = described_class.resolve(nil)
      expect(silent.error(Object.new, "anything", exception: IOError.new("x"))).to be_nil
      expect(described_class.resolve(silent)).to equal(silent)
      expect { described_class.resolve(Object.new) }.to raise_error(ArgumentError, /logger:/)
    end
  end

  describe "a client given a logger", :integration do
    it "writes what happens to its connection there, and so does a consumer built on it" do
      out = StringIO.new
      with_vhost do |vhost|
        Async::RabbitMQ::Client.open(endpoint(vhost: vhost), reconnect_interval: 0.1,
                                                             logger: Logger.new(out, level: :info)) do |client|
          managed = client.queue("work").managed_consumer
          running = Async { managed.run { raise "a handler with a bug" } }
          client.publisher { |publisher| publisher.publish("m", routing_key: "work") }
          wait_for { out.string.include?("Handler for work raised") }
          sever!(client.instance_variable_get(:@connection))
          client.open_channel
          managed.stop
          running.wait
        end
      end

      expect(out.string).to include("Connection lost; reconnecting (endpoint=amqp://")
      expect(out.string).to include("Reconnected (endpoint=amqp://")
      expect(out.string).to include("RuntimeError: a handler with a bug")
    end

    it "leaves an event the application has a hook for to the application" do
      out = StringIO.new
      Async::RabbitMQ.log_level = :info
      with_vhost do |vhost|
        Async::RabbitMQ::Client.open(endpoint(vhost: vhost), reconnect_interval: 0.1,
                                                             logger: Logger.new(out, level: :debug)) do |client|
          client.on_connection_lost { out.puts "mine: lost" }
          managed = client.queue("work").managed_consumer
          failed  = []
          managed.on_error { |error, _| failed << error.message }
          running = Async { managed.run { raise "a handler with a bug" } }
          client.publisher { |publisher| publisher.publish("m", routing_key: "work") }
          wait_for { failed.any? }
          sever!(client.instance_variable_get(:@connection))
          client.open_channel
          connection = client.instance_variable_get(:@connection)
          connection.send(:receive_connection_method, AMQ::Protocol::Connection::Blocked.new("low on memory"))
          client.on_blocked { out.puts "mine: blocked" }
          connection.send(:receive_connection_method, AMQ::Protocol::Connection::Unblocked.new)
          connection.send(:receive_connection_method, AMQ::Protocol::Connection::Blocked.new("low on disk"))
          connection.send(:receive_connection_method, AMQ::Protocol::Connection::Unblocked.new)
          managed.stop
          running.wait
        end
      end

      expect(out.string).to include("mine: lost").and(include("mine: blocked"))
      expect(out.string).not_to include("Connection lost; reconnecting")
      expect(out.string).not_to include("Handler for work raised")
      # No hook for these, so the client still says so itself.
      expect(out.string).to include("Reconnected (endpoint=")
      expect(out.string.scan("Broker has blocked publishing").size).to eq(1)
      expect(out.string.scan("Broker has unblocked publishing").size).to eq(2)
    end

    it "warns once that a cancelled consumer cannot consume again, however long that lasts" do
      out = StringIO.new
      with_vhost do |vhost|
        logger = Logger.new(out, level: :debug, formatter: ->(severity, *, message) { "#{severity} #{message}
" })
        Async::RabbitMQ::Client.open(endpoint(vhost: vhost), logger: logger) do |client|
          consumer = client.queue("work").consumer(on_cancel: :resubscribe)
          running  = Async { consumer.each(&:ack) }
          wait_for { consumer.consumer_tag }
          # Someone else takes the queue for themselves, and the broker
          # cancels this consumer: consuming again is refused.
          channel = consumer.instance_variable_get(:@channel)
          channel.call(AMQ::Protocol::Basic::Cancel.encode(channel.id, consumer.consumer_tag, false),
                       AMQ::Protocol::Basic::CancelOk)
          other = client.open_channel
          other.call(AMQ::Protocol::Basic::Consume.encode(other.id, "work", "", false, false, true, false, {}),
                     AMQ::Protocol::Basic::ConsumeOk)
          consumer.cancelled(nil)
          wait_for { out.string.scan("cannot consume again yet").size >= 2 }
          consumer.stop
          running.wait
        end
      end

      expect(out.string.scan(/^WARN Consumer of work was cancelled by the broker and cannot consume/).size).to eq(1)
      expect(out.string).to match(/^DEBUG Consumer of work was cancelled by the broker and cannot consume/)
    end

    it "is shared by every client of a cluster" do
      logger  = Logger.new(StringIO.new)
      cluster = Async::RabbitMQ::Cluster.new([endpoint, endpoint], logger: logger)

      expect(cluster.clients.map { |client| client.logger.logger }).to all(equal(logger))
    end
  end
end
