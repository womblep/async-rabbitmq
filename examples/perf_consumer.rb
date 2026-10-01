#!/usr/bin/env ruby
# frozen_string_literal: true

# Receiver and verifier for the performance / integrity pair. Start it first:
#
#   ruby -Ilib examples/perf_consumer.rb --streams 4
#   ruby -Ilib examples/perf_publisher.rb --streams 4 --messages 50000
#
# It reads one queue per publisher stream and checks every delivery against the
# contract in perf_common.rb, then reports what it saw: throughput, end-to-end
# latency, and any message that was lost, doubled, delivered out of sequence,
# handed over with another message's header, or routed to the wrong queue.
#
# Ordering is asserted only where AMQP promises it: one stream, one queue, one
# consumer, one handler at a time. Raising --consumers or --handlers above 1
# makes deliveries genuinely concurrent, so the report downgrades ordering to
# an observation and only loss, duplication and corruption stay fatal.

require_relative "perf_common"

require "async"
require "optparse"
require "async_rabbitmq"

options = {
  url: ENV["RABBITMQ_URL"] || "amqp://guest:guest@127.0.0.1:5672",
  run: "default",
  streams: 4,
  consumers: 1,
  handlers: 1,
  prefetch: 200,
  manual_ack: true,
  mode: "consume",
  idle: 15.0,
  purge: true,
  report_every: 2.0,
  verbose: false
}

OptionParser.new do |o|
  o.banner = "Usage: ruby -Ilib examples/perf_consumer.rb [options]"
  o.on("--url URL", "broker URL (default #{options[:url]})") { |v| options[:url] = v }
  o.on("--run NAME", "run id; must match the publisher (default #{options[:run]})") { |v| options[:run] = v }
  o.on("--streams N", Integer, "queues to read, one per publisher stream (default #{options[:streams]})") { |v| options[:streams] = v }
  o.on("--consumers N", Integer, "consumers per queue; above 1 gives up strict order (default 1)") { |v| options[:consumers] = v }
  o.on("--handlers N", Integer, "concurrent handlers per channel; above 1 gives up strict order (default 1)") { |v| options[:handlers] = v }
  o.on("--prefetch N", Integer, "unacked messages the broker may send ahead (default 200)") { |v| options[:prefetch] = v }
  o.on("--[no-]manual-ack", "acknowledge each message (default on)") { |v| options[:manual_ack] = v }
  o.on("--mode MODE", %w[consume get], "consume (push) or get (basic_get polling) (default consume)") { |v| options[:mode] = v }
  o.on("--idle SECONDS", Float, "give up this long after the last message (default 15)") { |v| options[:idle] = v }
  o.on("--[no-]purge", "purge the run queues before starting (default on)") { |v| options[:purge] = v }
  o.on("--report-every SECONDS", Float, "progress interval (default 2.0)") { |v| options[:report_every] = v }
  o.on("--verbose", "show the client's own debug log as well") { options[:verbose] = true }
  o.on("-h", "--help") do
    puts o
    exit 0
  end
end.parse!

strict_order = options[:consumers] == 1 && options[:handlers] == 1

stop = false
Signal.trap("INT") { stop = true }

trackers = (0...options[:streams]).to_h { |s| [s, Perf::StreamTracker.new(s)] }
latencies = Perf::Latencies.new
violations = []
received = 0
received_bytes = 0
redelivered = 0
first_at = nil
last_at = nil

Sync do
  session = AsyncRabbitMQ::Session.from_uri(options[:url], connection_name: "perf-consumer",
                                            logger: Perf.logger(options[:verbose]))
  session.connect

  control = session.open_channel
  control.exchange(Perf::EXCHANGE, type: :topic, durable: true)
  (0...options[:streams]).each do |stream|
    name = Perf.queue_name(options[:run], stream)
    q = control.durable_queue(name, arguments: { "x-expires" => Perf::QUEUE_TTL_MS })
    q.bind(exchange: Perf::EXCHANGE, routing_key: Perf.routing_key(options[:run], stream))
    control.queue_purge(name) if options[:purge]
  end

  puts Perf.rule("perf consumer")
  puts [
    "consumer #{options[:run]}: #{options[:streams]} queue(s)",
    "#{options[:mode]} mode",
    "#{options[:consumers]} consumer(s) per queue",
    "#{options[:handlers]} handler(s) per channel",
    "prefetch #{options[:prefetch]}",
    options[:manual_ack] ? "manual ack" : "auto ack",
    strict_order ? "strict order expected" : "order not asserted (concurrent delivery)"
  ].join(" | ")
  puts "waiting for messages, Ctrl-C to stop early"

  handle = lambda do |stream, delivery, header, body|
    now = Perf.monotonic_ns
    first_at ||= now
    last_at = now
    received += 1
    received_bytes += body.to_s.bytesize
    redelivered += 1 if delivery.respond_to?(:redelivered) && delivery.redelivered

    message = Perf.verify(header, body, expected_run: options[:run], expected_stream: stream)
    message.problems.each { |p| violations << "queue #{stream}, seq #{message.seq}: #{p}" }
    latencies << (now - message.sent_ns) if message.sent_ns

    tracker = trackers[message.stream] || trackers[stream]
    if message.eos
      tracker.end_of_stream(message.eos)
    else
      case tracker.observe(message.seq, body.to_s.bytesize)
      when :duplicate
        violations << "queue #{stream}, seq #{message.seq}: delivered twice" if violations.size < 200
      when :out_of_order
        if strict_order && violations.size < 200
          violations << "queue #{stream}, seq #{message.seq}: arrived after seq #{tracker.high_water}"
        end
      end
    end
  end

  workers = (0...options[:streams]).flat_map do |stream|
    queue_name = Perf.queue_name(options[:run], stream)

    if options[:mode] == "get"
      [Async do
        channel = session.open_channel
        until stop
          got = channel.basic_get(queue_name, manual_ack: options[:manual_ack])
          if got.nil?
            sleep 0.005
            next
          end
          delivery, header, body = got
          handle.call(stream, delivery, header, body)
          channel.basic_ack(delivery.delivery_tag) if options[:manual_ack]
        end
      end]
    else
      Array.new(options[:consumers]) do
        Async do
          channel = session.open_channel(pool_size: options[:handlers])
          # basic_qos couples the handler pool to prefetch, so set the pool after it.
          channel.basic_qos(prefetch_count: options[:prefetch]) if options[:manual_ack]
          channel.pool_size = options[:handlers]
          channel.basic_consume(queue_name, manual_ack: options[:manual_ack]) do |delivery, header, body|
            handle.call(stream, delivery, header, body)
            channel.basic_ack(delivery.delivery_tag) if options[:manual_ack]
          end
        end
      end
    end
  end

  reporter = Async do
    last_count = 0
    last_report = Perf.monotonic_ns
    loop do
      sleep options[:report_every]
      now = Perf.monotonic_ns
      window = (now - last_report) / 1e9
      puts format("  received %9d  %9.0f msg/s  streams finished %d/%d",
                  received, Perf.rate(received - last_count, window),
                  trackers.values.count(&:complete?), options[:streams])
      last_count = received
      last_report = now
    end
  end

  # Finish when every stream has reported its total and delivered it, or when
  # nothing has arrived for --idle seconds.
  until stop
    sleep 0.2
    break if trackers.values.all?(&:complete?)

    if last_at && (Perf.monotonic_ns - last_at) / 1e9 > options[:idle]
      puts format("nothing for %.0fs, stopping", options[:idle])
      break
    end
  end

  reporter.stop
  workers.each(&:stop)
  session.close
end

elapsed = first_at && last_at ? (last_at - first_at) / 1e9 : 0.0

puts ""
puts Perf.rule("consumer summary")
puts format("%-8s %10s %10s %8s %10s %13s", "stream", "received", "expected", "missing", "duplicate", "out of order")
trackers.each_value do |t|
  puts format("%-8d %10d %10s %8d %10d %13d",
              t.stream, t.received, t.total || "?", [t.missing_count, 0].max, t.duplicates, t.out_of_order)
end

expected_total = trackers.values.sum { |t| t.total || 0 }
missing_total = trackers.values.sum { |t| [t.missing_count, 0].max }
duplicate_total = trackers.values.sum(&:duplicates)
disorder_total = trackers.values.sum(&:out_of_order)

puts ""
puts format("deliveries       %d (%d data, %d end-of-stream, %d redelivered)",
            received, trackers.values.sum(&:received), trackers.values.count(&:total), redelivered)
if elapsed > 0.05
  puts format("throughput       %s, %s over %.2fs",
              Perf.fmt_rate(received, elapsed), Perf.fmt_throughput(received_bytes, elapsed), elapsed)
else
  puts format("throughput       too few messages to measure (%.3fs of deliveries)", elapsed)
end

if (summary = latencies.summary_ms)
  puts format("latency ms       p50 %.2f  p90 %.2f  p99 %.2f  max %.2f  min %.2f",
              summary[:p50], summary[:p90], summary[:p99], summary[:max], summary[:min])
  puts "                 (publish to handler, so both programs must share a clock: same host)"
  if summary[:sampled] < summary[:count]
    puts format("                 count and min/max exact over %d; percentiles from a %d sample",
                summary[:count], summary[:sampled])
  end
end

puts format("order            %s",
            strict_order ? "strict per stream, #{disorder_total} violation(s)" : "not asserted, #{disorder_total} late arrival(s)")

trackers.each_value do |t|
  gaps = t.missing(limit: 5)
  next if gaps.empty?

  more = t.missing_count > gaps.size ? ", ... (#{t.missing_count} in total)" : ""
  puts "  stream #{t.stream} never received: #{gaps.join(', ')}#{more}"
end

problems = []
problems << "interrupted before every stream finished" if stop && !trackers.values.all?(&:complete?)

# A stream whose end-of-stream marker never arrived is not a clean stream: the
# publisher's tail is simply missing, and there is no total to compare against.
truncated = trackers.values.reject(&:total)
unless truncated.empty?
  puts format("truncated        stream(s) %s stopped without an end-of-stream marker",
              truncated.map(&:stream).join(", "))
  problems << "#{truncated.size} stream(s) never delivered their end-of-stream marker (publisher tail lost)"
end
problems << "#{missing_total} of #{expected_total} message(s) never arrived" if missing_total.positive?
problems << "#{duplicate_total} message(s) delivered twice" if duplicate_total.positive?
problems << "#{disorder_total} message(s) out of sequence" if strict_order && disorder_total.positive?
problems << "#{violations.size} integrity violation(s)" unless violations.empty?
problems << "nothing received: does the publisher use the same --run and --streams?" if received.zero?

unless violations.empty?
  puts ""
  puts Perf.rule("integrity violations")
  violations.first(20).each { |v| puts "  #{v}" }
  puts "  ... and #{violations.size - 20} more" if violations.size > 20
end

puts ""
if problems.empty?
  puts "verdict          clean: every message arrived once, in order, with its own body and header"
  exit 0
else
  puts "verdict          #{problems.size} problem(s)"
  problems.each { |p| puts "  - #{p}" }
  exit 1
end
