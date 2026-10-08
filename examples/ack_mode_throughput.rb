#!/usr/bin/env ruby
# frozen_string_literal: true

# What acknowledgement mode costs, and what it buys.
#
#   ruby -Ilib examples/ack_mode_throughput.rb
#   ruby -Ilib examples/ack_mode_throughput.rb --messages 50000 --size 1024
#   ruby -Ilib examples/ack_mode_throughput.rb --prefetches 1,50,5000
#
# Publishes the same batch into a fresh queue once per ack mode, drains it, and
# reports how fast it drained and how much of the queue the client was holding
# while it did (Channel#backlog, sampled).
#
# Those two columns are the whole trade-off:
#
#   * Automatic acks are the fastest the broker can go, because it never waits
#     to hear back. But prefetch does not apply to them - RabbitMQ's limit is on
#     *unacknowledged* messages and an auto-ack consumer never has any - so the
#     broker pushes the whole queue as fast as the socket allows and the backlog
#     is held in this process. Peak backlog is the message count, whatever you
#     set prefetch to.
#   * Manual acks let prefetch bound that backlog exactly, at the cost of a
#     round trip each time the window drains. prefetch 1 pays it on every
#     message, which is why it is the classic throughput killer; raising it
#     trades memory back for speed and flattens out well before the queue depth.
#
# The run measures one bare request/reply to the broker first and prints it as a
# floor, because a consumer at prefetch 1 cannot beat it - it waits for the
# window to reopen after every message - and that floor is a property of the
# machine, not of this client. On Windows it lands on the ~15.6 ms scheduler
# tick, which caps prefetch 1 at around 64 msg/s however fast the broker is; on
# Linux loopback it is usually well under a millisecond. Read the prefetch 1 row
# against the floor, never on its own.
#
# Handler concurrency is pinned at 1 for every run so the numbers compare ack
# modes rather than concurrency. That needs saying out loud, because
# basic_qos(prefetch_count:) also resizes the handler pool in this client (a
# deliberate divergence from Bunny, which leaves the pool alone), so each run
# sets pool_size back afterwards. --handlers raises it for every run alike.

require_relative "perf_common"

require "async"
require "optparse"
require "securerandom"
require "async_rabbitmq"

options = {
  url: ENV["RABBITMQ_URL"] || "amqp://guest:guest@127.0.0.1:5672",
  messages: 5_000,
  size: 256,
  prefetches: [1, 10, 100, 1000],
  handlers: 1,
  verbose: false
}

OptionParser.new do |o|
  o.banner = "Usage: ruby -Ilib examples/ack_mode_throughput.rb [options]"
  o.on("--url URL", "broker URL (default #{options[:url]})") { |v| options[:url] = v }
  o.on("--messages N", Integer, "messages per run (default #{options[:messages]})") { |v| options[:messages] = v }
  o.on("--size B", Integer, "body size in bytes (default #{options[:size]})") { |v| options[:size] = v }
  o.on("--prefetches LIST", "manual-ack prefetch counts to try (default #{options[:prefetches].join(',')})") do |v|
    options[:prefetches] = v.split(",").map { |n| Integer(n) }
  end
  o.on("--handlers N", Integer, "pool_size for every run (default #{options[:handlers]})") { |v| options[:handlers] = v }
  o.on("--verbose", "show the client's own log") { options[:verbose] = true }
  o.on("-h", "--help") { puts o; exit }
end.parse!

COUNT   = options[:messages]
BODY    = ("x" * options[:size]).freeze
LOGGER  = Perf.logger(options[:verbose])
# Generous: prefetch 1 is a round trip per message and the point of the run is
# to show how slow that is, not to decide it has failed.
DRAIN_TIMEOUT = 300

# One row of the report.
Result = Struct.new(:label, :seconds, :peak_backlog, keyword_init: true)

# One synchronous request/reply, averaged. This is the floor for prefetch 1.
def round_trip_ms(session)
  ch = session.open_channel
  q  = ch.queue("rtt.#{SecureRandom.hex(4)}", durable: true)
  20.times { ch.queue(q.name, passive: true) } # warm the path up first
  started = Perf.monotonic_ns
  100.times { ch.queue(q.name, passive: true) }
  ms = (Perf.monotonic_ns - started) / 1e6 / 100
  ch.queue_delete(q.name)
  ch.close
  ms
end

# Fill a queue, then drain it and time only the draining. Publishing is done
# under confirms so the run starts with the whole batch demonstrably on the
# broker rather than still in flight, which would otherwise show up as consumer
# throughput.
def run_once(session, label:, manual_ack:, prefetch:, handlers:)
  qname = "ackmode.#{SecureRandom.hex(4)}"

  pub = session.open_channel
  pub.queue(qname, durable: true)
  pub.confirm_select
  COUNT.times { pub.basic_publish(BODY, routing_key: qname) }
  pub.wait_for_confirms
  pub.close

  ch = session.open_channel
  ch.basic_qos(prefetch_count: prefetch) if prefetch
  # basic_qos just coupled the pool to prefetch; put it back so every run has
  # the same handler concurrency and only the ack mode differs.
  ch.pool_size = handlers

  seen     = 0
  peak     = 0
  finished = Async::Condition.new
  tag      = nil

  # Sample what the client is holding. Channel#backlog is deliveries handed to
  # the handler workers and not yet picked up - the number Bunny reports as
  # ConsumerWorkPool#backlog.
  sampler = Async do
    loop do
      peak = [peak, ch.backlog].max
      sleep 0.002
    end
  end

  started = Perf.monotonic_ns
  tag = ch.basic_consume(qname, manual_ack: manual_ack) do |delivery, _header, _body|
    ch.basic_ack(delivery.delivery_tag) if manual_ack
    seen += 1
    finished.signal if seen == COUNT
  end

  # A lost message would otherwise park here for good; say so instead.
  Async::Task.current.with_timeout(DRAIN_TIMEOUT) { finished.wait }
  elapsed = (Perf.monotonic_ns - started) / 1e9
  peak    = [peak, ch.backlog].max
  sampler.stop

  # Cancel before deleting, or the broker cancels the consumer for us and the
  # run reports a warning that is an artefact of the measurement, not a result.
  ch.basic_cancel(tag)
  ch.queue_delete(qname)
  ch.close

  Result.new(label: label, seconds: elapsed, peak_backlog: peak)
end

Sync do
  session = AsyncRabbitMQ::Session.from_uri(options[:url], logger: LOGGER)
  session.connect

  rtt = round_trip_ms(session)

  puts
  puts Perf.rule("ack mode: #{COUNT} messages of #{options[:size]} B, pool_size #{options[:handlers]}")
  puts
  printf("  broker round trip here: %.2f ms, so prefetch 1 cannot exceed ~%.0f msg/s\n\n", rtt, 1000.0 / rtt)

  runs = [{ label: "auto-ack", manual_ack: false, prefetch: nil }]
  # Auto-ack with a prefetch set is worth showing precisely because it looks
  # like it should help and does nothing at all.
  runs << { label: "auto-ack, prefetch #{options[:prefetches].last}",
            manual_ack: false, prefetch: options[:prefetches].last }
  options[:prefetches].each do |n|
    runs << { label: "manual-ack, prefetch #{n}", manual_ack: true, prefetch: n }
  end

  results = runs.map do |r|
    begin
      result = run_once(session, label: r[:label], manual_ack: r[:manual_ack],
                                 prefetch: r[:prefetch], handlers: options[:handlers])
    rescue Async::TimeoutError
      puts "  #{r[:label]}: did not drain #{COUNT} messages within #{DRAIN_TIMEOUT}s"
      next
    end
    printf("  %-32s %12s  %7.2fs  peak backlog %7d\n",
           result.label, Perf.fmt_rate(COUNT, result.seconds), result.seconds, result.peak_backlog)
    result
  end.compact

  fastest = results.max_by { |r| Perf.rate(COUNT, r.seconds) }
  slowest = results.min_by { |r| Perf.rate(COUNT, r.seconds) }

  puts
  puts Perf.rule("what the numbers say")
  puts
  puts "  fastest: #{fastest.label} at #{Perf.fmt_rate(COUNT, fastest.seconds)}"
  puts "  slowest: #{slowest.label} at #{Perf.fmt_rate(COUNT, slowest.seconds)}" \
       " (#{format('%.1fx', Perf.rate(COUNT, fastest.seconds) / Perf.rate(COUNT, slowest.seconds))} slower)"
  puts
  puts "  The slowest row is a round-trip measurement, not a client one: at"
  puts "  prefetch 1 every message waits for the window to reopen, and the"
  puts "  floor above says what that costs on this machine."
  puts
  puts "  Peak backlog is what the client was holding, so roughly"
  puts "  peak x #{options[:size]} B of message plus the client's own per-delivery"
  puts "  overhead. The auto-ack rows peak at the whole batch however the"
  puts "  prefetch is set, which is the point: prefetch cannot bound them."
  puts "  The manual-ack rows peak at about their prefetch, which is the"
  puts "  bound you are paying those round trips for."
  puts

  session.close
end
