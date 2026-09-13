#!/usr/bin/env ruby
# frozen_string_literal: true

# Self-test for the receiver's checks. A verifier nobody has watched fail is
# worth nothing, so this publishes a short, deliberately broken stream: every
# fault the receiver claims to catch, once each.
#
#   ruby -Ilib examples/perf_consumer.rb --run faults --streams 1 --idle 8
#   ruby -Ilib examples/perf_fault_inject.rb --run faults
#
# The receiver should end with exactly one of each: a gap, two duplicates, one
# out-of-sequence arrival, one body carrying another message's header, one
# corrupted body, one message from a foreign run, and one message routed to the
# wrong queue. If it reports a clean run instead, the checks are broken.

require_relative "perf_common"

require "async"
require "optparse"
require "async_rabbitmq"

options = { url: ENV["RABBITMQ_URL"] || "amqp://guest:guest@127.0.0.1:5672", run: "faults", wait: 15 }

OptionParser.new do |o|
  o.banner = "Usage: ruby -Ilib examples/perf_fault_inject.rb [options]"
  o.on("--url URL", "broker URL (default #{options[:url]})") { |v| options[:url] = v }
  o.on("--run NAME", "run id; must match the receiver (default faults)") { |v| options[:run] = v }
  o.on("--wait SECONDS", Integer, "wait this long for the receiver (default 15)") { |v| options[:wait] = v }
  o.on("-h", "--help") do
    puts o
    exit 0
  end
end.parse!

SIZE = 256
TOTAL = 8

Sync do
  session = AsyncRabbitMQ::Session.from_uri(options[:url], connection_name: "perf-fault-inject",
                                            logger: Perf.logger(false))
  session.connect

  channel = session.open_channel
  channel.exchange(Perf::EXCHANGE, type: :topic, durable: true)
  name = Perf.queue_name(options[:run], 0)
  queue = channel.durable_queue(name, arguments: { "x-expires" => Perf::QUEUE_TTL_MS })
  queue.bind(exchange: Perf::EXCHANGE, routing_key: Perf.routing_key(options[:run], 0))

  # The receiver purges on startup, so wait for it rather than racing it.
  deadline = Perf.monotonic_ns + (options[:wait] * 1_000_000_000)
  until channel.queue(name, passive: true).consumer_count.positive?
    if Perf.monotonic_ns > deadline
      warn "No receiver on #{name} after #{options[:wait]}s. Start perf_consumer.rb --run #{options[:run]} --streams 1 first."
      exit 2
    end
    sleep 0.25
  end

  routing_key = Perf.routing_key(options[:run], 0)
  send = lambda do |body, props|
    channel.basic_publish(body, exchange: Perf::EXCHANGE, routing_key: routing_key, **props)
  end

  intact = lambda do |seq, stream: 0, run: options[:run]|
    [Perf.body(run: run, stream: stream, seq: seq, size: SIZE),
     Perf.properties(run: run, stream: stream, seq: seq, size: SIZE, persistent: false)]
  end

  puts Perf.rule("fault injection")

  puts "1. seq 0 and seq 1, both intact"
  [0, 1].each { |seq| send.call(*intact.call(seq)) }

  puts "2. seq 1 again (duplicate)"
  send.call(*intact.call(1))

  puts "3. seq 2 never sent (gap)"

  puts "4. seq 3 body under the header of seq 9 (crossed content)"
  body, = intact.call(3)
  _, props = intact.call(9)
  send.call(body, props)

  puts "5. seq 4 with one byte flipped in the body (corruption)"
  body, props = intact.call(4)
  corrupt = body.dup
  corrupt.setbyte(SIZE / 2, corrupt.getbyte(SIZE / 2) ^ 0xff)
  send.call(corrupt, props)

  puts "6. seq 6 before seq 5 (out of sequence)"
  send.call(*intact.call(6))
  send.call(*intact.call(5))

  puts "7. a message from run 'ghost' (foreign run)"
  send.call(*intact.call(6, run: "ghost"))

  puts "8. a stream 1 message on stream 0's queue (crossed route)"
  send.call(*intact.call(7, stream: 1))

  puts "9. end of stream: #{TOTAL} messages should have arrived"
  eos_body = Perf.body(run: options[:run], stream: 0, seq: TOTAL, size: Perf::MIN_SIZE)
  eos_props = Perf.properties(run: options[:run], stream: 0, seq: TOTAL,
                              size: Perf::MIN_SIZE, persistent: false, eos: TOTAL)
  send.call(eos_body, eos_props)

  sleep 0.5
  session.close
end

puts ""
puts "Injected. The receiver should report 1 missing, 2 duplicates, 1 out of sequence"
puts "and 4 integrity violations, then exit non-zero."
