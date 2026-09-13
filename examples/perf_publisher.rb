#!/usr/bin/env ruby
# frozen_string_literal: true

# Sender for the performance / integrity pair. Run examples/perf_consumer.rb
# first (this program waits for it), then:
#
#   ruby -Ilib examples/perf_publisher.rb --messages 50000 --streams 4
#
# By default every publisher fiber shares ONE channel, which is the case worth
# stressing: a channel is a multiplexed byte stream, so concurrent publishers
# and concurrent request/reply calls on it are where frames get interleaved and
# replies get handed to the wrong fiber. What this side checks:
#
#   * every confirm delivery tag is unique and the tags cover 1..published,
#     so no publish took another publish's tag and none went missing
#   * no message came back unroutable (basic.return)
#   * concurrent passive declares on the busy shared channel each get their
#     own reply, not another fiber's ("crossed replies", --rpc-probe)
#
# The receiver checks the rest: order, loss, duplication, and bodies paired
# with the wrong header.

require_relative "perf_common"

require "async"
require "optparse"
require "async_rabbitmq"

options = {
  url: ENV["RABBITMQ_URL"] || "amqp://guest:guest@127.0.0.1:5672",
  run: "default",
  streams: 4,
  messages: 20_000,
  size: 512,
  batch: 1,
  confirms: "simple",
  outstanding: 1_000,
  persistent: false,
  channels: "shared",
  rate: nil,
  wait: 30,
  purge: false,
  rpc_probe: 0,
  report_every: 2.0,
  verbose: false
}

OptionParser.new do |o|
  o.banner = "Usage: ruby -Ilib examples/perf_publisher.rb [options]"
  o.on("--url URL", "broker URL (default #{options[:url]})") { |v| options[:url] = v }
  o.on("--run NAME", "run id; must match the receiver (default #{options[:run]})") { |v| options[:run] = v }
  o.on("--streams N", Integer, "publisher fibers, one queue each (default #{options[:streams]})") { |v| options[:streams] = v }
  o.on("--messages N", Integer, "messages per stream (default #{options[:messages]})") { |v| options[:messages] = v }
  o.on("--size N", Integer, "body size in bytes (default #{options[:size]})") { |v| options[:size] = v }
  o.on("--batch N", Integer, "publish N at a time with basic_publish_batch (default 1)") { |v| options[:batch] = v }
  o.on("--confirms MODE", %w[none simple tracking], "none, simple or tracking (default simple)") { |v| options[:confirms] = v }
  o.on("--outstanding N", Integer, "unconfirmed limit for --confirms tracking (default 1000)") { |v| options[:outstanding] = v }
  o.on("--[no-]persistent", "publish persistent messages (default off)") { |v| options[:persistent] = v }
  o.on("--channels MODE", %w[shared per-stream], "shared or per-stream (default shared)") { |v| options[:channels] = v }
  o.on("--rate N", Integer, "cap the total publish rate, messages per second") { |v| options[:rate] = v }
  o.on("--wait SECONDS", Integer, "wait this long for the receiver, 0 to skip (default 30)") { |v| options[:wait] = v }
  o.on("--[no-]purge", "purge the run queues before publishing (default off)") { |v| options[:purge] = v }
  o.on("--rpc-probe N", Integer, "N fibers issuing passive declares on the busy channel (default 0)") { |v| options[:rpc_probe] = v }
  o.on("--report-every SECONDS", Float, "progress interval (default 2.0)") { |v| options[:report_every] = v }
  o.on("--verbose", "show the client's own debug log as well") { options[:verbose] = true }
  o.on("-h", "--help") do
    puts o
    exit 0
  end
end.parse!

options[:size] = Perf::MIN_SIZE if options[:size] < Perf::MIN_SIZE
options[:batch] = 1 if options[:batch] < 1

stop = false
Signal.trap("INT") { stop = true }

published = 0
published_bytes = 0
returned = []
rpc_calls = 0
rpc_crossed = []
confirm_tags = Hash.new { |h, k| h[k] = { seen: Perf::Bitset.new, count: 0, duplicates: 0, max: 0 } }
nacked = 0
unconfirmed = 0
started_at = nil
publish_done_at = nil
settled_at = nil

Sync do
  session = AsyncRabbitMQ::Session.from_uri(options[:url], connection_name: "perf-publisher",
                                            logger: Perf.logger(options[:verbose]))
  session.connect

  control = session.open_channel
  control.exchange(Perf::EXCHANGE, type: :topic, durable: true)
  queues = (0...options[:streams]).map do |stream|
    name = Perf.queue_name(options[:run], stream)
    q = control.durable_queue(name, arguments: { "x-expires" => Perf::QUEUE_TTL_MS })
    q.bind(exchange: Perf::EXCHANGE, routing_key: Perf.routing_key(options[:run], stream))
    control.queue_purge(name) if options[:purge]
    q
  end

  if options[:wait].positive?
    deadline = Perf.monotonic_ns + (options[:wait] * 1_000_000_000)
    waited = false
    loop do
      idle = queues.map(&:name).reject { |n| control.queue(n, passive: true).consumer_count.positive? }
      break if idle.empty? || stop

      if Perf.monotonic_ns > deadline
        puts "No consumer on #{idle.size} of #{queues.size} queue(s) after #{options[:wait]}s; publishing anyway."
        puts "(latency figures will then include the time messages sat in the queue)"
        break
      end
      unless waited
        print "Waiting for a receiver on #{queues.size} queue(s)"
        waited = true
      end
      print "."
      sleep 0.5
    end
    puts "" if waited
  end

  channels = if options[:channels] == "shared"
               shared = session.open_channel
               Array.new(options[:streams]) { shared }
             else
               Array.new(options[:streams]) { session.open_channel }
             end

  channels.uniq.each do |ch|
    ch.on_return { |ret, _header, body| returned << [ret.reply_text, ret.routing_key, body.to_s.bytesize] }
    case options[:confirms]
    when "simple" then ch.confirm_select
    when "tracking" then ch.confirm_select(tracking: true, outstanding_limit: options[:outstanding])
    end
  end

  puts Perf.rule("perf publisher")
  puts [
    "publisher #{options[:run]}: #{options[:streams]} stream(s) x #{options[:messages]} msg",
    "#{options[:size]} B",
    "batch #{options[:batch]}",
    "confirms #{options[:confirms]}",
    options[:channels] == "shared" ? "one shared channel" : "a channel per stream",
    options[:persistent] ? "persistent" : "transient",
    options[:rate] ? "capped at #{options[:rate]} msg/s" : "unthrottled"
  ].join(" | ")

  started_at = Perf.monotonic_ns

  reporter = Async do
    last_count = 0
    last_at = started_at
    loop do
      sleep options[:report_every]
      now = Perf.monotonic_ns
      window = (now - last_at) / 1e9
      outstanding = if options[:confirms] == "none"
                      ""
                    else
                      "unconfirmed #{channels.uniq.sum { |c| c.unconfirmed_tags.size }}"
                    end
      puts format("  %7.1fs  published %9d  %9.0f msg/s  %s",
                  (now - started_at) / 1e9, published,
                  Perf.rate(published - last_count, window), outstanding)
      last_count = published
      last_at = now
    end
  end

  # Passive declares issued from other fibers while the same channel is busy
  # publishing: each reply must come back to the fiber that asked for it.
  probes = (0...options[:rpc_probe]).map do |i|
    Async do
      target = queues[i % queues.size].name
      until stop || published >= options[:streams] * options[:messages]
        reply = channels.first.queue(target, passive: true)
        rpc_calls += 1
        rpc_crossed << [target, reply.name] if reply.name != target
        sleep 0.001
      end
    end
  end

  per_stream_rate = options[:rate] ? options[:rate].to_f / options[:streams] : nil

  senders = (0...options[:streams]).map do |stream|
    Async do
      channel = channels[stream]
      routing_key = Perf.routing_key(options[:run], stream)
      tags = confirm_tags[channel.object_id]
      stream_started = Perf.monotonic_ns
      sent = 0

      record = lambda do |tag|
        next if tag.nil?

        if tags[:seen].set?(tag)
          tags[:duplicates] += 1
        else
          tags[:seen].set(tag)
          tags[:count] += 1
          tags[:max] = tag if tag > tags[:max]
        end
      end

      while sent < options[:messages] && !stop
        chunk = [options[:batch], options[:messages] - sent].min

        if chunk == 1
          body = Perf.body(run: options[:run], stream: stream, seq: sent, size: options[:size])
          props = Perf.properties(run: options[:run], stream: stream, seq: sent,
                                  size: options[:size], persistent: options[:persistent])
          record.call(channel.basic_publish(body, exchange: Perf::EXCHANGE, routing_key: routing_key,
                                                  mandatory: true, **props))
          published_bytes += body.bytesize
        else
          bodies = (0...chunk).map do |i|
            Perf.body(run: options[:run], stream: stream, seq: sent + i, size: options[:size])
          end
          # One properties set covers the whole batch, so the per-message
          # sequence number travels in the body; the header states the base and
          # the batch size, and the receiver checks the body against them.
          props = Perf.properties(run: options[:run], stream: stream, seq: sent,
                                  size: options[:size], persistent: options[:persistent], batch: chunk)
          batch_tags = channel.basic_publish_batch(bodies, exchange: Perf::EXCHANGE, routing_key: routing_key,
                                                           mandatory: true, **props)
          Array(batch_tags).each { |t| record.call(t) }
          published_bytes += bodies.sum(&:bytesize)
        end

        sent += chunk
        published += chunk

        if per_stream_rate
          due = stream_started + (sent / per_stream_rate * 1_000_000_000).to_i
          behind = due - Perf.monotonic_ns
          sleep(behind / 1e9) if behind.positive?
        end
      end

      # End-of-stream marker: tells the receiver how many messages to expect, so
      # it can report loss instead of waiting for messages that never come.
      eos_body = Perf.body(run: options[:run], stream: stream, seq: sent, size: Perf::MIN_SIZE)
      eos_props = Perf.properties(run: options[:run], stream: stream, seq: sent,
                                  size: Perf::MIN_SIZE, persistent: options[:persistent], eos: sent)
      record.call(channel.basic_publish(eos_body, exchange: Perf::EXCHANGE, routing_key: routing_key,
                                                  mandatory: true, **eos_props))
      sent
    end
  end

  senders.each(&:wait)
  probes.each(&:stop)
  reporter.stop
  publish_done_at = Perf.monotonic_ns

  if options[:confirms] != "none"
    channels.uniq.each do |ch|
      begin
        ch.wait_for_confirms
      rescue AsyncRabbitMQ::MessageNacked => e
        nacked += e.nacked_tags.size
      end
      nacked += ch.nacked_tags.size if options[:confirms] == "simple"
      unconfirmed += ch.unconfirmed_tags.size
    end
  end

  # basic.return arrives asynchronously; give the broker a moment to send any.
  sleep 0.2
  session.close
end

elapsed = (Perf.monotonic_ns - started_at) / 1e9
expected_tags = options[:streams] * (options[:messages] + 1) # one end-of-stream marker per stream

puts ""
puts Perf.rule("publisher summary")
puts format("published        %d messages (%d data + %d end-of-stream) in %.2fs",
            published + options[:streams], published, options[:streams], elapsed)
puts format("throughput       %s, %s", Perf.fmt_rate(published, elapsed), Perf.fmt_throughput(published_bytes, elapsed))

problems = []
problems << "interrupted before finishing" if stop

if options[:confirms] == "none"
  puts "confirms         off, so nothing here says the broker received anything:"
  puts "                 basic_publish only queues the frames for the writer, and a"
  puts "                 backlog still queued when close gives up is discarded"
else
  total_tags = confirm_tags.values.sum { |t| t[:count] }
  dupes = confirm_tags.values.sum { |t| t[:duplicates] }
  gaps = confirm_tags.values.sum { |t| t[:max] - t[:count] }
  puts format("confirm tags     %d unique, %d issued twice, %d gap(s) below the highest tag", total_tags, dupes, gaps)
  puts format("confirms         %d nacked, %d still unconfirmed at close", nacked, unconfirmed)
  problems << "#{dupes} delivery tag(s) issued twice: two publishes crossed" if dupes.positive?
  problems << "#{gaps} delivery tag(s) never issued" if gaps.positive?
  problems << "#{total_tags} tags for #{expected_tags} publishes" if total_tags != expected_tags && !stop
  problems << "#{nacked} message(s) nacked by the broker" if nacked.positive?
  problems << "#{unconfirmed} message(s) unconfirmed at close" if unconfirmed.positive?
end

if options[:rpc_probe].positive?
  puts format("rpc probes       %d replies, %d crossed", rpc_calls, rpc_crossed.size)
  rpc_crossed.first(5).each { |asked, got| puts "  asked for #{asked}, reply named #{got}" }
  problems << "#{rpc_crossed.size} reply(ies) went to the wrong caller" unless rpc_crossed.empty?
end

unless returned.empty?
  puts format("unroutable       %d returned", returned.size)
  returned.first(3).each { |text, key, bytes| puts "  #{text} (routing key #{key}, #{bytes} B)" }
  problems << "#{returned.size} message(s) came back unroutable: does the receiver use the same --run and --streams?"
end

puts ""
if problems.empty?
  puts "verdict          send side clean"
  exit 0
else
  puts "verdict          #{problems.size} problem(s)"
  problems.each { |p| puts "  - #{p}" }
  exit 1
end
