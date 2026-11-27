# frozen_string_literal: true

# Soak test for Async::RabbitMQ. Publishes and consumes steadily while a
# chaos fiber cuts the connection, has the broker close it, raises the
# broker's memory alarm, deletes a queue under a consumer and cancels a
# consumer. Checks that every confirmed message is received, that the hooks
# fire, and watches for growth in live objects and fibers.
#
#   DURATION=7200 bundle exec ruby soak.rb

require "async"
require "async/rabbitmq"
require "amq/protocol"
require "logger"
require "net/http"
require "json"
require "securerandom"
require "set"
require "toxiproxy"

DURATION  = Integer(ENV.fetch("DURATION", "7200"))
HOST      = "127.0.0.1"
PROXY     = 21_111
MGMT      = 15_680
BROKER    = ENV.fetch("BROKER_CONTAINER", "spec-rabbitmq-1")
VHOST     = "soak-#{SecureRandom.hex(4)}"
LOG_PATH  = ENV.fetch("CLIENT_LOG", "soak-client.log")
WATERMARK = ENV.fetch("WATERMARK", "0.6")

Toxiproxy.host = "http://127.0.0.1:18474"
$stdout.sync = true

def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
START = now
def stamp = format("%7.1f", now - START)
def say(text) = puts("#{stamp} #{text}")

def http(verb, path, body = nil)
  uri     = URI("http://#{HOST}:#{MGMT}#{path}")
  request = Net::HTTP.const_get(verb).new(uri)
  request.basic_auth("guest", "guest")
  if body
    request.body = JSON.generate(body)
    request["content-type"] = "application/json"
  end
  Net::HTTP.start(uri.host, uri.port, open_timeout: 5, read_timeout: 10) { |http| http.request(request) }
end

# Which sequence numbers of each publisher have been received.
class Tracker
  attr_reader :unique, :duplicates

  def initialize
    @low        = Hash.new(0)
    @ahead      = Hash.new { |hash, key| hash[key] = Set.new }
    @unique     = 0
    @duplicates = 0
  end

  def record(body)
    publisher, sequence = body.split(":")
    sequence = sequence.to_i
    ahead    = @ahead[publisher]
    if sequence < @low[publisher] || ahead.include?(sequence)
      @duplicates += 1
      return
    end
    @unique += 1
    ahead << sequence
    @low[publisher] += 1 while ahead.delete?(@low[publisher])
  end

  # Sequence numbers below +count+ that never arrived.
  def missing(publisher, count)
    (@low[publisher]...count).reject { |sequence| @ahead[publisher].include?(sequence) }
  end

  def ahead_size = @ahead.each_value.sum(&:size)
end

events    = Hash.new(0)
chaos     = Hash.new(0)
confirmed = Hash.new(0)
errors    = Hash.new(0)
tracker   = Tracker.new
problems  = []
stopping  = false
quiet     = false # true while chaos is deliberately holding things up

logger           = Logger.new(LOG_PATH)
logger.level     = Logger::DEBUG
Async::RabbitMQ.log_level = :warn

def hook_client(client, name, events)
  client.on_connection_lost  { |_error| events[:"#{name}_lost"] += 1 }
  client.on_reconnect_failed { |_error, _attempt| events[:"#{name}_reconnect_failed"] += 1 }
  client.on_reconnected      { |_attempts| events[:"#{name}_reconnected"] += 1 }
  client.on_blocked          { |_reason| events[:"#{name}_blocked"] += 1 }
  client.on_unblocked        { events[:"#{name}_unblocked"] += 1 }
  client.on_gave_up          { |_error, _attempts| events[:"#{name}_gave_up"] += 1 }
end

def publish_loop(publisher, name, queue, confirmed, errors, pause, stopping)
  sequence = 0
  until stopping.call
    begin
      publisher.publish("#{name}:#{sequence}", routing_key: queue)
      sequence += 1
      confirmed[name] = sequence
    rescue Async::RabbitMQ::Error => error
      errors[error.class.name] += 1
      sleep 0.1
    end
    sleep pause if (sequence % 8).zero?
  end
end

http(:Put, "/api/vhosts/#{VHOST}")
http(:Put, "/api/permissions/#{VHOST}/guest", { configure: ".*", write: ".*", read: ".*" })
say "vhost #{VHOST}, duration #{DURATION}s, client log #{LOG_PATH}"

begin
  Toxiproxy[:rabbitmq]
rescue Toxiproxy::NotFound
  Toxiproxy.populate([{ name: "rabbitmq", listen: "0.0.0.0:11111", upstream: "rabbitmq:5672" }])
end
proxy = Toxiproxy[:rabbitmq]
proxy.enable

Sync do |task|
  endpoint   = Async::RabbitMQ::Endpoint.new(HOST, PROXY, vhost: VHOST)
  publishing = Async::RabbitMQ::Client.open(endpoint, logger: logger, reconnect_max_interval: 5)
  consuming  = Async::RabbitMQ::Client.open(endpoint, logger: logger, reconnect_max_interval: 5)
  hook_client(publishing, "pub", events)
  hook_client(consuming, "con", events)

  work   = consuming.queue("soak.work")
  plain  = consuming.queue("soak.plain")
  publishing.queue("soak.work")
  publishing.queue("soak.plain")

  is_stopping = -> { stopping }
  tasks       = []

  # Publishers: one that resends over a lost connection, one that does not
  # and so retries what it is told was not confirmed.
  resending = publishing.publisher("", resend: true)
  retrying  = publishing.publisher("")
  third     = publishing.publisher("", resend: true)
  tasks << task.async { publish_loop(resending, "a", "soak.work", confirmed, errors, 0.004, is_stopping) }
  tasks << task.async { publish_loop(retrying, "b", "soak.work", confirmed, errors, 0.004, is_stopping) }
  tasks << task.async { publish_loop(third, "c", "soak.plain", confirmed, errors, 0.006, is_stopping) }

  # A managed consumer on the work queue.
  managed = work.managed_consumer(concurrency: 8, prefetch: 32, on_cancel: :resubscribe)
  managed.on_error { |_error, *_rest| events[:handler_error] += 1 }
  managed.on_cancelled { events[:managed_cancelled] += 1 }
  managed.on_resubscribed { |_attempts, _consumer| events[:managed_resubscribed] += 1 }
  tasks << task.async do
    result = managed.run do |message|
      tracker.record(message.body)
      sleep 0.001 if rand(20).zero?
      raise "soak handler failure" if rand(20_000).zero?
    end
    say "managed consumer returned #{result.inspect}"
    problems << "managed consumer ended early with #{result.inspect}" unless stopping
  end

  # A plain consumer on its own queue; this is the one that gets cancelled.
  consumer = plain.consumer(prefetch: 50, on_cancel: :resubscribe)
  consumer.on_cancelled { events[:plain_cancelled] += 1 }
  consumer.on_resubscribed { |attempts, _consumer| events[:plain_resubscribed] += 1; events[:plain_resubscribe_attempts] += attempts }
  tasks << task.async do
    result = consumer.each do |message|
      tracker.record(message.body)
      message.ack
    end
    say "plain consumer returned #{result.inspect}"
    problems << "plain consumer ended early with #{result.inspect}" unless stopping
  end

  # A consumer whose queue is deleted under it: a real cancel from the broker.
  victim_publisher = publishing.publisher("", resend: true)
  tasks << task.async do
    until stopping
      begin
        victim_publisher.publish("v", routing_key: "soak.victim")
      rescue Async::RabbitMQ::Error
        nil
      end
      sleep 0.05
    end
  end
  tasks << task.async do
    until stopping
      begin
        queue  = consuming.queue("soak.victim")
        victim = queue.consumer(on_cancel: :resubscribe)
        victim.on_cancelled { events[:victim_cancelled] += 1 }
        victim.on_resubscribed { events[:victim_resubscribed] += 1 }
        result = victim.each do |message|
          events[:victim_received] += 1
          message.ack
        end
        events[:"victim_ended_#{result}"] += 1
        victim.close
        consuming.forget(queue)
      rescue Async::RabbitMQ::Error => error
        errors["victim #{error.class.name}"] += 1
        sleep 0.5
      end
    end
  end

  # Chaos.
  actions = {
    cut:          -> do
      seconds = rand(1.0..8.0)
      proxy.disable
      sleep seconds
      proxy.enable
    end,
    long_cut:     -> do
      proxy.disable
      sleep 45
      proxy.enable
    end,
    broker_close: -> do
      connections = JSON.parse(http(:Get, "/api/vhosts/#{VHOST}/connections").body)
      connections.each do |c|
        response = http(:Delete, "/api/connections/#{URI.encode_www_form_component(c['name']).gsub('+', '%20')}")
        chaos[:connections_closed] += 1 if response.code.to_i < 300
      end
    end,
    memory_alarm: -> do
      system("docker", "exec", BROKER, "rabbitmqctl", "-q", "set_vm_memory_high_watermark", "0.00001", out: File::NULL)
      sleep 6
      system("docker", "exec", BROKER, "rabbitmqctl", "-q", "set_vm_memory_high_watermark", WATERMARK, out: File::NULL)
    end,
    delete_queue: -> do
      http(:Delete, "/api/queues/#{VHOST}/soak.victim")
    end,
    cancel:       -> do
      # As the specs do: cancel at the broker, then tell the consumer the
      # broker did it. The queue stays, so the consumer should consume again.
      channel = consumer.instance_variable_get(:@channel)
      tag     = consumer.consumer_tag
      next chaos[:cancel_skipped] += 1 unless channel&.open? && tag

      channel.call(AMQ::Protocol::Basic::Cancel.encode(channel.id, tag, false), AMQ::Protocol::Basic::CancelOk)
      consumer.cancelled(nil)
    end
  }
  weights = { cut: 4, long_cut: 1, broker_close: 3, memory_alarm: 1, delete_queue: 2, cancel: 3 }
  deck    = weights.flat_map { |name, weight| [name] * weight }
  deck    = ENV["CHAOS_ONLY"].split(",").map(&:to_sym) if ENV["CHAOS_ONLY"]
  gap     = ENV["CHAOS_GAP"] ? (2.0..4.0) : (10.0..25.0)

  chaos_task = task.async do
    sleep 20
    until stopping
      name = deck.sample
      say "chaos: #{name}"
      quiet = true
      begin
        actions.fetch(name).call
        chaos[name] += 1
      rescue StandardError => error
        chaos[:"#{name}_failed"] += 1
        say "chaos #{name} failed: #{error.class}: #{error.message}"
      ensure
        proxy.enable rescue nil
      end
      sleep 10
      quiet = false
      sleep rand(gap)
    end
  end

  # Report every minute, and notice a stall.
  last_unique   = 0
  last_progress = now
  reporter = task.async do
    minute = 0
    loop do
      sleep 60
      minute += 1
      GC.start
      total   = confirmed.values.sum
      fibers  = ObjectSpace.each_object(Fiber).count { |fiber| fiber.alive? }
      if tracker.unique > last_unique
        last_unique   = tracker.unique
        last_progress = now
      elsif !quiet && now - last_progress > 150
        problems << "no message received for #{(now - last_progress).round}s at #{stamp.strip}s"
        say "STALL: nothing received for #{(now - last_progress).round}s"
        last_progress = now
      end
      say "min=#{minute} confirmed=#{total} unique=#{tracker.unique} dup=#{tracker.duplicates} " \
          "behind=#{total - tracker.unique} ahead=#{tracker.ahead_size} live=#{GC.stat(:heap_live_slots)} " \
          "fibers=#{fibers} unconfirmed=#{resending.unconfirmed_count + retrying.unconfirmed_count + third.unconfirmed_count} " \
          "links=#{publishing.link_count}/#{consuming.link_count}"
      say "    events=#{events.sort.to_h} errors=#{errors.sort.to_h}" if (minute % 5).zero?
    end
  end

  sleep DURATION
  say "stopping chaos and publishers"
  stopping = true
  chaos_task.wait
  proxy.enable
  system("docker", "exec", BROKER, "rabbitmqctl", "-q", "set_vm_memory_high_watermark", WATERMARK, out: File::NULL)
  sleep 2

  # Let the consumers catch up with what was confirmed.
  deadline = now + 120
  loop do
    behind = confirmed.sum { |name, count| tracker.missing(name, count).size }
    break if behind.zero? || now > deadline

    sleep 1
  end
  reporter.stop

  missing = confirmed.to_h { |name, count| [name, tracker.missing(name, count)] }
  missing.each do |name, list|
    problems << "publisher #{name}: #{list.size} confirmed messages never received, first #{list.first(5)}" if list.any?
  end

  managed.stop
  consumer.stop
  tasks.each { |t| t.wait rescue nil } if false
  sleep 1
  publishing.close
  consuming.close
  task.children.each(&:stop)

  log_lines = File.readlines(LOG_PATH)
  levels    = log_lines.group_by { |line| line[/\b(DEBUG|INFO|WARN|ERROR|FATAL)\b/] }.transform_values(&:size)

  say "chaos:     #{chaos.sort.to_h}"
  say "events:    #{events.sort.to_h}"
  say "errors:    #{errors.sort.to_h}"
  say "confirmed: #{confirmed.sort.to_h} total #{confirmed.values.sum}"
  say "received:  unique #{tracker.unique}, duplicates #{tracker.duplicates}"
  say "log lines: #{levels}"
  problems.each { |problem| say "PROBLEM: #{problem}" }
  say(problems.empty? ? "RESULT: PASS" : "RESULT: FAIL (#{problems.size})")
ensure
  begin
    proxy.enable
  rescue StandardError
    nil
  end
end

system("docker", "exec", BROKER, "rabbitmqctl", "-q", "set_vm_memory_high_watermark", WATERMARK, out: File::NULL)
http(:Delete, "/api/vhosts/#{VHOST}")
