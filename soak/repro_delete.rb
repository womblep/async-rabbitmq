# frozen_string_literal: true
require "async"
require "async/rabbitmq"
require "logger"
require "net/http"
require "json"
require "securerandom"
require "toxiproxy"

VHOST = "repro-#{SecureRandom.hex(3)}"
COUNT = Integer(ENV.fetch("COUNT", "60"))
TYPE  = ENV.fetch("TYPE", "quorum").to_sym
Toxiproxy.host = "http://127.0.0.1:36000"
$stdout.sync = true
T0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
def t = format("%.3f", Process.clock_gettime(Process::CLOCK_MONOTONIC) - T0)

def http(verb, path, body = nil, port: 36_011)
  uri     = URI("http://127.0.0.1:#{port}#{path}")
  request = Net::HTTP.const_get(verb).new(uri)
  request.basic_auth("guest", "guest")
  if body
    request.body = JSON.generate(body)
    request["content-type"] = "application/json"
  end
  Net::HTTP.start(uri.host, uri.port) { |h| h.request(request) }
end

http(:Put, "/api/vhosts/#{VHOST}")
http(:Put, "/api/permissions/#{VHOST}/guest", { configure: ".*", write: ".*", read: ".*" })
Toxiproxy.populate((1..3).map { |n| { name: "n#{n}", listen: "0.0.0.0:3600#{n}", upstream: "rmq#{n}:5672" } })

logger = Logger.new(ENV.fetch("CLIENT_LOG", "repro-client.log"))
Async::RabbitMQ.log_level = :debug
outcomes = Hash.new(0)

Sync do |task|
  endpoints = [36_001, 36_002, 36_003].map { |port| Async::RabbitMQ::Endpoint.new("127.0.0.1", port, vhost: VHOST) }
  client    = Async::RabbitMQ::Client.open(endpoints, logger: logger, reconnect_max_interval: 2)
  log       = []
  client.on_connection_lost { |error| log << "#{t} connection lost: #{error.class}: #{error.message}" }
  client.on_reconnected     { log << "#{t} reconnected" }
  publisher = client.publisher("", resend: true)

  COUNT.times do |i|
    log.clear
    name     = "victim"
    queue    = client.queue(name, type: TYPE)
    consumer = queue.consumer(on_cancel: :resubscribe)
    consumer.on_cancelled    { log << "#{t} on_cancelled" }
    consumer.on_resubscribed { |attempts, _| log << "#{t} on_resubscribed attempts=#{attempts}" }
    got      = 0
    running  = task.async { consumer.each { |m| got += 1; m.ack } }
    3.times { publisher.publish("x", routing_key: name) }
    sleep 0.05 until consumer.consumer_tag && got >= 3
    logger.info("==== iteration #{i} deleting at #{t}")
    log << "#{t} deleting"
    code = http(:Delete, "/api/queues/#{VHOST}/#{name}", port: [36_011, 36_012, 36_013].sample).code
    log << "#{t} deleted (#{code})"
    result = task.with_timeout(8) { running.wait } rescue :still_running
    exists = http(:Get, "/api/queues/#{VHOST}/#{name}").code
    log << "#{t} each returned #{result.inspect}; queue GET #{exists}"
    outcome = "#{result}/lost=#{log.any? { |l| l.include?('connection lost') }}"
    outcomes[outcome] += 1
    if result != :cancelled
      puts "---- iteration #{i}: #{outcome}"
      puts log
    end
    consumer.close
    running.wait rescue nil
    client.forget(queue)
    http(:Delete, "/api/queues/#{VHOST}/#{name}")
    sleep 0.3
  end
  puts "outcomes: #{outcomes}"
  client.close
end
http(:Delete, "/api/vhosts/#{VHOST}")
