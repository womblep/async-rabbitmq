s = File.binread("soak.rb")
def sub!(s, old, new)
  raise "not found: #{old[0, 60]}" unless s.include?(old)
  s.sub!(old) { new }
end

sub!(s, %q[PROXY     = 21_111
MGMT      = 15_680
BROKER    = ENV.fetch("BROKER_CONTAINER", "spec-rabbitmq-1")],
     %q[PORTS     = [36_001, 36_002, 36_003].freeze
MGMT      = [36_011, 36_012, 36_013].freeze
NODES     = %w[soak-rmq1 soak-rmq2 soak-rmq3].freeze])
sub!(s, %q[Toxiproxy.host = "http://127.0.0.1:18474"], %q[Toxiproxy.host = "http://127.0.0.1:36000"])

# Management requests go to whichever node answers.
sub!(s, %q[def http(verb, path, body = nil)
  uri     = URI("http://#{HOST}:#{MGMT}#{path}")],
     %q[def http(verb, path, body = nil)
  failure = nil
  MGMT.each do |port|
    return http_on(port, verb, path, body)
  rescue SystemCallError, IOError, Timeout::Error, Net::OpenTimeout, Net::ReadTimeout, EOFError => error
    failure = error
  end
  raise failure
end

def docker(*arguments) = system("docker", *arguments, out: File::NULL, err: File::NULL)

def rabbitmqctl(node, *arguments) = docker("exec", node, "rabbitmqctl", "-q", *arguments)

def http_on(port, verb, path, body = nil)
  uri     = URI("http://#{HOST}:#{port}#{path}")])

sub!(s, %q[begin
  Toxiproxy[:rabbitmq]
rescue Toxiproxy::NotFound
  Toxiproxy.populate([{ name: "rabbitmq", listen: "0.0.0.0:11111", upstream: "rabbitmq:5672" }])
end
proxy = Toxiproxy[:rabbitmq]
proxy.enable],
     %q[Toxiproxy.populate((1..3).map { |n| { name: "n#{n}", listen: "0.0.0.0:3600#{n}", upstream: "rmq#{n}:5672" } })
proxies = (1..3).map { |n| Toxiproxy[:"n#{n}"] }
proxies.each(&:enable)
restore = -> { proxies.each { |proxy| proxy.enable rescue nil } }])

sub!(s, %q[  endpoint   = Async::RabbitMQ::Endpoint.new(HOST, PROXY, vhost: VHOST)
  publishing = Async::RabbitMQ::Client.open(endpoint, logger: logger, reconnect_max_interval: 5)
  consuming  = Async::RabbitMQ::Client.open(endpoint, logger: logger, reconnect_max_interval: 5)
  hook_client(publishing, "pub", events)
  hook_client(consuming, "con", events)

  work   = consuming.queue("soak.work")
  plain  = consuming.queue("soak.plain")
  publishing.queue("soak.work")
  publishing.queue("soak.plain")
],
     %q[  endpoints  = PORTS.map { |port| Async::RabbitMQ::Endpoint.new(HOST, port, vhost: VHOST) }
  # A Cluster, with a client per node, and a single Client that fails over
  # between the three.
  cluster    = Async::RabbitMQ::Cluster.open(endpoints, logger: logger, reconnect_max_interval: 5)
  failover   = Async::RabbitMQ::Client.open(endpoints, logger: logger, reconnect_max_interval: 5)
  hook_client(cluster, "cl", events)
  hook_client(failover, "fo", events)
  publishing = failover
  consuming  = failover

  work   = cluster.queue("soak.work", type: :quorum)
  plain  = cluster.queue("soak.plain", type: :quorum)
  failover.queue("soak.work", type: :quorum)
  failover.queue("soak.plain", type: :quorum)
])
sub!(s, %q[resending = publishing.publisher("", resend: true)
  retrying  = publishing.publisher("")],
     %q[resending = cluster.publisher("", resend: true)
  retrying  = cluster.publisher("")])
sub!(s, %q[work.managed_consumer(concurrency: 8, prefetch: 32, on_cancel: :resubscribe)],
     %q[work.managed_consumer(concurrency: 8, prefetch: 32, on_cancel: :resubscribe, on_connection_loss: :move)])
sub!(s, %q[queue  = consuming.queue("soak.victim")], %q[queue  = consuming.queue("soak.victim", type: :quorum)])

# Chaos actions.
from = s.index("  actions = {")
to   = s.index("  deck    = weights.flat_map")
raise "actions" unless from && to
s[from...to] = <<'RUBY'
  await = lambda do |node|
    60.times do
      break if rabbitmqctl(node, "await_startup")

      sleep 2
    end
  end
  actions = {
    cut_node:      -> do
      proxy = proxies.sample
      proxy.disable
      sleep rand(1.0..8.0)
      proxy.enable
    end,
    long_cut_node: -> do
      proxy = proxies.sample
      proxy.disable
      sleep 45
      proxy.enable
    end,
    cut_all:       -> do
      proxies.each(&:disable)
      sleep rand(1.0..8.0)
      proxies.each(&:enable)
    end,
    broker_close:  -> do
      connections = JSON.parse(http(:Get, "/api/vhosts/#{VHOST}/connections").body)
      connections.each do |c|
        response = http(:Delete, "/api/connections/#{URI.encode_www_form_component(c['name']).gsub('+', '%20')}")
        chaos[:connections_closed] += 1 if response.code.to_i < 300
      end
    end,
    memory_alarm:  -> do
      node = NODES.sample
      rabbitmqctl(node, "set_vm_memory_high_watermark", "0.00001")
      sleep 6
      rabbitmqctl(node, "set_vm_memory_high_watermark", WATERMARK)
    end,
    stop_node:     -> do
      node = NODES.sample
      docker("stop", "-t", "10", node)
      sleep rand(10.0..25.0)
      docker("start", node)
      await.call(node)
    end,
    kill_node:     -> do
      node = NODES.sample
      docker("kill", node)
      sleep rand(5.0..15.0)
      docker("start", node)
      await.call(node)
    end,
    rebalance:     -> do
      docker("exec", NODES.sample, "rabbitmq-queues", "rebalance", "quorum")
    end,
    delete_queue:  -> do
      http(:Delete, "/api/queues/#{VHOST}/soak.victim")
    end,
    cancel:        -> do
      # As the specs do: cancel at the broker, then tell the consumer the
      # broker did it. The queue stays, so the consumer should consume again.
      channel = consumer.instance_variable_get(:@channel)
      tag     = consumer.consumer_tag
      next chaos[:cancel_skipped] += 1 unless channel&.open? && tag

      channel.call(AMQ::Protocol::Basic::Cancel.encode(channel.id, tag, false), AMQ::Protocol::Basic::CancelOk)
      consumer.cancelled(nil)
    end
  }
  weights = { cut_node: 4, long_cut_node: 1, cut_all: 2, broker_close: 3, memory_alarm: 1, stop_node: 3, kill_node: 2,
              rebalance: 2, delete_queue: 2, cancel: 3 }
RUBY

sub!(s, "      ensure\n        proxy.enable rescue nil\n      end\n      sleep 10\n", "      ensure\n        restore.call\n      end\n      sleep 10\n")
sub!(s, %q["links=#{publishing.link_count}/#{consuming.link_count}"],
     %q["links=#{cluster.clients.map(&:link_count).join('/')}+#{failover.link_count} " \
          "up=#{cluster.clients.count(&:connected?)}"])
sub!(s, %q[  chaos_task.wait
  proxy.enable
  system("docker", "exec", BROKER, "rabbitmqctl", "-q", "set_vm_memory_high_watermark", WATERMARK, out: File::NULL)
  sleep 2],
     %q[  chaos_task.wait
  restore.call
  NODES.each { |node| rabbitmqctl(node, "set_vm_memory_high_watermark", WATERMARK) }
  sleep 2])
sub!(s, "  deadline = now + 120\n", "  deadline = now + 240\n")
sub!(s, "  publishing.close\n  consuming.close\n", "  cluster.close\n  failover.close\n")
sub!(s, %q[ensure
  begin
    proxy.enable
  rescue StandardError
    nil
  end
end

system("docker", "exec", BROKER, "rabbitmqctl", "-q", "set_vm_memory_high_watermark", WATERMARK, out: File::NULL)],
     %q[ensure
  restore.call
end

NODES.each { |node| rabbitmqctl(node, "set_vm_memory_high_watermark", WATERMARK) }])
File.binwrite("soak_cluster.rb", s)
