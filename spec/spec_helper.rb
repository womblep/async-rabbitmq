require "simplecov"
SimpleCov.start do
  add_filter "/spec/"
  # Only enforce coverage minimum when integration tests actually ran (RabbitMQ available).
  minimum_coverage ENV["ENFORCE_COVERAGE"] ? 90 : 0
end

require "async"
require "async/rspec"
require "async_rabbitmq"
require "uri"
require "securerandom"
require "socket"

# Integration test helpers — require a live RabbitMQ.
# Tests are skipped automatically when RABBITMQ_URL is not set.
RABBITMQ_URL      = ENV.fetch("RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:15682")
RABBITMQ_HOST     = URI.parse(RABBITMQ_URL.sub("amqp://", "http://")).host rescue "127.0.0.1"
RABBITMQ_PORT     = (ENV["RABBITMQ_PORT"] || 15682).to_i
RABBITMQ_TLS_PORT = (ENV["RABBITMQ_TLS_PORT"] || 15681).to_i
RABBITMQ_MGMT     = (ENV["RABBITMQ_MGMT_PORT"] || 15680).to_i

# ---------------------------------------------------------------------------
# Toxiproxy — optional TCP fault injection.
#
# TOXIPROXY_RABBITMQ_UPSTREAM defaults to "rabbitmq:5672" (the Docker service
# name) because docker-compose is the standard way to run the test suite.
# If you run toxiproxy directly on the host, set:
#   TOXIPROXY_RABBITMQ_UPSTREAM=127.0.0.1:5672
# ---------------------------------------------------------------------------
TOXIPROXY_HOST                  = ENV.fetch("TOXIPROXY_HOST",                  "http://127.0.0.1:18474")
TOXIPROXY_PORT                  = (ENV["TOXIPROXY_RABBITMQ_PORT"] || 21111).to_i
TOXIPROXY_TLS_PORT              = (ENV["TOXIPROXY_RABBITMQ_TLS_PORT"] || 21112).to_i
TOXIPROXY_RABBITMQ_UPSTREAM     = ENV.fetch("TOXIPROXY_RABBITMQ_UPSTREAM",     "rabbitmq:5672")
TOXIPROXY_RABBITMQ_LISTEN       = ENV.fetch("TOXIPROXY_RABBITMQ_LISTEN",       "0.0.0.0:11111")
TOXIPROXY_RABBITMQ_TLS_UPSTREAM = ENV.fetch("TOXIPROXY_RABBITMQ_TLS_UPSTREAM", "rabbitmq:5671")
TOXIPROXY_RABBITMQ_TLS_LISTEN   = ENV.fetch("TOXIPROXY_RABBITMQ_TLS_LISTEN",   "0.0.0.0:11112")

require "toxiproxy"
Toxiproxy.host = TOXIPROXY_HOST

# ---------------------------------------------------------------------------
# ServiceBootstrap — starts docker-compose services automatically when
# RabbitMQ is not reachable. Checks once at load time; services are left
# running between test runs for speed. Stop manually with:
#   docker compose -f spec/docker-compose.yml down
# ---------------------------------------------------------------------------
module ServiceBootstrap
  COMPOSE_FILE = File.expand_path("docker-compose.yml", __dir__)
  CERTS_DIR    = File.expand_path("docker/certs",       __dir__)
  GEN_CERTS    = File.expand_path("docker/gen-certs.sh", __dir__)

  def self.ensure_running!
    return if port_open?(RABBITMQ_HOST, RABBITMQ_PORT)

    $stderr.puts "--> RabbitMQ not detected — starting docker compose services..."
    generate_certs unless certs_present?
    compose_up
    $stderr.puts "--> Services ready."
  end

  def self.certs_present?
    File.exist?(File.join(CERTS_DIR, "ca_certificate.pem"))
  end

  def self.generate_certs
    $stderr.puts "--> Generating TLS certificates..."
    system("bash", GEN_CERTS) or raise "gen-certs.sh failed (exit #{$?.exitstatus})"
  end

  # --wait blocks until every service with a healthcheck reports healthy,
  # so RabbitMQ is guaranteed ready before this returns.
  def self.compose_up
    system("docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "--wait") \
      or raise "docker compose up failed (exit #{$?.exitstatus})"
  end

  def self.port_open?(host, port)
    TCPSocket.new(host, port).close
    true
  rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, SocketError
    false
  end
end

ServiceBootstrap.ensure_running!

# ---------------------------------------------------------------------------
# Integration test helpers.
# ---------------------------------------------------------------------------
module IntegrationHelpers
  def rabbitmq_available?
    TCPSocket.new(RABBITMQ_HOST, RABBITMQ_PORT).close
    true
  rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT
    false
  end

  # Returns true when the toxiproxy daemon is reachable.
  # Creates the rabbitmq and rabbitmq_tls proxies if they don't already exist.
  def toxiproxy_available?
    return @toxiproxy_available if defined?(@toxiproxy_available)
    proxies_to_create = []
    begin Toxiproxy[:rabbitmq]; rescue Toxiproxy::NotFound
      proxies_to_create << {
        name:     "rabbitmq",
        listen:   TOXIPROXY_RABBITMQ_LISTEN,
        upstream: TOXIPROXY_RABBITMQ_UPSTREAM,
      }
    end
    begin Toxiproxy[:rabbitmq_tls]; rescue Toxiproxy::NotFound
      proxies_to_create << {
        name:     "rabbitmq_tls",
        listen:   TOXIPROXY_RABBITMQ_TLS_LISTEN,
        upstream: TOXIPROXY_RABBITMQ_TLS_UPSTREAM,
      }
    end
    Toxiproxy.populate(proxies_to_create) unless proxies_to_create.empty?
    @toxiproxy_available = true
  rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT, StandardError
    @toxiproxy_available = false
  end

  # Returns the Toxiproxy::Proxy for RabbitMQ AMQP traffic.
  def toxiproxy_rabbitmq
    Toxiproxy[:rabbitmq]
  end

  # Returns the Toxiproxy::Proxy for RabbitMQ TLS traffic.
  def toxiproxy_rabbitmq_tls
    Toxiproxy[:rabbitmq_tls]
  end

  # Create a new isolated vhost for this test and return a connected Session.
  # Create an isolated vhost, open a Session, yield, then clean up.
  # Optional frame_max: lets callers test with a custom frame size.
  def isolated_session(vhost: nil, frame_max: nil)
    vhost ||= "test-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    opts = { host: RABBITMQ_HOST, port: RABBITMQ_PORT, vhost: vhost }
    opts[:frame_max] = frame_max if frame_max
    session = AsyncRabbitMQ::Session.new(**opts)
    session.connect
    yield session, vhost
  ensure
    session&.close rescue nil
    delete_vhost(vhost) rescue nil
  end

  def create_vhost(vhost)
    http_put("/api/vhosts/#{URI.encode_www_form_component(vhost)}")
    http_put("/api/permissions/#{URI.encode_www_form_component(vhost)}/guest",
             { configure: ".*", write: ".*", read: ".*" }.to_json)
  end

  def delete_vhost(vhost)
    http_delete("/api/vhosts/#{URI.encode_www_form_component(vhost)}")
  end

  def http_put(path, body = nil)
    mgmt_request(:put, path, body)
  end

  def http_delete(path)
    mgmt_request(:delete, path)
  end

  def mgmt_request(method, path, body = nil)
    require "net/http"
    uri = URI("http://#{RABBITMQ_HOST}:#{RABBITMQ_MGMT}#{path}")
    req = case method
          when :put    then Net::HTTP::Put.new(uri)
          when :delete then Net::HTTP::Delete.new(uri)
          end
    req["Content-Type"] = "application/json"
    req.basic_auth("guest", "guest")
    req.body = body if body
    Net::HTTP.start(uri.host, uri.port) { |http| http.request(req) }
  end
end

RSpec.configure do |config|
  config.include IntegrationHelpers, :integration

  # Wrap each integration test in Sync { } so async 2.x APIs work (Async::Task.current, etc.).
  config.around(:each, :integration) do |example|
    if rabbitmq_available?
      Sync { example.run }
    else
      skip "RabbitMQ not available"
    end
  end

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.disable_monkey_patching!
  config.warnings = true
  config.order = :random
  Kernel.srand config.seed
end
