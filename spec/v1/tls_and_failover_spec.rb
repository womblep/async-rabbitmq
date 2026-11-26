# frozen_string_literal: true

require_relative "helper"
require "openssl"
require "tmpdir"

RSpec.describe "TLS and several endpoints", :integration do
  CERTS = File.expand_path("../docker/certs", __dir__)

  def ca_path
    File.join(CERTS, "ca_certificate.pem")
  end

  def tls_available?
    return @tls_available if defined?(@tls_available)

    @tls_available = File.exist?(ca_path) && begin
      context = OpenSSL::SSL::SSLContext.new
      context.set_params(verify_mode: OpenSSL::SSL::VERIFY_NONE)
      raw = TCPSocket.new(RABBITMQ_HOST, RABBITMQ_TLS_PORT)
      ssl = OpenSSL::SSL::SSLSocket.new(raw, context)
      ssl.connect
      ssl.close
      true
    rescue SystemCallError, OpenSSL::SSL::SSLError, IOError
      false
    end
  end

  def tls_endpoint(vhost: "/", **options)
    options[:ssl_context] ||= Async::RabbitMQ::TLS.context(ca_certificates: ca_path)
    endpoint(vhost: vhost, port: RABBITMQ_TLS_PORT, **options)
  end

  # A client certificate signed by the test CA: [certificate PEM, key PEM].
  def client_certificate
    ca     = OpenSSL::X509::Certificate.new(File.read(ca_path))
    ca_key = OpenSSL::PKey.read(File.read(File.join(CERTS, "ca_key.pem")))
    key    = OpenSSL::PKey::RSA.new(2048)
    cert   = OpenSSL::X509::Certificate.new
    cert.version    = 2
    cert.serial     = rand(1..1_000_000)
    cert.subject    = OpenSSL::X509::Name.parse("/CN=spec-client")
    cert.issuer     = ca.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now - 60
    cert.not_after  = Time.now + 3600
    extensions = OpenSSL::X509::ExtensionFactory.new(ca, cert)
    cert.add_extension(extensions.create_extension("extendedKeyUsage", "clientAuth"))
    cert.sign(ca_key, OpenSSL::Digest.new("SHA256"))
    [cert.to_pem, key.to_pem]
  end

  def current_connection(client)
    client.instance_variable_get(:@connection)
  end

  def round_trip(client)
    queue = client.queue("tls.check")
    client.publisher { |publisher| publisher.publish("over the wire", routing_key: queue) }
    queue.consumer do |consumer|
      consumer.each do |message|
        message.ack
        return message.body
      end
    end
  end

  describe "amqps" do
    before { skip "RabbitMQ TLS port not available" unless tls_available? }

    it "connects with the broker's CA, and publishes and consumes" do
      with_vhost do |vhost|
        Async::RabbitMQ::Client.open(tls_endpoint(vhost: vhost)) do |client|
          expect(client.endpoint).to be_secure
          expect(client.endpoint.to_s).to start_with("amqps://")
          expect(round_trip(client)).to eq("over the wire")
        end
      end
    end

    it "takes its TLS settings from the URL" do
      with_vhost do |vhost|
        url = "amqps://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_TLS_PORT}/#{vhost}" \
              "?cacertfile=#{URI.encode_www_form_component(ca_path)}"
        Async::RabbitMQ::Client.open(url) do |client|
          expect(client).to be_connected
          expect(client.endpoint).to be_secure
        end
      end
    end

    it "refuses a broker whose CA it does not trust" do
      key   = OpenSSL::PKey::RSA.new(2048)
      other = OpenSSL::X509::Certificate.new
      other.version = 2
      other.serial  = 1
      other.subject = other.issuer = OpenSSL::X509::Name.parse("/CN=not the broker's CA")
      other.public_key = key.public_key
      other.not_before = Time.now - 60
      other.not_after  = Time.now + 3600
      other.sign(key, OpenSSL::Digest.new("SHA256"))
      context = Async::RabbitMQ::TLS.context(ca_certificates: other.to_pem)

      expect { Async::RabbitMQ::Client.open(tls_endpoint(ssl_context: context)) }
        .to raise_error(Async::RabbitMQ::ConnectionError, /SSL/)
    end

    it "refuses a protocol version the broker will not speak" do
      context = OpenSSL::SSL::SSLContext.new
      context.set_params(verify_mode: OpenSSL::SSL::VERIFY_NONE)
      context.max_version = OpenSSL::SSL::TLS1_1_VERSION

      expect { Async::RabbitMQ::Client.open(tls_endpoint(ssl_context: context)) }
        .to raise_error(Async::RabbitMQ::ConnectionError)
    end

    it "reconnects over TLS" do
      with_vhost do |vhost|
        Async::RabbitMQ::Client.open(tls_endpoint(vhost: vhost), reconnect_interval: 0.1) do |client|
          first = current_connection(client)
          sever!(first)
          wait_for { client.connected? && !current_connection(client).equal?(first) }
          expect(round_trip(client)).to eq("over the wire")
        end
      end
    end

    describe "with a client certificate" do
      it "presents it, and is let in" do
        cert, key = client_certificate
        context   = Async::RabbitMQ::TLS.context(cert: cert, key: key, ca_certificates: ca_path)
        with_vhost do |vhost|
          target = tls_endpoint(vhost: vhost, ssl_context: context)
          expect(target).to be_client_certificate
          Async::RabbitMQ::Client.open(target) do |client|
            # This broker offers PLAIN only, so the certificate identifies the
            # connection to TLS and the password still authenticates it.
            expect(round_trip(client)).to eq("over the wire")
          end
        end
      end

      it "takes the certificate and key from the URL" do
        cert, key = client_certificate
        Dir.mktmpdir do |dir|
          cert_path = File.join(dir, "client.pem")
          key_path  = File.join(dir, "client.key")
          File.write(cert_path, cert)
          File.write(key_path, key)
          with_vhost do |vhost|
            query = { cacertfile: ca_path, certfile: cert_path, keyfile: key_path }
            url   = "amqps://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_TLS_PORT}/#{vhost}?#{URI.encode_www_form(query)}"
            target = Async::RabbitMQ::Endpoint.parse(url)
            expect(target).to be_client_certificate
            expect(target.ssl_context.cert.subject.to_s).to include("spec-client")
            Async::RabbitMQ::Client.open(target) { |client| expect(client).to be_connected }
          end
        end
      end

      it "is refused when its certificate is not one the broker trusts" do
        key  = OpenSSL::PKey::RSA.new(2048)
        cert = OpenSSL::X509::Certificate.new
        cert.version = 2
        cert.serial  = 1
        cert.subject = cert.issuer = OpenSSL::X509::Name.parse("/CN=self-signed stranger")
        cert.public_key = key.public_key
        cert.not_before = Time.now - 60
        cert.not_after  = Time.now + 3600
        cert.sign(key, OpenSSL::Digest.new("SHA256"))
        context = Async::RabbitMQ::TLS.context(cert: cert.to_pem, key: key.to_pem, ca_certificates: ca_path)

        expect { Async::RabbitMQ::Client.open(tls_endpoint(ssl_context: context, timeout: 3)) }
          .to raise_error(Async::RabbitMQ::ConnectionError)
      end

      it "raises AuthenticationError when EXTERNAL is asked for and the broker does not offer it" do
        cert, key = client_certificate
        context   = Async::RabbitMQ::TLS.context(cert: cert, key: key, ca_certificates: ca_path)

        expect { Async::RabbitMQ::Client.open(tls_endpoint(ssl_context: context, auth_mechanism: "EXTERNAL")) }
          .to raise_error(Async::RabbitMQ::AuthenticationError, /EXTERNAL/)
      end
    end
  end

  describe "several endpoints" do
    def dead_endpoint(vhost)
      endpoint(vhost: vhost, port: 1, timeout: 2)
    end

    it "connects to the first that answers, and says which" do
      with_vhost do |vhost|
        good = endpoint(vhost: vhost)
        Async::RabbitMQ::Client.open([dead_endpoint(vhost), good], shuffle: false) do |client|
          expect(client).to be_connected
          expect(client.endpoint).to equal(good)
          expect(client.endpoints.size).to eq(2)
        end
      end
    end

    it "takes URLs as well as endpoints" do
      with_vhost do |vhost|
        urls = ["amqp://guest:guest@#{RABBITMQ_HOST}:1/#{vhost}?connection_timeout=2",
                "amqp://guest:guest@#{RABBITMQ_HOST}:#{RABBITMQ_PORT}/#{vhost}"]
        Async::RabbitMQ::Client.open(urls, shuffle: false) do |client|
          expect(client.endpoint.port).to eq(RABBITMQ_PORT)
        end
      end
    end

    it "raises ConnectionError when none answers, and for an empty list" do
      expect { Async::RabbitMQ::Client.open([dead_endpoint("/"), dead_endpoint("/")]) }
        .to raise_error(Async::RabbitMQ::ConnectionError, /Could not connect/)
      expect { Async::RabbitMQ::Client.new([]) }.to raise_error(ArgumentError, /at least one/)
    end

    it "does not try the rest once the credentials are refused" do
      refused = endpoint(password: "wrong")
      client  = Async::RabbitMQ::Client.new([refused, endpoint], shuffle: false)
      expect { client.connect }.to raise_error(Async::RabbitMQ::AuthenticationError)
      expect(client).not_to be_connected
    end

    it "moves to another endpoint when the one it was on goes away" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_vhost do |vhost|
        through_proxy = endpoint(vhost: vhost, port: TOXIPROXY_PORT, timeout: 1)
        direct        = endpoint(vhost: vhost)
        Async::RabbitMQ::Client.open([through_proxy, direct], shuffle: false, reconnect_interval: 0.1) do |client|
          expect(client.endpoint).to equal(through_proxy)
          queue     = client.queue("work")
          publisher = client.publisher
          publisher.publish("before", routing_key: queue)

          toxiproxy_rabbitmq.down do
            wait_for { client.connected? && client.endpoint.equal?(direct) }
            wait_for { publisher.connected? }
            expect(publisher.publish("after", routing_key: queue)).to be true
          end
          expect(queue.message_count).to eq(2)
        end
      end
    end

    it "spreads clients across the endpoints when it shuffles" do
      skip "toxiproxy not available" unless toxiproxy_available?

      with_vhost do |vhost|
        endpoints = [endpoint(vhost: vhost, port: TOXIPROXY_PORT), endpoint(vhost: vhost)]
        ports = Array.new(14) do
          Async::RabbitMQ::Client.open(endpoints) { |client| client.endpoint.port }
        end
        expect(ports.uniq).to match_array([TOXIPROXY_PORT, RABBITMQ_PORT])
      end
    end
  end
end
