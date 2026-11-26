# frozen_string_literal: true

require "uri"
require "io/endpoint"
require "io/endpoint/host_endpoint"
require "io/endpoint/ssl_endpoint"
require "amq/uri"

require_relative "tls"

module Async
  module RabbitMQ
    # Where a broker is and how to talk to it: host, port, virtual host,
    # credentials, TLS, and the connection settings to ask for.
    #
    #   endpoint = Async::RabbitMQ::Endpoint.parse("amqps://app:secret@rabbit.example.com/orders")
    #   endpoint = Async::RabbitMQ::Endpoint.parse("amqp://localhost", heartbeat: 30)
    #
    # The URL may carry +heartbeat+, +channel_max+, +connection_timeout+ and
    # +auth_mechanism+ as query parameters, and for amqps +verify+,
    # +cacertfile+, +certfile+ and +keyfile+. A keyword argument wins over the
    # same setting in the URL.
    class Endpoint
      DEFAULT_PORT        = 5672
      DEFAULT_SECURE_PORT = 5671

      # A URL with no credentials means the broker's default account, which a
      # broker accepts from localhost only.
      DEFAULT_USERNAME = "guest"
      DEFAULT_PASSWORD = "guest"

      DEFAULTS = {
        heartbeat: 60,          # seconds; 0 asks for none
        frame_max: 131_072,
        channel_max: 2047,
        timeout: 10,            # seconds to connect and complete the handshake
        call_timeout: 15,       # seconds to wait for the broker to answer a request; nil waits for ever
        connection_name: nil,   # shown in the broker's management UI
        auth_mechanism: nil,    # nil negotiates; "PLAIN" or "EXTERNAL" forces
        tcp_user_timeout: nil   # milliseconds; nil derives it from the heartbeat
      }.freeze

      # Parse an amqp:// or amqps:// URL. See the class comment for +options+.
      def self.parse(string, **options)
        parsed = AMQ::URI.parse(string)
        query  = ::URI.parse(string).query
        params = query ? ::URI.decode_www_form(query).to_h : {}

        from_url = {}
        from_url[:heartbeat]      = Integer(params["heartbeat"])          if params.key?("heartbeat")
        from_url[:timeout]        = Integer(params["connection_timeout"]) if params.key?("connection_timeout")
        from_url[:channel_max]    = Integer(params["channel_max"])        if params.key?("channel_max")
        from_url[:auth_mechanism] = params["auth_mechanism"]              if params.key?("auth_mechanism")

        secure = parsed[:ssl] ? true : false
        if secure && %w[verify cacertfile certfile keyfile].any? { |key| params.key?(key) }
          from_url[:ssl_context] = TLS.context(cert: parsed[:certfile], key: parsed[:keyfile],
                                               ca_certificates: parsed[:cacertfile],
                                               verify_peer: params["verify"] != "false")
          from_url[:client_certificate] = true if parsed[:certfile]
        end

        # A URL carries an IPv6 literal in brackets; the socket wants it bare.
        host = (parsed[:host] || "localhost").delete_prefix("[").delete_suffix("]")
        # "amqp://host/" parses to an empty virtual host. Nobody means the one
        # literally named "", so it is read as the default.
        vhost = parsed[:vhost]
        vhost = "/" if vhost.nil? || vhost.empty?

        new(host, parsed[:port], secure: secure, vhost: vhost,
            username: parsed[:user] || DEFAULT_USERNAME, password: parsed[:pass] || DEFAULT_PASSWORD,
            **from_url, **options)
      end

      attr :hostname, :port, :vhost, :username, :password, :options

      def initialize(hostname, port = nil, secure: false, vhost: "/", username: DEFAULT_USERNAME,
                     password: DEFAULT_PASSWORD, ssl_context: nil, client_certificate: nil, **options)
        unknown = options.keys - DEFAULTS.keys
        raise ArgumentError, "unknown endpoint option(s): #{unknown.join(', ')}" unless unknown.empty?

        @hostname    = hostname
        @secure      = secure || !ssl_context.nil?
        @port        = port || (@secure ? DEFAULT_SECURE_PORT : DEFAULT_PORT)
        @vhost       = vhost
        @username    = username
        @password    = password
        @ssl_context = ssl_context
        # Whether this connection can be identified by a TLS certificate, which
        # decides if EXTERNAL is preferred to PLAIN. A context handed in ready
        # made is taken to carry one if it has a certificate set.
        @client_certificate = client_certificate.nil? ? !ssl_context&.cert.nil? : client_certificate
        @options     = DEFAULTS.merge(options).freeze
      end

      def secure?
        @secure
      end

      def client_certificate?
        @client_certificate
      end

      def scheme
        @secure ? "amqps" : "amqp"
      end

      DEFAULTS.each_key do |name|
        define_method(name) { @options[name] }
      end

      def ssl_context
        @ssl_context ||= TLS.context if @secure
      end

      # The same endpoint with some settings changed.
      def with(**changes)
        fields = { secure: @secure, vhost: @vhost, username: @username, password: @password,
                   ssl_context: @ssl_context, client_certificate: @client_certificate }
        self.class.new(@hostname, @port, **fields.merge(@options).merge(changes))
      end

      # Open the socket: TCP, then TLS if this is amqps. The caller owns it.
      def connect
        tcp    = ::IO::Endpoint.tcp(@hostname, @port)
        socket = tcp.connect
        socket = secure_socket(tcp, socket) if @secure
        apply_socket_timeouts(socket.respond_to?(:to_io) ? socket.to_io : socket)
        socket
      end

      # Never includes the password.
      def to_s
        host = @hostname.include?(":") ? "[#{@hostname}]" : @hostname
        "#{scheme}://#{@username}@#{host}:#{@port}/#{::URI.encode_www_form_component(@vhost)}"
      end

      def inspect
        "#<#{self.class.name} #{self}>"
      end

      # Two endpoints for the same broker node, whatever their settings.
      def address
        [@hostname, @port]
      end

      private

      # The TLS handshake, run here rather than through SSLEndpoint#connect.
      # That method closes the socket on a failed handshake with a bare
      # +rescue+, which catches StandardError only: a cancel arriving mid
      # handshake skips it, and nothing else would close the TCP socket until
      # GC. The endpoint's own pieces are still used: make_socket sets
      # sync_close, so closing the TLS socket closes the TCP one, and the
      # hostname drives SNI and certificate verification.
      def secure_socket(tcp, tcp_socket)
        endpoint = ::IO::Endpoint::SSLEndpoint.new(tcp, ssl_context: ssl_context)
        socket   = tcp_socket
        socket   = endpoint.make_socket(tcp_socket)
        if (name = endpoint.hostname)
          socket.hostname = name
        end
        socket.connect
        socket
      rescue Exception # rubocop:disable Lint/RescueException
        socket&.close rescue nil
        raise
      end

      # Bound how long the kernel retransmits unacknowledged data before it
      # gives up on the socket. Without it a peer that vanishes mid-write, with
      # the send buffer full, is only noticed when the TCP retransmit timer
      # runs out: about 15 minutes on Linux, far longer than the heartbeat
      # promises. TCP_USER_TIMEOUT is Linux-only; elsewhere the heartbeat's own
      # bounded write is the backstop, so a missing constant is not an error.
      def apply_socket_timeouts(socket)
        millis = tcp_user_timeout
        millis ||= heartbeat.to_i.positive? ? heartbeat.to_i * 2 * 1000 : nil
        return unless millis&.positive? && Socket.const_defined?(:TCP_USER_TIMEOUT)

        socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_USER_TIMEOUT, millis)
      rescue StandardError => error
        Console.debug(self, "Could not set TCP_USER_TIMEOUT", exception: error)
      end
    end
  end
end
