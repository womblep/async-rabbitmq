module AsyncRabbitMQ
  module SASL
    class Base
      # The AMQP mechanism name sent in connection.start-ok.
      def mechanism_name
        raise NotImplementedError
      end

      # Initial response sent with connection.start-ok.
      def initial_response
        raise NotImplementedError
      end

      # Handle a connection.secure challenge from the broker.
      # Returns the response string for connection.secure-ok.
      def challenge_response(_challenge)
        raise NotImplementedError
      end
    end

    class Plain < Base
      def initialize(username, password)
        @username = username
        @password = password
      end

      def mechanism_name
        "PLAIN"
      end

      def initial_response
        "\x00#{@username}\x00#{@password}"
      end

      def challenge_response(_challenge)
        initial_response
      end
    end

    class External < Base
      def mechanism_name
        "EXTERNAL"
      end

      # EXTERNAL relies on the transport layer (TLS client cert) for identity.
      # The initial response is empty — the broker extracts the identity from the cert.
      def initial_response
        ""
      end

      def challenge_response(_challenge)
        ""
      end
    end

    # Challenge-response mechanism for testing SASL multi-step flows.
    # The broker sends a challenge; we echo it back as the response.
    class CRRabbitMQDemo < Base
      def mechanism_name
        "RABBIT-CR-DEMO"
      end

      def initialize(username, password)
        @username = username
        @password = password
      end

      # Initial response is just the username — password comes in challenge_response.
      def initial_response
        @username
      end

      # The broker sends "Please tell me your password" (or similar);
      # we respond with the password.
      def challenge_response(_challenge)
        @password
      end
    end

    # Registry of built-in mechanism classes keyed by AMQP mechanism name.
    REGISTRY = {
      "PLAIN"          => Plain,
      "EXTERNAL"       => External,
      "RABBIT-CR-DEMO" => CRRabbitMQDemo,
    }.freeze

    # Select the best mechanism the broker supports, in preference order.
    # Returns an instantiated mechanism object.
    # Raises AuthenticationError if no common mechanism is found.
    def self.negotiate(broker_mechanisms_string, preferred: nil, username: nil, password: nil)
      broker_list = broker_mechanisms_string.split(/\s+/)

      if preferred
        name = preferred.upcase
        unless broker_list.include?(name)
          raise AuthenticationError, "Requested SASL mechanism #{name} not offered by broker (available: #{broker_list.join(', ')})"
        end
        return instantiate(name, username: username, password: password)
      end

      # Preference order: EXTERNAL (strongest, no credentials on wire), PLAIN (universal)
      %w[EXTERNAL PLAIN].each do |name|
        if broker_list.include?(name)
          return instantiate(name, username: username, password: password)
        end
      end

      raise AuthenticationError, "No supported SASL mechanism (broker offers: #{broker_list.join(', ')})"
    end

    def self.instantiate(name, username: nil, password: nil)
      klass = REGISTRY[name]
      raise AuthenticationError, "Unknown SASL mechanism: #{name}" unless klass

      if klass.instance_method(:initialize).arity == 0
        klass.new
      else
        klass.new(username, password)
      end
    end
    private_class_method :instantiate
  end
end
