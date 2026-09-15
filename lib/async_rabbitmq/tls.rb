# frozen_string_literal: true

require "openssl"

module AsyncRabbitMQ
  # Builds an OpenSSL::SSL::SSLContext from the things people actually have: a
  # client certificate, its key, and the CA that signed the broker's. Nobody
  # should need to know about X509::Store to connect over TLS; anyone who needs
  # more than this passes a ready-made +tls_context:+ to Session instead.
  #
  #   AsyncRabbitMQ::Session.new(tls_cert: "client.pem", tls_key: "client.key",
  #                              tls_ca_certificates: ["ca.pem"])
  #
  # Every certificate and key option takes either a file path or PEM text, so
  # material from a secrets manager works as well as a file. A certificate
  # value may hold the leaf followed by its chain; a CA value may hold several
  # certificates; +tls_ca_certificates+ takes one value or a list. With no CAs
  # given the system trust store is used.
  module TLS
    VERSIONS = {
      "TLS1_2" => OpenSSL::SSL::TLS1_2_VERSION,
      "TLS1_3" => OpenSSL::SSL::TLS1_3_VERSION
    }.freeze

    module_function

    def context(cert: nil, key: nil, ca_certificates: nil, verify_peer: true, min_version: :TLS1_2,
                certificate_store: nil)
      ctx = OpenSSL::SSL::SSLContext.new
      # set_params first: it supplies the system trust store, hostname
      # verification and OpenSSL's recommended options, which we then narrow.
      ctx.set_params(verify_mode: verify_peer ? OpenSSL::SSL::VERIFY_PEER : OpenSSL::SSL::VERIFY_NONE)
      ctx.verify_hostname = verify_peer
      ctx.min_version     = version(min_version)

      if cert
        leaf, *chain = OpenSSL::X509::Certificate.load(material(cert, "tls_cert"))
        ctx.cert = leaf
        ctx.extra_chain_cert = chain unless chain.empty?
      end
      ctx.key = OpenSSL::PKey.read(material(key, "tls_key")) if key
      raise ArgumentError, "tls_key given without tls_cert" if key && !cert

      if certificate_store
        ctx.cert_store = certificate_store
      elsif ca_certificates
        ctx.cert_store = store(Array(ca_certificates))
      end
      ctx
    end

    # A trust store holding exactly the given CAs (paths or PEM text, each of
    # which may contain several certificates).
    def store(ca_certificates)
      store = OpenSSL::X509::Store.new
      ca_certificates.each do |value|
        OpenSSL::X509::Certificate.load(material(value, "tls_ca_certificates")).each { |c| store.add_cert(c) }
      end
      store
    end

    # PEM text is returned as is; anything else is read as a file path.
    def material(value, option)
      text = value.to_s
      return text if text.include?("-----BEGIN")
      return File.read(text) if File.file?(text)

      raise ArgumentError, "#{option}: #{value.inspect} is neither PEM text nor a readable file"
    end

    # :TLS1_2, "TLS1_3", "TLSv1.2", 1.2 or an OpenSSL::SSL::*_VERSION constant.
    def version(value)
      return value if value.is_a?(Integer)

      key = value.to_s.sub(/\ATLSv?/i, "TLS").tr(".", "_").upcase
      key = "TLS#{key}" unless key.start_with?("TLS")
      VERSIONS.fetch(key) do
        raise ArgumentError, "tls_min_version #{value.inspect}: expected one of #{VERSIONS.keys.join(', ')}"
      end
    end
  end
end
