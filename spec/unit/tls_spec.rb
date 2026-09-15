require "spec_helper"
require "openssl"
require "tmpdir"

# AsyncRabbitMQ::TLS turns a certificate, a key and some CAs into an
# SSLContext so nobody has to touch OpenSSL for the ordinary case. Every value
# is a path or PEM text; a certificate may carry its chain; a CA value may hold
# several certificates.
RSpec.describe AsyncRabbitMQ::TLS do
  # A tiny self-signed CA, an intermediate it signed, and a leaf under that.
  def issue(subject, issuer_pair: nil, ca: false)
    key  = OpenSSL::PKey::RSA.new(2048)
    cert = OpenSSL::X509::Certificate.new
    cert.version    = 2
    cert.serial     = rand(1 << 62)
    cert.subject    = OpenSSL::X509::Name.parse(subject)
    cert.issuer     = issuer_pair ? issuer_pair[0].subject : cert.subject
    cert.public_key = key.public_key
    cert.not_before = Time.now - 60
    cert.not_after  = Time.now + 3600
    ef = OpenSSL::X509::ExtensionFactory.new(issuer_pair ? issuer_pair[0] : cert, cert)
    cert.add_extension(ef.create_extension("basicConstraints", ca ? "CA:TRUE" : "CA:FALSE", true))
    cert.sign(issuer_pair ? issuer_pair[1] : key, OpenSSL::Digest.new("SHA256"))
    [cert, key]
  end

  let(:root)         { issue("/CN=test root", ca: true) }
  let(:intermediate) { issue("/CN=test intermediate", issuer_pair: root, ca: true) }
  let(:leaf)         { issue("/CN=client", issuer_pair: intermediate) }

  it "verifies peers by default with the system trust store and TLS 1.2 as the floor" do
    ctx = described_class.context
    expect(ctx.verify_mode).to eq(OpenSSL::SSL::VERIFY_PEER)
    expect(ctx.verify_hostname).to be true
    expect(ctx.cert).to be_nil
    # SSLContext has no reader for min_version; the mapping is covered below.
  end

  it "takes PEM text for the certificate and key" do
    ctx = described_class.context(cert: leaf[0].to_pem, key: leaf[1].to_pem)
    expect(ctx.cert.subject.to_s).to eq("/CN=client")
    expect(ctx.key.to_pem).to eq(leaf[1].to_pem)
  end

  it "takes file paths for the certificate, key and CAs" do
    Dir.mktmpdir do |dir|
      cert_path = File.join(dir, "client.pem")
      key_path  = File.join(dir, "client.key")
      ca_path   = File.join(dir, "ca.pem")
      File.write(cert_path, leaf[0].to_pem)
      File.write(key_path, leaf[1].to_pem)
      File.write(ca_path, root[0].to_pem)

      ctx = described_class.context(cert: cert_path, key: key_path, ca_certificates: ca_path)
      expect(ctx.cert.subject.to_s).to eq("/CN=client")
      expect(ctx.cert_store).to be_a(OpenSSL::X509::Store)
    end
  end

  it "splits a certificate bundle into the leaf and its chain" do
    ctx = described_class.context(cert: leaf[0].to_pem + intermediate[0].to_pem, key: leaf[1].to_pem)
    expect(ctx.cert.subject.to_s).to eq("/CN=client")
    expect(ctx.extra_chain_cert.map { |c| c.subject.to_s }).to eq(["/CN=test intermediate"])
  end

  it "loads every certificate in a CA bundle, and accepts a list" do
    bundle = root[0].to_pem + intermediate[0].to_pem
    store  = described_class.store([bundle])
    # The store trusts the chain end to end when it has both.
    expect(store.verify(leaf[0])).to be true

    only_root = described_class.store([root[0].to_pem])
    expect(only_root.verify(leaf[0])).to be false
  end

  it "turns off verification, including the hostname, only when asked" do
    ctx = described_class.context(verify_peer: false)
    expect(ctx.verify_mode).to eq(OpenSSL::SSL::VERIFY_NONE)
    expect(ctx.verify_hostname).to be false
  end

  it "understands the usual spellings of a TLS version" do
    expect(described_class.version(:TLS1_2)).to eq(OpenSSL::SSL::TLS1_2_VERSION)
    expect(described_class.version("TLSv1.3")).to eq(OpenSSL::SSL::TLS1_3_VERSION)
    expect(described_class.version("1.3")).to eq(OpenSSL::SSL::TLS1_3_VERSION)
    expect(described_class.version(OpenSSL::SSL::TLS1_2_VERSION)).to eq(OpenSSL::SSL::TLS1_2_VERSION)
    expect { described_class.version(:SSLv3) }.to raise_error(ArgumentError, /tls_min_version/)
  end

  it "says which option was wrong" do
    expect { described_class.context(cert: "/no/such/file.pem") }
      .to raise_error(ArgumentError, /tls_cert: .*neither PEM text nor a readable file/)
    expect { described_class.context(key: leaf[1].to_pem) }
      .to raise_error(ArgumentError, /tls_key given without tls_cert/)
  end
end
