require_relative "lib/async_rabbitmq/version"

Gem::Specification.new do |spec|
  spec.name          = "async-rabbitmq"
  spec.version       = AsyncRabbitMQ::VERSION
  spec.authors       = ["Russell Penney"]
  spec.summary       = "Fiber-native Ruby AMQP 0-9-1 client built on the Async ecosystem"
  spec.description   = "A RabbitMQ client for the async fiber scheduler: no threads, " \
                       "channels are duck-typed streams, connection and topology recovery are built in."
  spec.homepage      = "https://github.com/womblep/async-rabbitmq"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.4"

  spec.files         = Dir["lib/**/*.rb", "exe/*", "LICENSE", "README.md", "CHANGELOG.md"]
  spec.bindir        = "exe"
  spec.executables   = ["async-rabbitmq"]
  spec.metadata      = {
    # No homepage_uri: it would duplicate spec.homepage, and RubyGems only
    # shows the first key for a repeated URL.
    "source_code_uri"       => "https://github.com/womblep/async-rabbitmq",
    "changelog_uri"         => "https://github.com/womblep/async-rabbitmq/blob/main/CHANGELOG.md",
    "bug_tracker_uri"       => "https://github.com/womblep/async-rabbitmq/issues",
    "documentation_uri"     => "https://github.com/womblep/async-rabbitmq/blob/main/README.md",
    # Require MFA to publish or yank a release of this gem.
    "rubygems_mfa_required" => "true",
  }
  spec.require_paths = ["lib"]

  # 2.29 is where Async::Promise appears (the one-shot latch behind every
  # reply/recovery wait) and where Queue/Condition became thread-safe; 2.24
  # introduced Queue#close, which FrameIO#stop relies on.
  spec.add_dependency "async",        "~> 2.29"
  spec.add_dependency "amq-protocol", "~> 2.9"
  # The default logger (see AsyncRabbitMQ::Logging). async depends on it
  # already; declared because this gem calls it directly.
  spec.add_dependency "console",      "~> 1.29"
  # Connection establishment (TCP, TLS, address resolution) - the same layer
  # async-http and the rest of the socketry stack build on.
  spec.add_dependency "io-endpoint",  "~> 0.19"
  # Buffered reads and writes over the socket, for the 1.0 connection layer.
  spec.add_dependency "io-stream",    "~> 0.11"

  spec.add_development_dependency "rspec",      "~> 3.13"
  spec.add_development_dependency "simplecov",  "~> 0.22"
  spec.add_development_dependency "async-pool", "~> 0.11"
  spec.add_development_dependency "toxiproxy",  "~> 2.0"
  spec.add_development_dependency "opentelemetry-api", "~> 1.1"
  spec.add_development_dependency "opentelemetry-sdk", "~> 1.1"
  spec.add_development_dependency "opentelemetry-metrics-api", "~> 0.6"
  spec.add_development_dependency "opentelemetry-metrics-sdk", "~> 0.15"
end
