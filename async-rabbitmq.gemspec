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

  spec.files         = Dir["lib/**/*.rb", "LICENSE", "README.md", "CHANGELOG.md"]
  spec.metadata      = {
    "changelog_uri"   => "https://github.com/womblep/async-rabbitmq/blob/main/CHANGELOG.md",
    "source_code_uri" => "https://github.com/womblep/async-rabbitmq",
  }
  spec.require_paths = ["lib"]

  spec.add_dependency "async",        "~> 2.0"
  spec.add_dependency "amq-protocol", "~> 2.9"

  spec.add_development_dependency "rspec",      "~> 3.13"
  spec.add_development_dependency "simplecov",  "~> 0.22"
  spec.add_development_dependency "async-pool", "~> 0.11"
  spec.add_development_dependency "toxiproxy",  "~> 2.0"
  spec.add_development_dependency "opentelemetry-api", "~> 1.1"
  spec.add_development_dependency "opentelemetry-sdk", "~> 1.1"
end
