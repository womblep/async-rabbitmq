require_relative "lib/async_rabbitmq/version"

Gem::Specification.new do |spec|
  spec.name          = "async-rabbitmq"
  spec.version       = AsyncRabbitMQ::VERSION
  spec.authors       = ["Russell"]
  spec.summary       = "Fiber-native Ruby AMQP 0-9-1 client built on the Async ecosystem"
  spec.description   = "An async-io based RabbitMQ client using Fibers instead of threads. " \
                       "Channels are duck-typed streams. Connection recovery is built-in."
  spec.homepage      = "https://github.com/russellthedev/async-rabbitmq"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.3"

  spec.files         = Dir["lib/**/*.rb", "LICENSE", "README.md"]
  spec.require_paths = ["lib"]

  spec.add_dependency "async",        "~> 2.0"
  spec.add_dependency "amq-protocol", "~> 2.3"

  spec.add_development_dependency "rspec",      "~> 3.13"
  spec.add_development_dependency "simplecov",  "~> 0.22"
  spec.add_development_dependency "async-pool", "~> 0.11"
  spec.add_development_dependency "toxiproxy",  "~> 2.0"
end
