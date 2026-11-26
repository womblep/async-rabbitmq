# frozen_string_literal: true

require "async/rabbitmq/version"
require "async_rabbitmq/version"

# The gem holds two clients, each with a version constant of its own so that
# neither has to load the other. They are one gem and must say the same.
RSpec.describe "the gem's version" do
  it "is the same in both clients, and is what the gemspec releases" do
    expect(Async::RabbitMQ::VERSION).to eq(AsyncRabbitMQ::VERSION)
    gemspec = Gem::Specification.load(File.expand_path("../../async-rabbitmq.gemspec", __dir__))
    expect(gemspec.version.to_s).to eq(Async::RabbitMQ::VERSION)
  end
end
