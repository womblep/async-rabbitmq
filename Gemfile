source "https://rubygems.org"

gemspec

gem "logger"   # explicit dep — will leave stdlib in Ruby 4.0
gem "fiddle"   # win32/registry.rb uses fiddle; explicit dep silences Ruby 4.0 warning

group :test do
  gem "rspec",           "~> 3.13"
  gem "simplecov",       "~> 0.22"
  gem "async-pool",      "~> 0.11"
  gem "async-rspec",     "~> 1.17"
  gem "toxiproxy",       "~> 2.0"
  # For spec/integration/47_open_telemetry_spec.rb; the telemetry layer itself is
  # optional and not a runtime dependency.
  gem "opentelemetry-api", "~> 1.1"
  gem "opentelemetry-sdk", "~> 1.1"
end
