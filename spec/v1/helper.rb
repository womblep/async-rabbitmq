# frozen_string_literal: true

require "spec_helper"
require "async/rabbitmq"

# Helpers for the specs of the 1.0 client (Async::RabbitMQ). They sit beside
# the 0.x helpers in spec_helper.rb until the old client is removed.
module V1Helpers
  # A fresh virtual host for one example, so nothing declared in it can meet
  # anything from another.
  def with_vhost
    vhost = "v1-#{SecureRandom.hex(6)}"
    create_vhost(vhost)
    yield vhost
  ensure
    delete_vhost(vhost) rescue nil
  end

  def endpoint(vhost: "/", port: RABBITMQ_PORT, **options)
    Async::RabbitMQ::Endpoint.new(RABBITMQ_HOST, port, vhost: vhost, **options)
  end

  # An open Connection in a virtual host of its own.
  def with_connection(**options)
    with_vhost do |vhost|
      connection = Async::RabbitMQ::Connection.open(endpoint(vhost: vhost, **options))
      begin
        yield connection, vhost
      ensure
        connection.close
      end
    end
  end

  def wait_for(timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.02
    end
  end

  # Cut the socket under a connection, as a network failure would.
  def sever!(connection)
    io = connection.instance_variable_get(:@framer).stream.io
    io = io.to_io if io.respond_to?(:to_io)
    io.shutdown(Socket::SHUT_RDWR)
  rescue IOError, SystemCallError
    # already gone
  end

  # Records what a channel hands its delegate.
  class RecordingDelegate
    include Async::RabbitMQ::Channel::Delegate

    attr_reader :events

    def initialize
      @events = []
    end

    def deliver(method, header, body) = @events << [:deliver, method, header, body]
    def returned(method, header, body) = @events << [:returned, method, header, body]
    def confirmed(method) = @events << [:confirmed, method]
    def cancelled(method) = @events << [:cancelled, method]
    def closed(error) = @events << [:closed, error]

    def of(kind) = @events.select { |event| event.first == kind }
  end
end

RSpec.configure do |config|
  config.include V1Helpers, :integration
end
