require "spec_helper"

# Step 20: SASL mechanism negotiation.
RSpec.describe "SASL mechanism negotiation", :integration do

  it "connects with explicit auth_mechanism: PLAIN" do
    isolated_session(auth_mechanism: "PLAIN") do |session, _|
      expect(session).to be_open
      ch = session.open_channel
      q  = ch.queue("test.sasl.plain", durable: false)
      ch.basic_publish("sasl-plain-test", routing_key: q.name)
      _, _h, body = ch.basic_get(q.name)
      expect(body).to eq("sasl-plain-test")
      ch.close
    end
  end

  it "raises AuthenticationError when requesting unsupported mechanism" do
    # Connect directly (not via isolated_session) since we expect failure during handshake.
    session = AsyncRabbitMQ::Session.new(
      host: RABBITMQ_HOST,
      port: RABBITMQ_PORT,
      auth_mechanism: "GSSAPI"
    )
    expect { session.connect }.to raise_error(AsyncRabbitMQ::AuthenticationError, /GSSAPI not offered/)
  end
end
