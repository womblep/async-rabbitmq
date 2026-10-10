require "spec_helper"

# What happens to a delivery whose handler raised. The default dead-letters it
# after one attempt; :retry hands it back so the broker can count the failures
# and retire it at x-delivery-limit; :leave settles nothing.
#
# The verb matters as much as the requeue flag. RabbitMQ only counts a delivery
# as failed on basic.reject -- after basic.nack the x-delivery-count does not
# move, so a nacked message is redelivered forever and never reaches the limit.
RSpec.describe "consumer error disposition", :integration do

  def wait_until(timeout = 10)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not met within #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      sleep 0.1
    end
  end

  # A quorum queue that retires a message after +limit+ failed deliveries and
  # dead-letters it to +park+.
  def retry_queue(ch, suffix, limit:)
    main, park = "disp.main.#{suffix}", "disp.park.#{suffix}"
    ch.quorum_queue(park)
    ch.quorum_queue(main, arguments: {
      "x-delivery-limit"          => limit,
      "x-delayed-retry-type"      => "disabled",
      "x-dead-letter-exchange"    => "",
      "x-dead-letter-routing-key" => park,
    })
    [main, park]
  end

  describe "the default" do
    it "dead-letters after a single attempt rather than retrying" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, park = retry_queue(ch, SecureRandom.hex(4), limit: 5)

        attempts = 0
        ch.basic_consume(main, manual_ack: true) { |_di, _h, _b| attempts += 1; raise "always fails" }
        ch.basic_publish("poison", routing_key: main)

        wait_until { attempts >= 1 }
        sleep 1.5                      # a retry would show up as more attempts

        expect(attempts).to eq(1)
        got = ch.basic_get(park, manual_ack: false)
        expect(got).not_to be_nil
        expect(got[2]).to eq("poison")
        ch.close
      end
    end
  end

  describe ":retry" do
    it "hands the message back so the broker counts the failures and retires it" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, park = retry_queue(ch, SecureRandom.hex(4), limit: 3)

        counts = []
        ch.basic_consume(main, manual_ack: true, on_error: :retry) do |_di, header, _b|
          counts << (header.properties[:headers] || {})["x-delivery-count"]
          raise "fails every time"
        end
        ch.basic_publish("poison", routing_key: main)

        # limit 3 means three further deliveries after the first.
        wait_until(20) { counts.size >= 4 }
        sleep 1.5

        # The broker counted each failure, which is what basic.nack would not do.
        expect(counts.first).to be_nil
        expect(counts[1..3]).to eq([1, 2, 3])
        expect(counts.size).to eq(4)   # retired, not redelivered forever

        got = ch.basic_get(park, manual_ack: false)
        expect(got).not_to be_nil
        expect(got[2]).to eq("poison")
        ch.close
      end
    end
  end

  describe ":release" do
    it "hands the message back without counting it against the message" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, park = retry_queue(ch, SecureRandom.hex(4), limit: 2)

        counts = []
        ch.basic_consume(main, manual_ack: true, on_error: :release) do |_di, header, _b|
          counts << (header.properties[:headers] || {})["x-delivery-count"]
          raise "the gateway is down, not this message"
        end
        ch.basic_publish("innocent", routing_key: main)

        # Well past a limit of 2: under :retry this would have been retired by
        # now. Released deliveries never advance the count, so it keeps coming.
        wait_until(20) { counts.size >= 5 }
        expect(counts.uniq).to eq([nil])
        expect(ch.basic_get(park, manual_ack: false)).to be_nil   # not dead-lettered

        ch.close
      end
    end

    it "differs from :retry only in whether the failure is counted" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, _park = retry_queue(ch, SecureRandom.hex(4), limit: 2)

        seen = []
        ch.basic_consume(main, manual_ack: true, on_error: :retry) do |_di, header, _b|
          seen << (header.properties[:headers] || {})["x-delivery-count"]
          raise "boom"
        end
        ch.basic_publish("poison", routing_key: main)

        # Same queue, same limit, same handler: rejecting retires it.
        wait_until(20) { seen.size >= 3 }
        sleep 1.5
        expect(seen).to eq([nil, 1, 2])
        ch.close
      end
    end
  end

  describe ":leave" do
    it "settles nothing, so the broker still holds the delivery" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        qname = "disp.leave.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)

        seen = 0
        ch.basic_consume(qname, manual_ack: true, on_error: :leave) { |_di, _h, _b| seen += 1; raise "boom" }
        ch.basic_publish("x", routing_key: qname)
        wait_until { seen >= 1 }
        sleep 1.0
        expect(seen).to eq(1)          # not rejected, so not redelivered

        # Asked of the broker rather than of its statistics, which lag: an
        # unacknowledged delivery is requeued when its channel goes away.
        ch.close
        sleep 0.5
        other = session.open_channel
        got = other.basic_get(qname, manual_ack: false)
        expect(got).not_to be_nil
        expect(got[2]).to eq("x")
        other.close
      end
    end
  end

  describe "the on_handler_error hook" do
    it "decides the disposition from what it returns, overriding the default" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, _park = retry_queue(ch, SecureRandom.hex(4), limit: 3)

        ch.on_handler_error { |_e, _m, _q| :retry }   # channel default is :dead_letter
        attempts = 0
        ch.basic_consume(main, manual_ack: true) { |_di, _h, _b| attempts += 1; raise "boom" }
        ch.basic_publish("poison", routing_key: main)

        wait_until(20) { attempts >= 4 }
        expect(attempts).to eq(4)      # retried to the limit, not dead-lettered at once
        ch.close
      end
    end

    it "runs before the delivery is settled" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        qname = "disp.order.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)

        settled_when_hook_ran = nil
        ch.on_handler_error do |_e, method, _q|
          settled_when_hook_ran = ch.send(:unsettled?, method.delivery_tag)
          nil
        end
        ch.basic_consume(qname, manual_ack: true) { |_di, _h, _b| raise "boom" }
        ch.basic_publish("x", routing_key: qname)
        wait_until { !settled_when_hook_ran.nil? }

        # Still unsettled while the hook runs, which is what lets it choose.
        expect(settled_when_hook_ran).to be true
        ch.close
      end
    end

    it "falls back to the default when it returns something meaningless" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, park = retry_queue(ch, SecureRandom.hex(4), limit: 5)

        ch.on_handler_error { |_e, _m, _q| "yes please" }
        attempts = 0
        ch.basic_consume(main, manual_ack: true) { |_di, _h, _b| attempts += 1; raise "boom" }
        ch.basic_publish("poison", routing_key: main)
        wait_until { attempts >= 1 }
        sleep 1.5

        expect(attempts).to eq(1)
        expect(ch.basic_get(park, manual_ack: false)).not_to be_nil
        ch.close
      end
    end

    it "does not lose the delivery when the hook itself raises" do
      isolated_session do |session, _|
        ch = session.open_channel
        ch.basic_qos(prefetch_count: 5)
        main, park = retry_queue(ch, SecureRandom.hex(4), limit: 5)

        ch.on_handler_error { |_e, _m, _q| raise "the hook is broken too" }
        ch.basic_consume(main, manual_ack: true) { |_di, _h, _b| raise "boom" }
        ch.basic_publish("poison", routing_key: main)

        # Still dead-lettered by the default, rather than left hanging.
        wait_until(15) { ch.basic_get(park, manual_ack: false) }
        expect(ch.open?).to be true
        ch.close
      end
    end
  end

  describe "configuration" do
    it "takes a channel-wide default that a consumer can override" do
      isolated_session do |session, _|
        ch = session.open_channel
        expect(ch.on_error_disposition).to eq(:dead_letter)
        ch.on_error_disposition = :leave
        expect(ch.on_error_disposition).to eq(:leave)
        ch.close
      end
    end

    it "refuses a disposition it does not know, at the point of the mistake" do
      isolated_session do |session, _|
        ch = session.open_channel
        qname = "disp.bad.#{SecureRandom.hex(4)}"
        ch.queue(qname, durable: true)

        expect { ch.on_error_disposition = :requeue }
          .to raise_error(ArgumentError, /dead_letter/)
        expect { ch.basic_consume(qname, on_error: :requeue) { |_d, _h, _b| } }
          .to raise_error(ArgumentError, /dead_letter/)
        ch.close
      end
    end
  end
end
