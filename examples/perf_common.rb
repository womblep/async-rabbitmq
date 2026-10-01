# frozen_string_literal: true

# Shared plumbing for the throughput/integrity pair:
#
#   examples/perf_publisher.rb   sender
#   examples/perf_consumer.rb    receiver and verifier
#
# The pair exists to catch integrity bugs, not only to measure a rate. Every
# message states its own identity twice, in the AMQP properties and inside the
# body, and the body is fully determined by (run, stream, seq, size). The
# receiver rebuilds the bytes it should have been given and compares them, so
# each of these shows up as a counted, named violation rather than a silent
# pass:
#
#   * a body paired with another message's header ("crossed content")
#   * a body spliced, truncated or interleaved with another publisher's frames
#   * a delivery on the wrong queue or to the wrong consumer ("crossed route")
#   * a message lost, doubled, or delivered out of sequence
#   * a leftover message from an earlier run
#
# Ordering is only asserted where AMQP actually promises it: within one
# publisher stream, on one queue, read by one consumer with one handler. The
# receiver states in its report whether that held for the run it just did.

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "logger"

# The frame writer uses IO::Buffer, still flagged experimental in Ruby 4.0.
Warning[:experimental] = false if Warning.respond_to?(:[]=)

module Perf
  EXCHANGE = "perf.x"

  # Run queues clean themselves up when idle. RabbitMQ 4.2+ refuses a queue that
  # is both non-durable and non-exclusive, so these are durable with an expiry
  # rather than transient (see the README).
  QUEUE_TTL_MS = 300_000

  # Smallest body that still fits the identity prefix and the terminator.
  MIN_SIZE = 96

  BODY_PREFIX = "PERF"
  BODY_SUFFIX = "|END"

  module_function

  def queue_name(run, stream)
    "perf.#{run}.#{stream}"
  end

  def routing_key(run, stream)
    "perf.#{run}.#{stream}"
  end

  def monotonic_ns
    Process.clock_gettime(Process::CLOCK_MONOTONIC, :nanosecond)
  end

  # Deterministic body for one message: given the same four values every
  # process builds the same bytes. The filler varies per sequence number, so
  # two messages of one stream spliced together cannot look intact.
  def body(run:, stream:, seq:, size:)
    size = MIN_SIZE if size < MIN_SIZE
    head = "#{BODY_PREFIX}|#{run}|#{stream}|#{seq}|#{size}|"
    room = [size - head.bytesize - BODY_SUFFIX.bytesize, 0].max
    seed = format("%08x", ((stream + 1) * 2_654_435_761 + seq) & 0xffffffff)
    filler = (seed * ((room / seed.bytesize) + 1))[0, room]
    "#{head}#{filler}#{BODY_SUFFIX}".b
  end

  # Identity carried in the AMQP properties, alongside the copy in the body.
  # +batch+ is the number of messages sharing this header when publishing with
  # basic_publish_batch: the properties are shared by the whole batch, so the
  # per-message sequence number then lives in the body only.
  def properties(run:, stream:, seq:, size:, persistent:, batch: nil, eos: nil)
    headers = {
      "run" => run.to_s,
      "stream" => stream,
      "seq" => seq,
      "sz" => size,
      "t" => monotonic_ns
    }
    headers["batch"] = batch if batch
    headers["eos"] = eos if eos
    {
      message_id: "#{run}:#{stream}:#{seq}",
      correlation_id: "#{run}:#{stream}:#{seq}",
      content_type: "application/octet-stream",
      app_id: "perf-publisher",
      persistent: persistent,
      headers: headers
    }
  end

  # Parsed view of one delivery, plus every rule it broke.
  Message = Struct.new(:run, :stream, :seq, :size, :sent_ns, :eos, :problems, keyword_init: true) do
    def ok?
      problems.empty?
    end
  end

  # Check one delivery against the contract above. +expected_stream+ is the
  # stream this queue is supposed to carry, so a message arriving on another
  # stream's queue is reported instead of silently accepted.
  def verify(header, body_bytes, expected_run:, expected_stream:)
    problems = []
    props = header.respond_to?(:properties) ? (header.properties || {}) : {}
    h = props[:headers] || {}
    raw = body_bytes.to_s.b

    fields = raw.split("|", 6)
    unless fields.size >= 5 && fields[0] == BODY_PREFIX
      return Message.new(run: nil, stream: nil, seq: nil, size: raw.bytesize,
                         sent_ns: h["t"], eos: nil, problems: ["unparsable body"])
    end

    b_run = fields[1]
    b_stream = fields[2].to_i
    b_seq = fields[3].to_i
    b_size = fields[4].to_i

    expected_bytes = body(run: b_run, stream: b_stream, seq: b_seq, size: b_size)
    if expected_bytes != raw
      problems << if expected_bytes.bytesize != raw.bytesize
                    "body is #{raw.bytesize} bytes, should be #{expected_bytes.bytesize} (truncated or spliced)"
                  else
                    "body bytes differ from the message it claims to be (corrupt or interleaved)"
                  end
    end

    problems << "run #{b_run.inspect}, expected #{expected_run.to_s.inspect} (message from another run)" if b_run != expected_run.to_s

    if expected_stream && b_stream != expected_stream
      problems << "stream #{b_stream} arrived on the queue for stream #{expected_stream} (crossed route)"
    end

    if h.empty?
      problems << "no application headers"
    else
      problems << "header stream #{h['stream']}, body stream #{b_stream} (crossed content)" if h["stream"] != b_stream
      problems << "header size #{h['sz']}, body size #{b_size}" if h["sz"] != b_size

      if h["batch"]
        base = h["seq"].to_i
        last = base + h["batch"].to_i - 1
        problems << "body seq #{b_seq} outside its batch #{base}..#{last}" unless b_seq.between?(base, last)
      else
        problems << "header seq #{h['seq']}, body seq #{b_seq} (crossed content)" if h["seq"] != b_seq
        expected_id = "#{b_run}:#{b_stream}:#{b_seq}"
        if props[:message_id] && props[:message_id] != expected_id
          problems << "message_id #{props[:message_id].inspect}, body says #{expected_id.inspect} (crossed properties)"
        end
        if props[:correlation_id] && props[:correlation_id] != props[:message_id]
          problems << "correlation_id #{props[:correlation_id].inspect} does not match message_id #{props[:message_id].inspect}"
        end
      end
    end

    Message.new(run: b_run, stream: b_stream, seq: b_seq, size: b_size,
                sent_ns: h["t"], eos: h["eos"], problems: problems)
  end

  # Sparse set of seen sequence numbers, one bit each: a few million messages
  # cost a few hundred KB instead of a Set of boxed Integers.
  class Bitset
    def initialize
      @bytes = +"".b
    end

    def set?(index)
      byte = @bytes.getbyte(index >> 3)
      !byte.nil? && (byte & (1 << (index & 7))) != 0
    end

    def set(index)
      slot = index >> 3
      @bytes << ("\0".b * (slot + 1 - @bytes.bytesize)) if @bytes.bytesize <= slot
      @bytes.setbyte(slot, @bytes.getbyte(slot) | (1 << (index & 7)))
    end
  end

  # Per-stream sequence bookkeeping: what arrived, what came back twice, what
  # arrived after a later message, and what never turned up.
  class StreamTracker
    attr_reader :stream, :received, :bytes, :duplicates, :out_of_order, :high_water, :total

    def initialize(stream)
      @stream = stream
      @seen = Bitset.new
      @received = 0
      @bytes = 0
      @duplicates = 0
      @out_of_order = 0
      @high_water = nil
      @total = nil # set when the end-of-stream marker arrives
    end

    def observe(seq, bytes)
      if @seen.set?(seq)
        @duplicates += 1
        return :duplicate
      end

      @seen.set(seq)
      @received += 1
      @bytes += bytes
      if @high_water.nil? || seq > @high_water
        @high_water = seq
        :ok
      else
        @out_of_order += 1
        :out_of_order
      end
    end

    def end_of_stream(total)
      @total = total
    end

    def complete?
      !@total.nil? && @received >= @total
    end

    def ceiling
      @total || (@high_water ? @high_water + 1 : 0)
    end

    def missing(limit: 10)
      found = []
      (0...ceiling).each do |i|
        next if @seen.set?(i)

        found << i
        break if found.size >= limit
      end
      found
    end

    def missing_count
      ceiling - @received
    end
  end

  # Latency samples in nanoseconds; percentiles computed on demand.
  # Five numbers are reported from however many samples arrive, so the samples
  # are not all kept. A soak takes millions: at one Integer slot each that was
  # ~50 MB over a three-hour run, which in a tool whose job is to find leaks
  # reads exactly like one. Count, min and max stay exact; the percentiles come
  # from a bounded uniform sample of the stream.
  class Latencies
    # 500k samples is ~4 MB, flat however long the run is, and keeps every
    # percentile within about 1% of the exact value even where the tail is
    # sparse. Shorter runs never reach it, so they stay exact.
    CAPACITY = 500_000

    def initialize(capacity: CAPACITY)
      @capacity = capacity
      @values   = []
      @count    = 0
      @min      = nil
      @max      = nil
    end

    def <<(nanoseconds)
      @count += 1
      @min = nanoseconds if @min.nil? || nanoseconds < @min
      @max = nanoseconds if @max.nil? || nanoseconds > @max

      if @values.size < @capacity
        @values << nanoseconds
      else
        # Vitter's algorithm R: every sample seen so far has the same chance of
        # being one of the ones kept, so the percentiles stay unbiased however
        # long the run is.
        slot = Kernel.rand(@count)
        @values[slot] = nanoseconds if slot < @capacity
      end
    end

    def empty?
      @count.zero?
    end

    def summary_ms
      return nil if @count.zero?

      sorted = @values.sort
      at = ->(quantile) { sorted[[(quantile * sorted.size).ceil - 1, 0].max] / 1e6 }
      { count: @count, sampled: sorted.size, min: @min / 1e6, p50: at.call(0.50),
        p90: at.call(0.90), p99: at.call(0.99), max: @max / 1e6 }
    end
  end

  def rate(count, seconds)
    seconds.positive? ? count / seconds : 0.0
  end

  def fmt_rate(count, seconds)
    format("%.0f msg/s", rate(count, seconds))
  end

  def fmt_throughput(bytes, seconds)
    format("%.1f MB/s", seconds.positive? ? bytes / seconds / 1_048_576.0 : 0.0)
  end

  def rule(title)
    "#{title} #{'-' * [72 - title.length, 0].max}"
  end

  # Quiet by default: a run prints its own report, not the client's debug log.
  def logger(verbose)
    log = Logger.new($stdout)
    log.level = verbose ? Logger::DEBUG : Logger::WARN
    log.formatter = ->(severity, _time, _progname, msg) { "  [#{severity.downcase}] #{msg}\n" }
    log
  end
end
