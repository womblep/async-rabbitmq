# async-rabbitmq

A fiber-native RabbitMQ (AMQP 0-9-1) client for Ruby, built on the
[async](https://github.com/socketry/async) ecosystem and
[amq-protocol](https://github.com/ruby-amqp/amq-protocol). No threads: the
reader, writer, heartbeat and every consumer handler are fibers, so it fits
Falcon, async-http and anything else running under the fiber scheduler.

Requires Ruby 3.4+ (CI runs 3.4 and 4.0) and RabbitMQ 3.13+ (tested against 4.x).

```ruby
require "async"
require "async_rabbitmq"

Sync do
  session = AsyncRabbitMQ::Session.from_uri("amqp://guest:guest@localhost/%2F")
  session.connect

  channel  = session.open_channel
  exchange = channel.topic("events", durable: true)
  queue    = channel.durable_queue("events.audit")
  queue.bind(exchange: exchange.name, routing_key: "audit.#")

  exchange.publish("user 42 logged in", routing_key: "audit.login", persistent: true)

  queue.subscribe(manual_ack: true) do |delivery, header, body|
    puts "#{header.properties[:content_type]}: #{body}"
    channel.basic_ack(delivery.delivery_tag)
  end

  sleep 1
  session.close
end
```

## Connecting

```ruby
AsyncRabbitMQ::Session.new(
  host: "localhost", port: 5672, vhost: "/", username: "guest", password: "guest",
  hosts: ["rabbit1", "rabbit2"],            # or addresses: ["rabbit1:5672", "rabbit2:5673"]
  tls: false,                               # implied by any of the four below
  tls_cert: nil, tls_key: nil,              # client certificate and key: a path or PEM text
  tls_ca_certificates: nil,                 # CA path(s) or PEM text; default is the system store
  verify_peer: true, tls_min_version: :TLS1_2,
  tls_context: nil,                         # or an OpenSSL::SSL::SSLContext you built, for anything else
  heartbeat: 60, frame_max: 131_072, channel_max: 2047,
  connect_timeout: 30, rpc_timeout: 15,     # seconds; rpc_timeout: nil waits forever
  auth_mechanism: nil,                      # "PLAIN" / "EXTERNAL", negotiated by default
  connection_name: "orders-worker",
  auto_recover: true, recovery_attempts: nil, recovery_interval: 1.0, recovery_max_interval: 30.0,
  recover_topology: true, topology_recovery_filter: nil,
  instrumenter: nil,                        # ->(event_name, payload) { ... }
  notifier: nil,                            # or one Notifier shared between sessions
  logger: AsyncRabbitMQ::Log.new            # anything with debug/info/warn/error
)
```

`Session.from_uri` accepts one or more `amqp://` / `amqps://` URIs and the
standard query parameters (`heartbeat`, `connection_timeout`, `channel_max`,
`auth_mechanism`; on `amqps://` also `verify`, `cacertfile`, `certfile`,
`keyfile`). Several URIs form a failover list. Keyword arguments override the
URI.

`Session#update_secret(new_secret, reason)` rotates the credential on a live
connection (for refreshed OAuth 2 tokens); the new value is used for reconnects.
`Session#store_secret` records it without sending it, for a session that is
down while the secret rotates.

### TLS

The ordinary case needs no OpenSSL: give it the CA that signed the broker's
certificate, and a client certificate and key if the broker asks for one. Each
value is a file path or PEM text, so material from a secrets manager works as
is. A certificate value may carry the leaf followed by its chain, and a CA
value may hold several certificates.

```ruby
AsyncRabbitMQ::Session.new(host: "rabbit", port: 5671,
                           tls_ca_certificates: "/etc/ssl/rabbit-ca.pem")
AsyncRabbitMQ::Session.new(host: "rabbit", port: 5671,
                           tls_cert: ENV["RABBIT_CERT"], tls_key: ENV["RABBIT_KEY"],
                           tls_ca_certificates: [ENV["RABBIT_CA"]])
```

Peers and their hostnames are verified unless `verify_peer: false`, and TLS 1.2
is the floor unless `tls_min_version:` says otherwise. `amqps://` URIs take the
same things as `cacertfile`, `certfile`, `keyfile` and `verify`. For anything
this does not cover, pass `tls_context:` and it is used as given.

### Logging

`logger:` takes anything that responds to `debug`, `info`, `warn` and `error`,
so pass whatever your application already uses. The gem does not depend on the
`logger` gem, which stopped being a default gem in Ruby 4.0.

```ruby
AsyncRabbitMQ::Session.new(logger: Rails.logger)
AsyncRabbitMQ::Session.new(logger: Logger.new($stdout, level: Logger::INFO))
AsyncRabbitMQ::Session.new(logger: AsyncRabbitMQ::Log.silent)          # say nothing
AsyncRabbitMQ::Session.new(logger: AsyncRabbitMQ::Log.new($stdout, level: :debug))
```

The default is `AsyncRabbitMQ::Log`, which writes warnings and errors, one line
each, to `$stderr`. Structured events (see below) are the better hook for
metrics and tracing; the log is for the things a human should read.

## Channels

Everything synchronous (`queue`, `exchange`, `basic_consume`, ...) is a
request/reply that blocks only the calling fiber and gives up after
`rpc_timeout` with `RpcTimeoutError`. A channel may be shared by many fibers:
requests are serialised per channel and a publish's frames always reach the
wire contiguously.

```ruby
ch = session.open_channel(pool_size: 4)   # up to 4 consumer handlers at once on this channel
session.with_channel { |ch| ... }         # closed afterwards

ch.queue("name", durable: true, exclusive: false, auto_delete: false, arguments: {})
ch.durable_queue("name")                  # classic, durable, non-exclusive, non-auto-delete
ch.quorum_queue("name")                   # x-queue-type: quorum
ch.stream("name")                         # x-queue-type: stream
ch.temporary_queue                        # server-named, exclusive, auto-delete
ch.queue("name", passive: true)           # assert it exists (404 ChannelError otherwise)

ch.exchange("name", type: :topic, durable: true)   # or ch.direct / fanout / topic / headers
ch.default_exchange

ch.basic_publish(body, exchange: "", routing_key: "q", persistent: true, content_type: "text/plain",
                 headers: {}, correlation_id: "...", reply_to: "...", expiration: "60000", ...)
ch.basic_publish_batch([b1, b2, b3], routing_key: "q")   # one write, best throughput

ch.basic_get("q")                          # => [delivery_info, header, body] or nil; manual ack by default
ch.basic_get("q", manual_ack: false)
ch.basic_ack(tag) / basic_nack(tag, requeue: true) / basic_reject(tag, requeue: true)
ch.basic_qos(prefetch_count: 10)           # also sizes the handler pool
tag = ch.basic_consume("q", manual_ack: true) { |delivery, header, body| ... }
ch.basic_cancel(tag)
ch.each("q") { |delivery, header, body| ... }   # blocks until the consumer or channel goes away
```

Consumer handlers run in their own fibers, at most `pool_size` at a time per
channel. Set `basic_qos` before `basic_consume`: without a prefetch limit the
broker sends the whole queue as fast as it can and one fiber is created per
delivery, so memory tracks queue depth rather than concurrency. The client
warns once per channel if you don't.

### Acknowledging, and delivery tags across a reconnect

`delivery.delivery_tag` is a `VersionedDeliveryTag`: the broker's tag plus the
generation of the channel it arrived on. It converts (`to_int`), compares,
sorts, hashes and prints as the integer it wraps, so passing it to
`basic_ack`, comparing it or putting it in a collection all work unchanged.
Call `.to_i` if you need a real `Integer` — to serialise it, or where something
tests `is_a?(Integer)`.

The generation is what makes an ack safe across a reconnect. Delivery tags are
scoped to a channel on a connection, and the broker restarts numbering at 1
when a channel is reopened, so a handler that was still running when the
connection dropped would otherwise acknowledge whichever message now holds that
number:

```ruby
ch.basic_qos(prefetch_count: 10)
ch.basic_consume("q", manual_ack: true) do |delivery, _header, body|
  handle(body)                              # may outlive the connection
  ch.basic_ack(delivery.delivery_tag)       # => false if the connection went; nothing is sent
end
```

`basic_ack`, `basic_nack` and `basic_reject` return `true` when the frame was
sent and `false` when the tag belonged to an earlier generation and was
dropped. A dropped ack is not a lost message: the broker requeued it when the
channel went, and it is redelivered on the new connection.

When a handler raises, the delivery is nacked with `requeue: false` — so a
message that always fails dead-letters instead of looping — and
`on_handler_error` is called:

```ruby
ch.on_handler_error do |error, delivery, queue_name|
  Sentry.capture_exception(error, extra: { queue: queue_name })
end
```

### RabbitMQ 4.2+ and transient queues

RabbitMQ 4.2 and later refuse to declare a queue that is both non-durable
and non-exclusive; the broker answers with a **connection-level** error
(`541 INTERNAL_ERROR`, "Feature transient_nonexcl_queues is deprecated")
and this client reconnects. The AMQP default (`durable: false,
exclusive: false`) is therefore rejected by a current broker. Use
`durable: true` (add `"x-expires"` to have idle queues clean themselves up),
or `exclusive: true` / `temporary_queue` for a queue that should live only as
long as the connection.

## Publisher confirms

```ruby
ch.confirm_select
ch.basic_publish(...)                      # => delivery tag
ch.wait_for_confirms                       # true, or false if the broker nacked something
ch.nacked_tags                             # tags rejected since confirm_select
ch.unconfirmed_tags

ch.confirm_select(tracking: true, outstanding_limit: 1000)
# publishes park while 1000 messages are unconfirmed (backpressure);
# wait_for_confirms raises AsyncRabbitMQ::MessageNacked on a nack.

ch.wait_for_confirms(timeout: 5)           # ConfirmTimeoutError instead of waiting forever
ch.unconfirmed_messages                    # what the broker has not resolved, with payload and routing
```

`wait_for_confirms` waits indefinitely by default. Pass `timeout:` if a broker
that accepts a publish and then never confirms it should raise
`ConfirmTimeoutError` (which carries `unconfirmed_tags`) rather than park the
fiber for good.

Messages published under confirms are kept until the broker acks them. If
the connection drops first they are re-published on the recovered channel
(the usual at-least-once trade-off: a message the broker had already
accepted may be delivered twice).

### What a returned publish means here

Two differences from Bunny on the sending side, both deliberate.

**`basic_publish` returns when the frames are queued for the writer fiber,
not when they are on the socket.** Bunny writes inline on the calling thread,
so there a returned publish means the bytes reached the kernel. Neither is a
delivery guarantee: RabbitMQ's own guidance is that a client which has written
frames to its socket still cannot assume the broker received or processed
them. Publisher confirms are the only thing that tells you. What the queue
does change is the size of the window. Closing a session with a backlog
discards whatever the writer has not reached yet, because the close handshake
is bounded at five seconds; the client logs a warning naming the number of
queued writes it dropped, since a publisher without confirms has no other way
to find out. If it matters, `confirm_select` and `wait_for_confirms` before
`close`.

**A channel may be published to from many fibers at once.** RabbitMQ's
documentation says concurrent publishing on a shared channel is not supported
by client libraries, and for most clients that is true. Here publishes and
request/reply calls are serialised per channel, so a message's frames always
reach the wire contiguously and confirm tags follow wire order. Sharing a
channel is still a throughput bottleneck, and synchronous calls queue behind a
publish backlog, so give a busy publisher its own channel when latency on
declares matters.

## Transactions

`ch.tx_select`, `ch.tx_commit`, `ch.tx_rollback`, `ch.using_tx?`. A channel
cannot be both transactional and in confirm mode.

## Connection recovery

When the connection is lost the session reconnects with exponential backoff
(`recovery_interval` doubling up to `recovery_max_interval`, ±25% jitter,
`recovery_attempts: nil` = forever), trying every address in the list.
Meanwhile each channel is in the *recovering* state: operations already
waiting for a reply raise `ConnectionError`, and new publishes and requests
**park** until the channel is reopened, so nothing issued during the outage
is lost. After reconnect the session

1. reopens every channel with its prefetch, confirm and tx settings,
2. replays the recorded topology (`recover_topology`): exchanges, queues
   (server-named queues come back under a new name, which is propagated to
   their bindings, consumers and `Queue` objects), then bindings,
3. re-registers consumers and releases the parked callers,
4. calls `on_recovery`.

Passive declares are not recorded; deleted or unbound entities are not
recovered. The registry follows what the broker does rather than which channel
declared what: a durable or exclusive queue outlives the channel that declared
it and is still recovered, while an auto-delete queue is forgotten, with its
bindings, the moment its last consumer goes, whether by `basic_cancel`, a
broker-side cancel or the channel closing, and an auto-delete exchange goes
when its last binding does. A worker that opens a channel and a temporary
queue per unit of work therefore leaves nothing behind for the next reconnect
to re-declare. `topology_recovery_filter:` takes an object implementing any of
`filter_exchanges`, `filter_queues`, `filter_queue_bindings`,
`filter_exchange_bindings`. If recovery is disabled, exhausted
(`on_recovery_exhausted`) or the broker refuses the credentials, channels are
closed and parked callers raise.

Callbacks: `on_connection_lost` (the connection dropped; fires before recovery
starts), `on_blocked` / `on_unblocked` (connection.blocked),
`on_recovery_attempt`, `on_recovery`, `on_recovery_exhausted`;
`Channel#on_return` (mandatory messages the broker could not route),
`on_cancel` (broker cancelled a consumer), `on_error` (broker closed the
channel). A broker-closed channel can be reopened in place with
`Channel#reopen`.

## Errors

All errors derive from `AsyncRabbitMQ::Error`: `ConnectionTimeoutError`
(could not connect), `AuthenticationError` (credentials or vhost refused,
`code` 403), `ConnectionError` (`code`, `text`; broker closed the
connection or it was lost), `ChannelError` (`code`, `text`, `channel_id`,
`close_method`, plus `delivery_ack_timeout?`, `unknown_delivery_tag?`,
`message_too_large?`), `NotOpenError`, `RpcTimeoutError`, `MessageNacked`
(`nacked_tags`), `ConfirmTimeoutError` (`unconfirmed_tags`, `channel_id`),
`ClusterError` (a cluster-wide operation that did not succeed on every node),
`HeartbeatTimeoutError`, `ChannelLimitError` (every id up to
the negotiated `channel_max` is in use; the broker would otherwise have closed
the connection). Channel ids are reused as channels close, so opening a
channel per unit of work is fine for the life of the connection.

## Events

`Session#on_event` subscribes to structured events for metrics, tracing and
debugging. Nothing is emitted until something subscribes: the payload is built
inside a block that only runs when a subscriber is listening.

```ruby
session.on_event { |name, payload| logger.info("#{name} #{payload}") }
session.on_event("message.")    { |name, _|  statsd.increment(name) }
session.on_event("channel.rpc") { |_, p|     histogram.record(p[:duration]) }
session.on_event(/^recovery\./) { |name, p|  pager.note(name, p) }

# or hand the whole stream to something at construction
AsyncRabbitMQ::Session.new(instrumenter: ->(name, payload) { ... })
```

The pattern is `nil` for everything, a String for one event name or, ending in
a dot, a prefix, or a Regexp. `on_event` returns a handle that
`session.notifier.unsubscribe(handle)` takes back. A subscriber that raises is
logged and skipped; it never breaks the connection.

| Event | Payload |
|---|---|
| `connection.open` | `host`, `port`, `vhost`, `tls`, `heartbeat`, `frame_max`, `channel_max`, `duration` |
| `connection.closed` | `reason` (`:user`), `host`, `port` |
| `connection.lost` | `host`, `port`, `error`, `message`, `recovering` |
| `connection.blocked` | `reason` |
| `connection.unblocked` | — |
| `recovery.attempt` | `attempt`, `delay` |
| `recovery.succeeded` | `attempts`, `host`, `port`, `channels`, `duration` |
| `recovery.exhausted` | `attempts`, `reason` (`:attempts_exceeded`, `:authentication_failed`) |
| `heartbeat.sent` | `interval` |
| `channel.open` | `channel` |
| `channel.closed` | `channel`, `reason` (`:user`, `:broker`, `:dropped`), `code`, `text` |
| `channel.rpc` | `channel`, `method` (`"queue.declare-ok"`), `duration` |
| `consumer.registered` | `channel`, `queue`, `consumer_tag`, `manual_ack` |
| `consumer.cancelled` | `channel`, `consumer_tag`, `queue`, `reason` (`:client`, `:broker`) |
| `message.published` | `channel`, `exchange`, `routing_key`, `count`, `bytes`, `delivery_tag` |
| `message.confirmed` | `channel`, `delivery_tag`, `multiple`, `acked` |
| `message.returned` | `channel`, `exchange`, `routing_key`, `code`, `text`, `bytes` |
| `message.consumed` | `channel`, `queue`, `consumer_tag`, `bytes`, `redelivered`, `duration` |

Durations are seconds as a Float. `bytes` on `message.published` is the encoded
frames, including the header and properties; elsewhere it is the body. A batch
published with `basic_publish_batch` is one event with `count` set and the last
delivery tag. Event names are API and do not change without a major version;
`AsyncRabbitMQ::Notifier::EVENTS` lists them.

### OpenTelemetry

Tracing is a separate, optional layer. Add `opentelemetry-api` to your bundle,
then:

```ruby
require "async_rabbitmq/telemetry/open_telemetry"
AsyncRabbitMQ::Telemetry::OpenTelemetry.install
```

Spans and attributes match `opentelemetry-instrumentation-bunny`, so a service
moving over from Bunny keeps the traces and dashboards it had. Publishing opens
a PRODUCER span `"<exchange>.<routing key> publish"` and injects the W3C trace
context into the message headers; `basic_get` opens a CONSUMER span
`"<destination> receive"`; and a consumer handler runs inside a CONSUMER span
`"<destination> process"` whose parent is the context extracted from the
headers, so one trace spans both sides of the broker. Attributes are
`messaging.system`, `messaging.destination`, `messaging.destination_kind`,
`messaging.protocol`, `messaging.protocol_version`,
`messaging.rabbitmq.routing_key`, `messaging.operation`, `net.peer.name` and
`net.peer.port`, plus `messaging.batch.message_count` on a batch publish.

`install` takes `tracer_provider:`, `tracer_name:` and `tracer_version:`.
`uninstall` stops tracing. One difference from Bunny: a pushed delivery here
goes straight to the handler, so there is one process span per delivery rather
than a receive span with a process span under it.

## Connection pool

Add `gem "async-pool"` to your Gemfile: it is not a dependency of this gem, so
that an application that never pools does not install it.

```ruby
require "async_rabbitmq/pool"
pool = AsyncRabbitMQ::Pool.new(max: 5, host: "localhost")
pool.acquire { |session| session.with_channel { |ch| ch.basic_publish(...) } }
pool.close
```

Backed by `Async::Pool`; a session is shared by up to `channel_max` fibers
before another connection is opened.

## One connection per cluster node

A `Session` is on one node. Every channel it opens is there, and so is every
exclusive queue declared through it, because the broker always places those on
the connecting node, whatever `queue_leader_locator` says. A process that holds
a channel and a temporary queue per client therefore puts all of its clients on
one node, and loses them together when that node goes.

`AsyncRabbitMQ::Cluster` takes the same options as `Session` and presents the
same methods, so it drops in where a `Session` was. It pins one connection to
each address, opens each new channel on the node with the fewest, and retries a
node that is down until it is back, after which new channels drift to it until
the counts are level. Nothing is ever moved: a channel stays on its node for
life.

```ruby
cluster = AsyncRabbitMQ::Cluster.new(
  addresses: %w[rabbit1:5672 rabbit2:5672 rabbit3:5672],   # one connection each
  username: "app", password: secret,
  on_node_down: :drop,            # :park (default), :drop, or ->(session, channels, error) { ... }
  clear_topology_on_drop: true    # with :drop, forget the lost connection's transient topology
)
cluster.connect                   # every node, concurrently; the ones that are down are retried in the background
channel = cluster.open_channel    # on the node with the fewest channels
cluster.on_node_down { |session, channels, error| ... }
# declare a fourth parameter to also receive the publishes the broker never
# confirmed, so they can be sent again on another node:
cluster.on_node_down do |session, channels, error, unconfirmed|
  unconfirmed.each { |m| elsewhere.basic_publish(m.payload, exchange: m.exchange, routing_key: m.routing_key) }
end
cluster.on_node_up   { |session| ... }
```

`on_node_down:` decides what happens to the channels on a node whose connection
is lost. `:park` is what a `Session` does: they wait, and resume on the same
node when it returns. `:drop` closes them at once: calls in flight raise
`ConnectionError`, consumer loops return, later calls raise `NotOpenError`, and
the fibers using them can open a new channel, which lands on a node that is up.
A callable receives the node's session, its channels and the error while the
channels are parked, and closes the ones it wants dropped; the rest wait. With
`:drop` the lost connection's transient topology is forgotten as well
(`clear_topology_on_drop: true`): exclusive, auto-delete and server-named
queues, auto-delete exchanges, and the bindings that referred to them. Durable
exchanges, queues and bindings are kept, so a node that comes back from an
empty data directory still has them re-declared; `false` keeps the usual
registry rules. Publishing and shared durable
topology are expected to live on a plain `Session` alongside: the cluster is
for the mass of per-client channels.

`connect` connects every node concurrently and returns once each one is either
connected or has failed a first attempt, so the channels opened next are spread
over every node that is reachable. The wait is bounded by one `connect_timeout`
however many nodes are down. It raises only if none can be reached, or at once
with `AuthenticationError` if a node refuses the credentials. Nodes that are
down at startup, or later, are retried with the recovery backoff.
`open?` is true while any node is; `close` closes them all. `open_channel`,
`with_channel`, `queue_exists?` and `exchange_exists?` use the least loaded
node; `update_secret` stores the new secret on every node first and then updates each
one that is up, so a node that refuses it cannot leave the nodes after it on
the old credential; failures are raised together as `ClusterError`.
The `on_*` callbacks fire for every node with that node's `Session`, `on_event`
sees every node's events (each carries `host` and `port`), `topology` is a
read-only view over every node's registry, and `host` and `port` are the first
address. `sessions`, `on_node_down` and `on_node_up` are the additions.

## Command line

Installing the gem puts an `async-rabbitmq` command on your path, written on
this API, for the things you would otherwise open a console for.

```bash
export RABBITMQ_URL=amqp://guest:guest@localhost:5672   # or pass --url

async-rabbitmq publish orders '{"id":1}' --count 10 --persistent
async-rabbitmq publish orders --file payload.json
echo '{"id":2}' | async-rabbitmq publish orders
async-rabbitmq publish events.audit --exchange events   # queue name becomes the routing key

async-rabbitmq inspect orders          # orders: 12 messages, 2 consumers
async-rabbitmq consume orders --count 5
async-rabbitmq consume orders --peek   # print without acknowledging: nothing is removed
async-rabbitmq purge orders
```

`publish` uses confirms and mandatory routing, so it exits non-zero and says so
when the broker nacks a message or sends it back unroutable. `consume`
acknowledges what it prints, stops after `--count` or `--timeout` seconds of
quiet, and leaves anything beyond the count on the queue. `--quiet` prints the
messages and nothing else, for piping. Options go after the command.

## Performance and integrity harness

`examples/` holds a sender and a receiver that load the broker and check what
comes out the other end. They are meant for the failure modes a throughput
number hides: a message lost, doubled or delivered out of sequence, a body
handed over with another message's header, a delivery on the wrong queue, a
reply given to the wrong caller.

```bash
ruby -Ilib examples/perf_consumer.rb  --streams 4                     # start first
ruby -Ilib examples/perf_publisher.rb --streams 4 --messages 50000
```

Every message is deterministic in `(run, stream, seq, size)` and states its
identity twice, in the AMQP properties and inside the body, so the receiver can
rebuild the bytes it should have been given and compare. One queue per
publisher stream, one consumer, one handler: that is where AMQP promises order,
so out-of-sequence delivery there is a real fault, and the report says so.
`--consumers` or `--handlers` above 1 makes deliveries concurrent and the
report downgrades ordering to an observation.

The sender publishes from several fibers over **one shared channel** by default
(`--channels per-stream` for one each), which is the case worth stressing: it
checks that confirm delivery tags are unique and cover every publish, that
nothing came back unroutable, and, with `--rpc-probe N`, that passive declares
issued from other fibers while the channel is saturated each get their own
reply. Useful switches: `--batch`, `--confirms none|simple|tracking`,
`--persistent`, `--rate`, `--size`. The receiver takes `--mode get` to exercise
the `basic_get` path instead of a consumer, plus `--prefetch` and
`--[no-]manual-ack`. Both exit non-zero when anything failed, so they can gate
a build.

`examples/perf_fault_inject.rb` publishes one of each fault on purpose; run it
against a receiver to see the checks fire rather than trusting them.

Two things the numbers do not say. Latency is measured from a monotonic clock,
so both programs must run on the same host, and it only means anything while
the receiver keeps up (pace the sender with `--rate`). And with `--confirms
none`, nothing proves the broker received anything: see *What a returned
publish means here* above.

## Development

The suite runs against a real RabbitMQ (with Toxiproxy for network faults):

```bash
docker compose -f spec/docker-compose.yml up -d --wait
bundle exec rspec
```

`spec_helper` starts the containers itself if nothing listens on the
configured port, and generates the TLS certificates with
`spec/docker/gen-certs.sh`. Ports and hosts come from `.env`.

## Releasing

The version lives in `lib/async_rabbitmq/version.rb` and nowhere else. To cut a
release: bump it, date the section in `CHANGELOG.md`, run the suite against a
real broker, then

```bash
gem build async-rabbitmq.gemspec        # writes async-rabbitmq-<version>.gem
gem install ./async-rabbitmq-<version>.gem   # optional: check it installs and the command runs
gem push async-rabbitmq-<version>.gem   # asks for your RubyGems OTP
git tag -a v<version> -m "v<version>" && git push origin v<version>
```

The gemspec sets `rubygems_mfa_required`, so publishing and yanking need
multi-factor authentication on the RubyGems account.

## License

MIT, see [LICENSE](LICENSE).
