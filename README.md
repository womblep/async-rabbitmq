# async-rabbitmq

A RabbitMQ (AMQP 0-9-1) client for Ruby's fiber scheduler, built on
[Async](https://github.com/socketry/async) and
[amq-protocol](https://github.com/ruby-amqp/amq-protocol). No threads: the
reader, the heartbeat and every handler are fibers, so it fits Falcon and
anything else that runs under `async`.

```ruby
require "async"
require "async/rabbitmq"

Sync do
  Async::RabbitMQ::Client.open("amqp://guest:guest@localhost") do |client|
    events = client.exchange("events", :topic)
    audit  = client.queue("events.audit").bind(events, routing_key: "audit.#")

    events.publisher do |publisher|
      publisher.publish("user 42 logged in", routing_key: "audit.login")
    end

    audit.consumer do |consumer|
      consumer.each do |message|
        puts message.body
        message.ack
        consumer.stop
      end
    end
  end
end
```

## Contents

- [Installing](#installing)
- [Two clients in one gem](#two-clients-in-one-gem)
- [How it is put together](#how-it-is-put-together)
- [Connecting](#connecting)
- [Queues, exchanges and bindings](#queues-exchanges-and-bindings)
- [Publishing](#publishing)
- [Consuming](#consuming)
- [When the connection is lost](#when-the-connection-is-lost)
- [A cluster](#a-cluster)
- [Errors](#errors)
- [Logging](#logging)
- [OpenTelemetry](#opentelemetry)
- [What is not here](#what-is-not-here)
- [Development](#development)

## Installing

```ruby
# Gemfile
gem "async-rabbitmq", "~> 0.10"
```

Requires Ruby 3.4+ and RabbitMQ 3.13+ (tested against 4.x). Everything here
runs inside a reactor: `Sync do ... end`, an `Async` task, or a Falcon
application.

## Two clients in one gem

The gem holds two clients. An application uses one or the other, chosen by
what it requires:

| | This client | The legacy engine |
|---|---|---|
| Require | `require "async/rabbitmq"` | `require "async_rabbitmq"` |
| Namespace | `Async::RabbitMQ` | `AsyncRabbitMQ` |
| Shape | Built for `async` | Mirrors Bunny's API |
| Status | Where new work goes | Kept working, no new features |
| Documentation | This file | [docs/legacy-engine.md](docs/legacy-engine.md) |

Neither loads any of the other's code.

Bundler requires a gem by its name, and `async-rabbitmq` is read as
`async/rabbitmq`. An application that stays on the legacy engine should say
which it wants:

```ruby
gem "async-rabbitmq", "~> 0.10", require: "async_rabbitmq"
```

## How it is put together

There are four things to know, and the rest follows from them.

- **A `Client` is one connection to one broker, and it keeps it.** When the
  connection is lost the client opens another and declares again whatever it
  was asked to declare.
- **A `Publisher` publishes and a consumer consumes.** Each has an AMQP
  channel to itself, which you never see, and each moves itself to the
  client's next connection when one is lost.
- **`publish` returns when the broker has taken the message**, and raises if
  it did not. Publisher confirms are always on.
- **A consumer is pulled from.** `each` hands your block one message at a
  time, and the message settles itself: `message.ack`.

While there is no connection, anything that needs one waits for it. Nothing
raises because of an outage unless you ask for that.

## Connecting

```ruby
client = Async::RabbitMQ::Client.open("amqps://app:secret@rabbit.example.com/orders")
# ...
client.close
```

With a block, `open` closes the client when the block returns or raises.
Closing a client closes every publisher and consumer made through it.

The address is a URL, or an `Endpoint` when there is more to say:

```ruby
endpoint = Async::RabbitMQ::Endpoint.parse("amqp://rabbit.example.com/orders",
                                           heartbeat: 30, connection_name: "billing-worker")
client   = Async::RabbitMQ::Client.open(endpoint)
```

| Endpoint option | Default | Meaning |
|---|---|---|
| `heartbeat` | 60 | Seconds; 0 asks for none |
| `timeout` | 10 | Seconds to connect and complete the handshake |
| `call_timeout` | 15 | Seconds to wait for the broker to answer a request; `nil` waits for ever |
| `connection_name` | none | Shown in the broker's management UI |
| `channel_max` | 2047 | Most channels, and so most publishers and consumers, on the connection |
| `frame_max` | 131072 | Largest frame, in bytes |
| `auth_mechanism` | negotiated | `"PLAIN"` or `"EXTERNAL"` to force one |
| `tcp_user_timeout` | from the heartbeat | Milliseconds before the kernel gives up on unacknowledged data |

A URL may carry `heartbeat`, `channel_max`, `connection_timeout` and
`auth_mechanism` as query parameters. A keyword argument wins over the URL.

| Client option | Default | Meaning |
|---|---|---|
| `reconnect_interval` | 1 | Seconds before the first attempt to reconnect; doubles with each failure, with jitter |
| `reconnect_max_interval` | 30 | Where the doubling stops |
| `reconnect_attempts` | `nil` | Failures in a row that end the client; `nil` never gives up |
| `shuffle` | `true` | With several endpoints, try them in a new random order each time |

### Several addresses

Given a list, the client connects to whichever will have it, and tries them
all again when that connection is lost. It is still one connection to one
node at a time. For a connection to every node, see [A cluster](#a-cluster).

```ruby
Async::RabbitMQ::Client.open(%w[amqp://rabbit1 amqp://rabbit2 amqp://rabbit3])
```

### TLS

An `amqps://` URL verifies the broker against the system's trust store. For a
private CA or a client certificate, build a context:

```ruby
context  = Async::RabbitMQ::TLS.context(cert: "client.pem", key: "client.key",
                                        ca_certificates: ["ca.pem"])
endpoint = Async::RabbitMQ::Endpoint.parse("amqps://rabbit.example.com", ssl_context: context)
```

Each of `cert`, `key` and `ca_certificates` takes a file path or PEM text, so
material from a secrets manager works as well as a file. The same can be said
in the URL with `cacertfile`, `certfile`, `keyfile` and `verify=false`.

With a client certificate the client offers `EXTERNAL` authentication, where
the broker takes the user from the certificate, and falls back to `PLAIN`.
`EXTERNAL` needs the `rabbitmq_auth_mechanism_ssl` plugin on the broker. It
is not covered by this gem's integration tests, because the test broker does
not have the plugin.

## Queues, exchanges and bindings

```ruby
orders = client.queue("orders")                            # durable
events = client.exchange("events", :topic)                 # durable
orders.bind(events, routing_key: "order.*")

quorum = client.queue("payments", type: :quorum)
ttl    = client.queue("scratch", arguments: { "x-message-ttl" => 60_000 })
```

Whatever is declared through the client is remembered, and declared again on
every new connection before anything else is let onto it. `client.forget(queue)`
drops one from that record; `queue.delete` and `exchange.delete` do so too.

| On a queue | |
|---|---|
| `bind(exchange, routing_key:, arguments:)`, `unbind(...)` | Returns the queue, so it chains |
| `message_count`, `consumer_count`, `exists?` | Asked of the broker each time |
| `purge`, `delete(if_unused:, if_empty:)` | |
| `consumer(**options)`, `managed_consumer(**options)` | A consumer of this queue |

| On an exchange | |
|---|---|
| `bind(source, routing_key:, arguments:)`, `unbind(...)` | Exchange-to-exchange |
| `exists?`, `delete(if_unused:)` | |
| `publisher(**options)` | A publisher to this exchange |

`client.queue?("name")` and `client.exchange?("name")` ask whether one exists
without declaring it. `client.default_exchange` is the default exchange.

A queue declared on the client is always durable. RabbitMQ 4 refuses a queue
that is neither durable nor exclusive, and closes the whole connection to say
so, so `durable: false` raises `ArgumentError` before it is sent. For a queue
that should go when you do, let a consumer declare its own: see [A queue of the consumer's own](#a-queue-of-the-consumers-own).

## Publishing

```ruby
publisher = client.publisher("events")          # an exchange, by name or object
publisher.publish("hello", routing_key: "order.created")

publisher = client.publisher                    # the default exchange:
publisher.publish("hello", routing_key: "orders")   # the routing key is the queue
```

`publish` returns `true` once the broker has confirmed the message. Many
fibers can publish through one publisher at once, and each waits only for its
own message.

It raises when the message was not taken:

| Raises | When |
|---|---|
| `NackedError` | The broker refused it |
| `ReturnedError` | It was `mandatory` and no queue took it |
| `UnconfirmedError` | The connection was lost before the broker answered |
| `ChannelError` | The broker closed the channel, for example over an exchange that does not exist |
| `ClosedError` | The publisher was closed first |

The message's AMQP properties are keyword arguments: `content_type`,
`content_encoding`, `headers`, `priority`, `correlation_id`, `reply_to`,
`expiration`, `message_id`, `timestamp`, `type`, `user_id`, `app_id`. So are
`exchange`, `mandatory` and `persistent`, to override the publisher's own for
one message. A body is a `String`.

| Publisher option | Default | Meaning |
|---|---|---|
| `persistent` | `true` | Messages survive a broker restart, in a durable queue |
| `mandatory` | `false` | The broker returns a message no queue takes, and `publish` raises |
| `timestamps` | `true` | Stamp each message with when it was published; see [Message age](#message-age) |
| `max_unconfirmed` | 1000 | Messages sent and not yet answered; a publish beyond that waits |
| `resend` | `false` | Send unanswered messages again after a lost connection; see below |

### Not waiting: `submit`

`submit` sends the message and returns an `Outcome` without waiting for the
broker. This is how to publish a batch and then collect the answers:

```ruby
outcomes = orders.map { |order| publisher.submit(order.to_json, routing_key: "orders") }
outcomes.each(&:wait)        # raises for the first that was not taken
```

`outcome.wait(timeout: 5)` raises `TimeoutError` if no answer comes in time,
and can be called again. `outcome.status` is `:pending`, `:acked`, `:nacked`,
`:returned` or `:failed`, and `outcome.error` is what `wait` would raise.

### A message caught by a lost connection

A message that had been sent, and not answered for, when the connection went
is a problem with no clean answer: the broker may have it or may not, and
nothing will ever say which. By default it fails with `UnconfirmedError` and
you decide. `resubmit` sends it again, at the risk of a second copy, and the
same outcome then answers for the new one:

```ruby
outcome = publisher.submit(body, routing_key: "orders")
begin
  outcome.wait
rescue Async::RabbitMQ::UnconfirmedError
  outcome.resubmit
  retry
end
```

A publisher made with `resend: true` makes that choice for every message. On
the new connection it sends again everything unanswered, in the order it was
first sent, and fibers waiting in `publish` go on waiting. Use it when
consumers can stand to see a message twice.

A message published while there is no connection has not been sent at all. It
waits for the next connection, under either setting.

### Message age

With `timestamps` on, each message carries the standard `timestamp` property
(seconds) and a `timestamp_in_ms` header. A consumer reads them back:

```ruby
message.published_at    # a Time, to the millisecond
message.elapsed         # seconds since it was published, as a Float
```

Both are `nil` for a message that carries neither. Setting the two fields
costs a few microseconds a message; `timestamps: false` leaves them out.

## Consuming

There are two consumers. `Consumer` gives you each message and leaves the
rest to you. `ManagedConsumer` runs your handler and looks after
concurrency, acknowledging and stopping.

### `Consumer`

```ruby
consumer = client.consumer("orders", prefetch: 10)
consumer.each do |message|
  process(message.body)
  message.ack
rescue InvalidOrder
  message.reject(requeue: false)
end
```

`each` runs the block in the fiber that called it, one message at a time.
Nothing is settled for you: a block that returns without `ack`, `nack` or
`reject` leaves the message unacknowledged, which is how a message is handed
to another fiber to finish later. An exception out of the block ends the loop
and is raised from `each`.

`each` returns when the consumer goes away, and says why:

| Returns | When |
|---|---|
| `:stopped` | `consumer.stop` was called |
| `:cancelled` | The broker cancelled the consumer, as when the queue is deleted |
| `:closed` | The consumer or its client was closed |
| `:connection_lost` | The connection was lost and the consumer was made with `on_connection_loss: :end` |

However it ends, messages that had arrived but had not reached the block go
back on the queue. `stop` is safe from any fiber. It does not interrupt a
block that is running; `each` returns when the block finishes.

| Consumer option | Default | Meaning |
|---|---|---|
| `prefetch` | 100 | Unsettled messages the broker sends at once; `nil` for no limit |
| `on_connection_loss` | `:wait` | `:wait` or `:end`; in a cluster also `:move` |
| `exclusive` | `true` with no queue named | The queue is the consumer's own |
| `queue_arguments` | `{}` | Arguments for the consumer's own queue |
| `arguments` | `{}` | Arguments for `basic.consume` |

### A message

| | |
|---|---|
| `body`, `headers`, `routing_key`, `exchange` | |
| `message_id`, `correlation_id`, `reply_to`, `content_type`, and the other AMQP properties | |
| `redelivered?`, `persistent?` | |
| `published_at`, `elapsed` | See [Message age](#message-age) |
| `ack`, `nack(requeue: true)`, `reject(requeue: true)` | Settle it. Only the first of these is sent |
| `settled?`, `settlement` | Whether, and how, it was settled |
| `stale?` | Its connection has gone; the broker has requeued it |

### `ManagedConsumer`

```ruby
consumer = client.managed_consumer("orders", concurrency: 10)
consumer.run do |message|
  process(message.body)
end
```

`run` blocks until the consumer goes away, and returns what `each` would.
Handlers run in fibers of their own, up to `concurrency` at a time. A handler
that returns has its message acked. One that raises has it nacked without
requeue, so it is dead-lettered if the queue has a dead-letter exchange and
discarded if not, and the consumer carries on. A handler's exception never
ends `run`. A handler may settle the message itself, and what it did stands.

| ManagedConsumer option | Default | Meaning |
|---|---|---|
| `concurrency` | 1 | Handlers running at once |
| `prefetch` | `concurrency` | Unsettled messages the broker sends at once |
| `ack_on_success` | `true` | Ack when the handler returns |
| `exception` | `:nack` | What to do when the handler raises: `:ack`, `:nack` or `:reject` |
| `requeue` | `false` | For `:nack` and `:reject`, whether the message goes back on the queue |
| `drain_timeout` | 30 | Seconds a stop waits for running handlers; `nil` waits for ever |

It takes `Consumer`'s options too.

Stopping is graceful, whether by `stop`, by `close`, or by stopping the task
that is running `run`. Nothing new arrives, messages that had not started go
back on the queue, and running handlers get `drain_timeout` to finish. Any
still running after that are stopped and their messages requeued. A stop
never loses a message.

`consumer.on_error { |error, message| ... }` is called when a handler raises,
before the message is settled.

### A queue of the consumer's own

Given no queue, the consumer declares one that is its alone: exclusive to its
connection, and gone when that connection goes.

```ruby
replies = client.consumer(prefetch: 50)
replies.queue_name                               # the name the broker chose
replies.bind("events", routing_key: "order.*")
```

After a lost connection the queue is declared and bound again on the next
one. Whatever was in it went with the old connection, and a queue the broker
named has a new name. `on_attach` is how to hear of it:

```ruby
replies.on_attach { |consumer| announce(consumer.queue_name) }
```

The queue can be named instead, by passing a name with `exclusive: true`. The
same name is then declared again after every reconnect.

- **On a single node this is fine.** The old queue is gone by the time the
  new connection declares it.
- **In a cluster it can fail.** An exclusive queue is deleted when its
  connection closes, and the nodes do not all learn of that at once. A
  declaration that arrives before the old queue is gone everywhere is
  refused, and `each` raises `ChannelError` with `resource_locked?` true.

In a cluster, let the broker name the queue. A name it chooses is new each
time, so it cannot collide with the old one.

## When the connection is lost

The client reconnects, with a delay that starts at `reconnect_interval`,
doubles to `reconnect_max_interval`, and is jittered so a fleet does not come
back in one instant. On the new connection it declares its queues, exchanges
and bindings again. Then each publisher and consumer attaches itself.

| | What happens |
|---|---|
| A declaration or a query in progress | Sent again on the new connection |
| A `publish` not yet sent | Waits, and is sent on the new connection |
| A `publish` sent and not answered | Raises `UnconfirmedError`, or is sent again with `resend: true` |
| A consumer | Consumes again on the new connection |
| A message delivered on the old connection | Stale: the broker requeues it and sends it again. Settling it does nothing, and one not yet handed to the block is dropped |
| A consumer's own queue | Declared and bound again, empty |

A consumer made with `on_connection_loss: :end` does not wait: `each` returns
`:connection_lost`.

With `reconnect_attempts` set, the client gives up after that many failures
in a row. Everything waiting on it then raises `ConnectionError`.

When the broker blocks publishing (a memory or disk alarm), publishes wait
until it unblocks.

## A cluster

A `Client` is one connection to one node, and everything made through it
lives on that node. An application that makes many consumers, each with a
queue of its own, puts all those queues on the one node it happens to be
connected to. A `Cluster` holds a `Client` for every node and shares the load
out.

```ruby
nodes = %w[amqp://rabbit1 amqp://rabbit2 amqp://rabbit3]
Async::RabbitMQ::Cluster.open(nodes) do |cluster|
  replies = cluster.consumer(prefetch: 50)     # on the node with the fewest
  replies.queue_name
end
```

A cluster has the same methods as a client: `queue`, `exchange`,
`publisher`, `consumer`, `managed_consumer`. It takes the client's options,
which apply to every node.

- **Placement.** A new publisher or consumer goes to the connected node with
  the fewest publishers and consumers already, at random between ties.
- **Declarations** are sent to one node and remembered by all.
- **Starting.** `open` returns once every node has answered or failed. Those
  that failed join in the background. It raises only if no node could be
  reached, or if one refused the credentials.

When a node goes away, nothing moves by default. A queue lives on a node, and
a consumer moved elsewhere has a new, empty queue, so the application has to
be ready for that. A consumer chooses with `on_connection_loss:`:

| Setting | What happens |
|---|---|
| `:wait` (default) | Stays with its node and consumes again when it is back |
| `:end` | `each` returns `:connection_lost` |
| `:move` | Goes to the least-loaded connected node. Its own queue is declared and bound again there, empty, and `on_attach` is called |

A consumer that moved does not move back. With nowhere to go, it waits where
it is. A publisher always stays with its node and waits for it; one that
should publish through whichever node is up belongs on a `Client` given all
the addresses.

The cluster is tested against one broker reached by two routes, not against
a real multi-node cluster.

## Errors

Everything raised is an `Async::RabbitMQ::Error`.

| Error | Meaning |
|---|---|
| `ConnectionError` | The connection failed or was lost. Has `code` and `text` when the broker closed it |
| `ConnectTimeoutError`, `AuthenticationError`, `HeartbeatTimeoutError`, `ProtocolError` | Kinds of `ConnectionError` |
| `ChannelError` | The broker refused a request. Has `code`, `text`, and `not_found?`, `access_refused?`, `resource_locked?`, `precondition_failed?` |
| `PublishError` | A message was not taken: `NackedError`, `ReturnedError`, `UnconfirmedError` |
| `ClosedError` | The client, publisher or consumer has been closed |
| `TimeoutError` | A wait ran out of time |
| `ChannelLimitError` | The connection has no channel left for another publisher or consumer |

A `ChannelError` from a declaration is the broker's answer and is raised to
the caller. The client stays usable.

## Logging

The client logs through [Console](https://github.com/socketry/console), like
the rest of the async stack: a warning when a connection is lost, a line when
it is back, an error when a handler raises. Set the level with
`CONSOLE_LEVEL=debug`, or configure Console as you would for any async
application. There is no `logger:` option.

## OpenTelemetry

Spans and metrics are optional, and not loaded unless you ask:

```ruby
require "async/rabbitmq/open_telemetry"
Async::RabbitMQ::OpenTelemetry.install      # after OpenTelemetry is configured
```

This needs `opentelemetry-api` in your bundle, and `opentelemetry-metrics-api`
for the metrics. `install(tracer_provider:, meter_provider:)` takes either as
`nil` to leave that half off.

| Span | Kind | Around |
|---|---|---|
| `publish <exchange>` | producer | `submit` and `publish`. Ends when the message is written, not when it is confirmed |
| `process <queue>` | consumer | The block of `Consumer#each`, or the handler of `ManagedConsumer#run` |

The trace context travels in the message's headers, and the process span is a
child of the publish span, so a trace crosses the broker. A handler's
exception is recorded on its span. Attributes follow OpenTelemetry's
messaging conventions.

| Metric | Type | |
|---|---|---|
| `messaging.client.sent.messages` | counter | One per answer from the broker, with `error.type` when it was not an ack |
| `messaging.client.operation.duration` | histogram, s | From `submit` to the broker's answer |
| `messaging.client.consumed.messages` | counter | With `error.type` when the block or handler raised |
| `messaging.process.duration` | histogram, s | Time in the block or handler |
| `async.rabbitmq.message.age` | histogram, s | Publish to delivery, for messages with a timestamp |
| `async.rabbitmq.client.connections` | counter | Connections made |
| `async.rabbitmq.client.connection.losses` | counter | Connections lost |

Tracing is not free. On a development machine, spans and metrics together
roughly halved the publish and consume rates of small messages.

## What is not here

Left out on purpose. The legacy engine has most of them.

- Channels. Each publisher and consumer has its own, out of sight.
- `basic.get`. Use a consumer.
- Transactions. Use confirms, which are always on.
- A connection pool. One client serves many fibers.
- Publishing without confirms.
- A command-line tool. The `async-rabbitmq` command belongs to the legacy
  engine.
- Changing credentials on a live client.

## Development

The suite runs against a real RabbitMQ, with Toxiproxy for network faults:

```bash
docker compose -f spec/docker-compose.yml up -d --wait
bundle exec rspec             # both clients, about nine minutes
bundle exec rspec spec/v1     # this client only
```

`spec_helper` starts the containers itself if nothing listens on the
configured port, and generates the TLS certificates with
`spec/docker/gen-certs.sh`. Ports and hosts come from `.env`.

How the client is built is in
[docs/designs/async-client.md](docs/designs/async-client.md).

### Releasing

The version is in `lib/async/rabbitmq/version.rb` and
`lib/async_rabbitmq/version.rb`, which must agree; a spec checks it. To cut a
release: bump both, date the section in `CHANGELOG.md`, run the suite, then

```bash
gem build async-rabbitmq.gemspec
gem push async-rabbitmq-<version>.gem
git tag -a v<version> -m "v<version>" && git push origin v<version>
```

The gemspec sets `rubygems_mfa_required`, so publishing needs multi-factor
authentication on the RubyGems account.

## License

MIT, see [LICENSE](LICENSE).
