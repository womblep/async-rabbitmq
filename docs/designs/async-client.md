# Async::RabbitMQ: how the client is built

This describes what exists in `lib/async/rabbitmq/`. The [README](../../README.md) is the usage
documentation; this is for people changing the internals. The older client in `lib/async_rabbitmq/`
is described in [legacy-engine.md](legacy-engine.md), and shares no code with this one.

## The idea

The legacy engine mirrors Bunny: sessions, channels, callbacks, and recovery that repairs a channel
in place. Most of its difficulty came from that last part. A channel that survives its connection
has to renumber delivery tags, replay confirms, re-register consumers and keep every caller's view
consistent while it does.

This client does not repair anything. A connection that fails is thrown away with everything on it.
What survives is the things the application holds (a publisher, a consumer), and each of those
knows how to attach itself to a new connection on a new channel. Recovery is the same code path as
starting.

## Layers

| Layer | File | Lives for | Job |
|---|---|---|---|
| `Endpoint` | `endpoint.rb` | The application | Where the broker is, credentials, TLS, connection settings |
| `Protocol::Framer` | `protocol/framer.rb` | One connection | Bytes to frames and back, over an `IO::Stream` |
| `Connection` | `connection.rb` | One connection | Handshake, reader fiber, heartbeat, channel table. Never reconnects |
| `Channel` | `channel.rb` | One connection | One AMQP channel with a single owner. Internal |
| `Client` | `client.rb` | The application | Supervises connections, reconnects, re-declares topology |
| `Publisher`, `Consumer`, `ManagedConsumer` | | The application | "Links": each owns a channel and re-attaches itself |
| `Cluster` | `cluster.rb` | The application | One `Client` per node, and placement between them |

## Connection

A `Connection` is disposable. `Connection.open` does the handshake and starts two fibers: a reader,
and a heartbeat sender. When anything goes wrong (a read error, a missed heartbeat, the broker's
`connection.close`) the connection resolves a promise with the error, fails every channel, and is
finished. `connection.wait` returns that error. Nothing here retries.

Writes are batched. `Connection#write` appends to the stream's buffer without flushing and signals
a flusher fiber, which flushes once per turn of the reactor. Many fibers publishing in the same
turn share one system call. The stream's own write lock gives backpressure when the socket is full.

## Channel

A `Channel` has one owner and is not shared. Requests that expect a reply (`call`) go one at a time
under a semaphore. Everything the broker sends unasked (deliveries, confirms, returns, cancels, a
close) is handed to the owner's `Delegate` on the reader fiber. A delegate method must not wait: it
records the event and wakes the fiber that cares.

## Client

`Client#connect` opens a connection, declares the recorded topology on it, and resolves `@ready`
with it. A supervisor task then waits on `connection.wait`. When the connection is lost it replaces
`@ready` with a new promise and reconnects with jittered exponential backoff. Anything that needs a
connection waits on `@ready`.

`Topology` records every queue, exchange and binding declared through the client. It is declared
again on each new connection, before `@ready` resolves, so no link can reach a connection that does
not yet have its queues. An entity the broker refuses is logged and skipped.

Declarations and queries go through one "admin" channel, opened when needed. A request cut off by a
lost connection is sent again on the next one, since all of them can be repeated without harm. One
that loses the connection three times running is taken to be the cause, and raises.

The `on_*` hooks are lists of blocks on the client, called in line by whichever fiber noticed:
the supervisor for a loss and for each reconnect attempt, the connection's reader for blocked
and unblocked (the client gives each connection a block to call). They are not tasks of their
own, so the order they are called in is the order things happened in.

## Links

A link registers with the client so that closing the client closes it. Each holds a channel and
replaces it when it closes.

**Publisher.** A keeper task waits for the channel to close and attaches to the next connection.
Each message is an `Outcome` held in a hash keyed by its confirm tag, in the order sent.

- A confirm resolves the outcome, or every outcome up to the tag for a `multiple` confirm.
- A lost connection fails the outcomes that had been sent (`UnconfirmedError`), unless the
  publisher resends, in which case they are replayed in order on the new channel before anything
  new is sent.
- Outcomes not yet sent are kept either way.
- A channel closed by the broker fails every sent outcome with the `ChannelError`.
- A returned message carries no delivery tag, so a mandatory publish adds an
  `x-async-rabbitmq-publish-id` header and the return is matched by that.

**Consumer.** Deliveries are pushed onto a queue by the delegate and pulled off by `each`, in the
caller's fiber. The broker's prefetch bounds the queue. A `Message` holds the channel it arrived
on, and is stale once that channel has closed: settling it does nothing, and one still in the queue
is dropped, because the broker has requeued it. On a lost connection `each` either waits and
subscribes again, returns, or (in a cluster) asks to be moved. If the queue is not there to
subscribe to again, `each` returns `:cancelled`.

A `basic.cancel` from the broker ends `each` with `:cancelled` unless the consumer was made with
`on_cancel: :resubscribe`. Then `each` asks on the client's admin channel whether the queue
exists, and sends `basic.consume` again if it does, backing off from 1s to 30s while the broker
refuses. It never declares, so a deleted queue stays deleted. The first refusal after a cancel is
a warning and the rest are debug. `on_cancelled` and `on_resubscribed` are single blocks on the
consumer, like `on_attach`, called in the fiber running `each`. `on_resubscribed` is given the
number of attempts before the consumer, as the client's hooks put the client last.

A consumer's own queue is declared `exclusive` by the consumer on its own channel, and re-declared
and re-bound each time it attaches.

**ManagedConsumer.** A `Consumer` plus a pool of worker fibers, grown on demand up to `concurrency`
and reused. `run` only moves messages from the consumer to the pool's queue. Stopping cancels the
consumer, requeues what had not started, waits `drain_timeout` for what had, then stops the rest.

## Cluster

A `Cluster` holds one `Client` per node, all sharing one `Topology`. Placement is the connected
client with the fewest links, at random between ties. A consumer made with
`on_connection_loss: :move` is given a callback that returns another connected client; the consumer
then registers there and attaches as it would after any reconnect.

## Logging

Every log call has Console's shape, `logger.warn(self, "message", key: value, exception: error)`.
The client resolves its `logger:` once (`logging.rb`) into something that takes that call:
a forwarder to Console, a shim that flattens the call to one line for a stdlib-style logger,
or a null. The first two check `Async::RabbitMQ.log_level` before they pass anything on. Links log through their client's logger, and a channel through its connection's,
which the client passes in when it opens the connection.

## Telemetry

`open_telemetry.rb` is opt-in and prepends onto `Publisher`, `Publisher::Outcome`, `Consumer`,
`ManagedConsumer` and `Client`. The core carries no hooks for it. The `traces` and `metrics` gems
were considered and not used: the `traces` OpenTelemetry backend cannot set a span's kind, and
`metrics` has no OpenTelemetry backend.

## Tests

`spec/v1/` holds the specs for this client. They run against a real RabbitMQ, with Toxiproxy for
outages, and each example has a virtual host of its own. The cluster specs use one broker reached
by two routes, the direct port and the proxy.
