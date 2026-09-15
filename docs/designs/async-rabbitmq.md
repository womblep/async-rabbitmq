# async-rabbitmq — architecture

How the client is built, as of 0.2.0 (September 2026). It describes what exists,
not what was planned. `README.md` is the usage documentation; this document is
for people changing the internals.

## Purpose

A RabbitMQ (AMQP 0-9-1) client for Ruby applications already running under the
`async` fiber scheduler: Falcon, async-http, async-job. Nothing here uses a
thread. The reader, the writer, the heartbeat, connection recovery and every
consumer handler are fibers on the caller's reactor, so a blocking call yields
to the scheduler instead of parking a thread.

Bunny is the reference for the public API, and most of it is mirrored
deliberately, so that code and habits carry over. Bunny's execution model does
not carry over: consumer handlers run concurrently in their own fibers, not
serially on one consumer thread.

## What it does and does not do

Implemented: connection handshake with SASL negotiation (EXTERNAL preferred over
PLAIN, plus RABBIT-CR-DEMO, and `connection.secure` challenge/response), TLS,
multi-host failover, heartbeats, channels, queues, exchanges, bindings, consumers,
`basic_get`, publisher confirms with optional tracking and backpressure,
transactions, `basic.return`, `connection.blocked`, `channel.flow`,
`connection.update-secret`, automatic connection recovery with topology replay,
structured events for metrics and tracing, and an opt-in connection pool.

The gem also installs an `async-rabbitmq` command (`lib/async_rabbitmq/cli.rb`)
for publishing, consuming, inspecting and purging from a terminal. It is written
on the public API with no privileged access, which makes it a standing check
that the API is usable.

Not implemented: AMQP 1.0, the RabbitMQ HTTP management API, and a Bunny
compatibility shim. The shim was dropped deliberately rather than deferred:
anyone adopting this client is moving to fibers anyway, and an untested
compatibility layer would be worse than none.

## Layers

```
Session ── Channel ── Queue / Exchange              public API
   │          │
   │          ├── per-channel Async::Queue of decoded frames
   │          ├── @rpc_sem      one request/reply in flight
   │          ├── @publish_sem  one publish's frames written contiguously
   │          └── @pool_sem     bounds concurrent consumer handlers
   │
   ├── TopologyRegistry     what to re-declare after a reconnect
   ├── Notifier             structured events, shared with every channel
   ├── heartbeat task       direct socket write, bypasses the write queue
   ├── channel-0 monitor    blocked/unblocked, update-secret, connection.close
   └── recovery task        backoff, reconnect, reopen, replay
   │
FrameIO                                             transport
   ├── reader task   socket -> frames -> content reassembly -> channel queues
   └── writer task   Async::LimitedQueue(1024) -> socket
   │
amq-protocol (frame encode/decode) ── TCPSocket / OpenSSL::SSL::SSLSocket
```

One connection has exactly one reader fiber and one writer fiber. Everything
else is a caller's fiber parked on a condition or a queue.

## Frames

`AMQ::Protocol::Frame.decode` is an abstract stub in amq-protocol, so `FrameIO`
reads frames itself: 7 header bytes, `decode_header` for type, channel and
payload size, the payload, then the 0xCE terminator. The socket is a plain
`TCPSocket` or `OpenSSL::SSL::SSLSocket`; under the fiber scheduler `IO#read`
yields, so no stream wrapper is needed.

Content arrives as a method frame, a header frame and one or more body frames.
`FrameIO` reassembles them per channel (`@content_state`) and pushes one
`[:content, header, body]` message to the channel's queue, so a channel never
sees a partial message. A body frame without a header is dropped with a warning
rather than corrupting the next message.

Writes go through one `Async::LimitedQueue` of 1024 entries, drained by the
writer fiber. One entry is one `write_frame` call: `basic_publish` encodes
method, header and body into a single buffer so a message's frames cannot be
interleaved with another fiber's, and `basic_publish_batch` does the same for a
whole batch. A full queue makes the publishing fiber yield, which is the
backpressure mechanism. Heartbeats bypass the queue and write to the socket
under a semaphore, so a backlog can never starve them.

Because publishing only enqueues, a returned `basic_publish` does not mean the
broker has the message. Confirms are the only thing that does. `Session#close`
bounds the close handshake at 5 seconds and discards whatever the writer has not
reached by then, logging the count.

## Concurrency on a channel

RabbitMQ's documentation says concurrent publishing on a shared channel is not
supported by client libraries. Here it is, because a fiber-native application
will do it whether or not it is safe: `@rpc_sem` serialises request/reply pairs
so a reply always reaches the fiber that asked for it, and `@publish_sem`
serialises publishes so frames stay contiguous and confirm delivery tags follow
wire order. Sharing a channel is still a bottleneck, and a synchronous call
queues behind a publish backlog.

Consumer handlers are spawned as tasks and gated by `@pool_sem`, sized by
`pool_size` and coupled to `basic_qos(prefetch_count:)`. Deliveries to one queue
are therefore ordered on the wire but may be processed concurrently, so ordering
guarantees only hold end to end with one handler.

## Channel states and recovery

A channel is `:open`, `:recovering` or `:closed`.

When the connection drops, the session marks every channel `:recovering`. Calls
already waiting for a reply fail with `ConnectionError`; new publishes and
requests park on a condition rather than raising, so work issued during an
outage is not lost. The recovery task reconnects with exponential backoff
(1 s doubling to 30 s, ±25% jitter, unlimited attempts by default), walking the
address list. On success the session:

1. reopens every channel, restoring prefetch, confirm mode and transaction state,
2. replays the recorded topology: exchanges, then queues, then bindings,
3. re-publishes whatever was still unconfirmed, then re-registers consumers,
   releases the parked callers, and fires `on_recovery`.

The re-publish deliberately follows the replay. A message addressed to an
exchange the broker no longer has is answered with a 404 that closes the
freshly reopened channel, and one addressed to a live exchange whose binding is
missing is dropped while the broker still acks it, which looks like success.
`Channel#reopen` is the exception: it recovers one broker-closed channel while
the connection and its topology are intact, so it re-publishes immediately.

Unconfirmed messages are held as encoded frames, so a re-published message
carries the exchange and routing key it was published with. If that routing key
named a server-named queue, the queue comes back under a new name and the
re-published copy goes to the old one: unroutable, acked, gone. Such a queue is
exclusive or auto-delete in practice, so it did not survive the disconnect
either way.

`TopologyRegistry` records what was declared through the session, filtered by an
optional `topology_recovery_filter`. Passive declares are not recorded, and
deleted or unbound entities are removed. A server-named queue comes back under a
new name, which is propagated to its bindings, its consumers and the `Queue`
object the caller still holds.

The registry mirrors the broker, not the channel that did the declaring. A
channel closing does not remove what it declared: a durable queue still exists
and an exclusive one lives as long as the connection, and both are commonly
declared on one channel and used from another. What a channel closing does
remove is its consumers, and the broker deletes an auto-delete queue when the
last consumer goes. So the registry tracks consumers as well, and forgets an
auto-delete queue with its last one, by `basic_cancel`, broker-side cancel or
channel close, taking its bindings with it and any auto-delete exchange those
bindings were keeping alive. Consumers on other connections are invisible to
it; the cost of guessing wrong there is only that recovery skips a queue that
still exists, which is harmless.

Channel ids come from `ChannelIdAllocator`, a bitset (`AMQ::BitSet`, the
structure under Bunny's allocator) over `1..channel_max`. Ids are released when
a channel closes, so a channel per unit of work does not exhaust the 16-bit id
space, and `open_channel` raises `ChannelLimitError` at the negotiated limit
rather than letting the broker close the connection with a 530. `IntAllocator`
from the same gem was not used because `Channel#reopen` needs to reserve a
specific id, which it cannot do.

Re-publishing unconfirmed messages is at-least-once: a message the broker had
already accepted but not yet confirmed is delivered twice.

If recovery is disabled, exhausted, or the broker refuses the credentials, every
channel is marked `:closed` and parked callers raise.

## Publisher confirms

`confirm_select` alone tracks delivery tags and lets `wait_for_confirms` return
false when something was nacked. `confirm_select(tracking: true)` adds
backpressure: publishing parks while `outstanding_limit` messages (default 1000)
are unconfirmed, and `wait_for_confirms` raises `MessageNacked` instead of
returning false. Unconfirmed messages are held as encoded bytes, which is what
makes the re-publish on recovery possible.

## Errors

Soft errors close the channel and leave the connection up. They raise
`ChannelError`, which carries `code`, `text`, `channel_id` and the decoded
`close_method`, with predicates for the cases worth branching on
(`delivery_ack_timeout?`, `unknown_delivery_tag?`, `message_too_large?`). The
soft set is 311, 312, 313, 403, 404, 405 and 406. A broker-closed channel can be
reopened in place with `Channel#reopen`.

Everything else is a hard error: the connection is gone, `ConnectionError` is
raised, and recovery takes over. A 403 during the handshake becomes
`AuthenticationError` and is never retried. A synchronous call that gets no
reply within `rpc_timeout` raises `RpcTimeoutError` naming the AMQP method it
was waiting for.

## Connection pool

`AsyncRabbitMQ::Pool` wraps `Async::Pool::Controller` with a constructor that
returns a connected `Session`. `Session` implements the resource contract the
controller expects: `reusable?` (open and not recovering), `viable?` (open) and
`concurrency` (the negotiated `channel_max`, else 2047). Because concurrency is
the channel limit, the controller hands one session to many fibers and only
opens another connection when the existing ones are saturated.

## Events

`Notifier` holds the subscribers; `Session` owns one and hands the same object
to every channel it opens, so one subscription sees the whole connection. Call
sites go through `Instrumented#instrument`, which checks for subscribers before
running the block that builds the payload. An unwatched session therefore pays
one method call and one array check per event and allocates nothing, which is
what makes it acceptable to instrument the publish and delivery paths.

Timing works the same way: `instrument_clock` returns nil when nobody is
listening, so the clock reads are skipped too. A subscriber that raises is
caught and logged, because instrumentation must not be able to take a
connection down. Event names are treated as API and listed in `Notifier::EVENTS`;
payload keys are documented in the README.

Tracing is separate, in `lib/async_rabbitmq/telemetry/open_telemetry.rb`, and is
neither loaded nor depended on by default. It cannot be built on the events
above: a span has to wrap the operation, and the trace context has to be
injected into the headers before the frames are encoded, which is too early for
an after-the-fact notification. It is therefore a module prepended to `Channel`
by `Telemetry::OpenTelemetry.install`, inert until a tracer is set. Span names
and attributes copy opentelemetry-instrumentation-bunny so that a service moving
over keeps its dashboards.

## Defaults

| Setting | Value |
|---|---|
| Connect timeout (handshake) | 30 s |
| RPC timeout (synchronous calls) | 15 s, `nil` waits forever |
| Close handshake budget | 5 s |
| Heartbeat send interval | negotiated timeout / 2 |
| Heartbeat dead-connection threshold | negotiated timeout x 2 |
| Recovery backoff | 1 s, doubling to 30 s, ±25% jitter, unlimited |
| Write queue | 1024 entries |
| Confirm tracking outstanding limit | 1000 |
| Consumer handlers per channel | 1, or `prefetch_count` after `basic_qos` |

## Design decisions

| Decision | Choice | Why |
|---|---|---|
| Channel stream interface | Duck-typed `#each` / `#write` | A channel is a logical multiplex, not physical IO; inheriting a stream class was a category error |
| Frame reading | Manual header, payload, terminator | `AMQ::Protocol::Frame.decode` is an abstract stub |
| Write path | Single writer fiber behind a bounded queue | One writer keeps frames ordered; the bound prevents unbounded memory under publish pressure |
| Heartbeat | Direct socket write under a semaphore | Must never be starved by a publish backlog |
| Shared-channel publishing | Supported, serialised per channel | Fiber applications share channels regardless; serialising is cheaper than corrupt frames |
| During recovery | Park new work, fail work already in flight | A caller mid-request must learn it failed; a caller about to publish would rather wait |
| Unconfirmed on reconnect | Re-publish | At-least-once beats silent loss; documented as a duplicate risk |
| Close with a backlog | Discard and log the count | Fire-and-forget publishing offers no guarantee, but the loss should be visible |
| Test queues | Durable or exclusive | RabbitMQ 4.2+ refuses transient non-exclusive queues at connection level |

## Tests

283 examples, almost all integration tests against a real broker
(`rabbitmq:4-management-alpine` plus Toxiproxy, started by `spec/docker-compose.yml`
or by `spec_helper` if nothing is listening). Each test gets its own vhost, so
tests cannot contaminate each other. CI runs Ruby 3.4 and 4.0 over
`spec/integration` and enforces 90% line coverage.

Beyond the per-feature specs, the suite covers what only breaks under stress or
failure: a severed connection mid-consume with recovery, topology replay and
consumer re-registration, heartbeat cadence and dead-connection detection,
publishes parked on `connection.blocked`, bodies several times `frame_max`
reassembled intact, 20 fibers sharing one channel without crossed replies, ten
channels publishing concurrently without interleaving, and a 10,000 message run
over 100 fibers under confirms (opt-in with `FRIDAY_2AM=1`).

A connection is severed with `shutdown(2)`, not `close`: on Linux, closing a
socket from another fiber does not wake a reader blocked in the scheduler, so a
close-based test passes on Windows and hangs until the heartbeat timeout in CI.
`spec_helper` provides `sever_connection!` and `recover_connection!`, and tests
wait for the `on_recovery` callback rather than polling `open?`.

`examples/` holds a load-generating publisher and a verifying consumer for the
faults a throughput number hides: loss, duplication, out-of-sequence delivery, a
body paired with another message's header, a delivery on the wrong queue, a
reply handed to the wrong caller. `examples/perf_fault_inject.rb` injects one of
each so the checks can be seen to fail.

## Naming and versions

The gem is `async-rabbitmq`; the namespace is `AsyncRabbitMQ`. Ruby 3.4 is the
floor (3.2 dropped April 2026, 3.3 dropped September 2026). RabbitMQ 3.13 and
later are supported, and 4.x is what the suite runs against.
