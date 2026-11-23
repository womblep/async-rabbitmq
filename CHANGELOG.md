# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed

- A publisher parked on a full write queue is released when the connection stops. It was woken
  only by the writer draining the queue, and the writer had just been cancelled - so it stayed
  parked forever, holding the channel's publish semaphore, and every publish on that channel
  after recovery queued behind it. A full write queue at disconnect is the likely case: TCP
  backpressure is how a partition presents itself to a writer. The queue is closed on stop; a
  write that loses the race is discarded, which is what already happened to every write that
  reached the dead queue. Requires async >= 2.24 (where `Async::Queue#close` appeared).
- A channel close releases every parked waiter, whichever way the close came. The broker
  closing the channel (a 404 publish under confirms is the canonical case) and the user calling
  `close` each woke the RPC caller but not a fiber in `wait_for_confirms` - which, with no
  timeout given (the default), parked forever on an ack that can never arrive - nor a publisher
  parked on an outstanding-confirm slot, which stalled `rpc_timeout` seconds holding the publish
  semaphore. All teardown paths now release all waiters, and `wait_for_confirms` raises the
  error it was handed - the channel's actual fate - rather than a generic disconnect message.
- `connect` refuses to run while the session is recovering or already connecting. A connect
  racing the recovery loop fought it for the connection, and when that connect failed - the
  broker being down is why recovery was running - its cleanup cancelled the recovery task and
  marked the session closed, stranding every recovering channel's parked fibers permanently:
  `close` was by then a no-op and a later `connect` never reopens recovering channels. Close the
  session first to force a fresh connection.
- SASL negotiation no longer auto-selects EXTERNAL over PLAIN. EXTERNAL authenticates by TLS
  certificate and sends no credentials, so picking it for a caller who supplied a password
  silently discarded that password - refused credentials against any broker whose
  `auth_mechanisms` advertises EXTERNAL, despite the password being valid. PLAIN now wins when
  a password is given and no client certificate is configured, as in Bunny and the Java client.
  Certificate material (`tls_cert:` / `tls_context:`) keeps EXTERNAL first - the certificate is
  the identity the caller configured on purpose, where the password is usually the untouched
  guest default - and `auth_mechanism:` still forces either outright.
- CI runs the unit specs, which were never exercised there, and the job times out after 30
  minutes instead of holding a runner for six hours when a toxiproxy spec wedges.

## [0.8.1] - 2026-11-15

### Fixed

- A consumer the broker cancelled is no longer given up on when the connection goes at the same
  moment. Deleting a quorum queue is both events at once: the consumer is cancelled, and the
  re-subscribe is answered with a connection-level `541 INTERNAL_ERROR ... noproc` that takes the
  connection down. 0.8.0's recovery loop stopped at the first sight of a session that was not
  open, and nothing else would have brought that consumer back - it is no longer in the channel's
  registry, so the reconnect does not re-register it either. It now waits through however many
  reconnects it takes, and gives up only when the channel or the session is finished for good.
- A queue whose consumer the broker cancelled is held out of topology recovery until the client
  has established whether it still exists. Without that the two race: a reconnect re-declares the
  whole registry, so a queue deleted a moment earlier came back - and a queue the broker hands
  back with a consumer attached cannot be deleted at all, which is the bug 0.8.0 set out to fix.
  Measured against RabbitMQ 4.4, deleting a quorum queue reached exactly that state.
- A connection lost mid-re-subscribe is no longer reported as `consumer.recovery_failed`. It has
  its own event and its own recovery, both already running, and an application alarming on
  recovery failures was told twice about one outage, in the words of the more serious of the two.
- A `404` that says the queue's process is stopped, rather than that there is no such queue, is no
  longer read as a deletion. A classic queue on a stopped node answers `NOT_FOUND - queue 'q' in
  vhost '/' process is stopped by supervisor` and comes back with its node; the client forgot it,
  so the consumer was never restored and the queue was never re-declared either. The probe now
  answers `:deleted`, `:available`, `:unavailable` or `:unreachable`, and only `:available` is followed by an
  attempt to consume - a `basic_consume` the broker answers with a 404 closes the channel and
  takes the consumers still working on it down with it.
- `on_cancel` now runs to completion before the client decides what became of the queue. The two
  ran alongside each other, and the documented way to keep a queue - re-declare it in `on_cancel` -
  is an RPC that yields, so the probe sometimes got its answer in first and concluded the queue
  was deleted. The callback is still off the dispatch fiber, so an RPC in it still gets its reply
  and an exception in it still cannot stop the channel.
- A cancelled queue is released back to topology recovery only on a definite answer that it
  still exists. A probe the broker could not answer is not evidence the queue is there, and
  treating it as such re-declared a queue that had just been deleted - the one thing this
  handling exists to prevent. The queue now stays held back until the broker says `:deleted`
  or `:available`, retrying with the same backoff the recovering path uses.
- The retry backoff no longer grows for a failure that was the connection's rather than the
  queue's, and resets once the connection is back. A two-second blip could otherwise leave a
  healthy queue unconsumed for the next thirty seconds, since each unanswerable probe doubled the
  delay. The queue probe now answers `:unreachable` - no answer at all - separately from
  `:unavailable`, which is the broker saying the queue is not being served.

## [0.8.0] - 2026-11-14

### Fixed

- A server-sent `basic.cancel` no longer leaves the queue in the topology registry. The broker
  cancels a consumer when it can no longer serve the queue, and one of the ways that happens is
  somebody deleting the queue - after which the next connection recovery re-declared it and the
  deletion never stuck. The client now checks, passively, whether the queue is really gone and
  forgets it, with its bindings, if it is. Only `NOT_FOUND` counts as gone: a replicated queue
  mid-election is unavailable, not deleted, and is left in the registry to be re-declared.
- `on_cancel` and `on_error` no longer run on the channel's dispatch fiber. The obvious thing to
  write in either is an RPC - re-consume, reopen - and an RPC on that fiber waits for a reply only
  that fiber can deliver: fifteen seconds to `rpc_timeout`, then a channel resync. An exception out
  of either escaped `dispatch_loop`, whose rescue sits outside its loop, so the channel stopped
  delivering anything at all, confirms and RPC replies included. Both now run in a task of their
  own and an exception is logged. Bunny dispatches cancel notifications to its consumer work pool
  and the Java client to its work service, for the same reasons. `on_error` also runs after the
  close has released the fibers it stranded, not before.
- The log line for a broker-sent cancel names the queue, not just the consumer tag.

### Added

- `recover_cancelled_consumers:` on `Session.new` and `Session#open_channel`, with
  `Channel#recover_cancelled_consumers=` and `Channel#recovers_cancelled_consumers?`. Off by
  default, as it is in Bunny. When on, a consumer the broker cancels is re-subscribed under the
  same tag, backing off from 1s to 30s for as long as the queue exists and the channel is open.
  Recovery declares nothing: it passively checks that the queue is still there and re-consumes, so
  a queue somebody deleted is never handed back. Re-declaring is the application's call.
- `consumer.recovered` and `consumer.recovery_failed` events. `consumer.recovery_failed` carries
  `reason: :queue_deleted` - emitted whether or not recovery is enabled, since that is the point
  at which a consumer is known not to be coming back - or `reason: :error` with the exception, once
  per episode rather than once per attempt.

## [0.7.1] - 2026-10-18

### Added

- `AsyncRabbitMQ::DEFAULT_CONTENT_TYPE` and `AsyncRabbitMQ::Channel::DEFAULT_CONTENT_TYPE` constants (`"application/octet-stream"`).

### Changed

- Default publish properties aligned with Bunny parity: `content_type` defaults to `"application/octet-stream"`, `persistent` defaults to `true` (`delivery_mode: 2`), and `priority` defaults to `0`.

## [0.7.0] - 2026-10-17

### Fixed

- A consumer's `on_error:` survives re-registration after a reconnect or a channel reopen; it no
  longer falls back to the channel default.
- A consumer's `exclusive:` flag and `arguments:` (such as `x-priority`) survive re-registration
  after a reconnect or a channel reopen; they no longer come back as the defaults.

### Added

- `Channel#each` passes its remaining keywords to `basic_consume`, so `on_error:`, `exclusive:`
  and `arguments:` work there as they do on the other two ways to start a consumer.

## [0.6.0] - 2026-10-11

### Fixed

- A consumer handler raising outside `StandardError` no longer stops the channel dispatching.
  `SystemStackError` and `ScriptError` now fail that delivery through the configured error
  disposition. `Async::Stop`, `SystemExit`, `SignalException` and `NoMemoryError` still propagate.
  Applies equally to the `on_handler_error` hook and to settling after an error.
- Handler workers are created on demand instead of one per prefetch slot. `basic_qos(prefetch_count: 500)`
  previously created 500 fibers and retired all but `pool_size` of them unused: 12.6 MB at prefetch
  500 across two channels. Concurrency is unchanged.

### Added

- `on_error:` on a consumer, and `Channel#on_error_disposition` for the channel default, choose what
  happens to a delivery whose handler raised:
  - `:dead_letter` (default) — `reject(requeue: false)`: one attempt, then the dead-letter route.
  - `:retry` — `reject(requeue: true)`: counts towards `x-delivery-limit`.
  - `:release` — `nack(requeue: true)`: requeued without counting towards the limit.
  - `:leave` — settles nothing; the handler is responsible.
- `Channel#on_handler_error` receives `(error, delivery_info, queue_name)` and may return a
  disposition to override the default for that delivery.

## [0.5.0] - 2026-10-09

### Fixed

- **A consumer backlog no longer costs a fiber per queued message.** The dispatch loop started an
  `Async::Task` per delivery and took the pool semaphore from inside it, so `pool_size` capped how
  many handlers *ran*, not how many *existed*. A parked task measured ~15.7 KB (the fiber object
  plus its touched stack pages) against ~48 bytes for a queue entry, and 20,000 backlogged messages
  cost 315 MB of RSS. Handlers now run in `pool_size` long-lived worker fibers draining a queue of
  deliveries — the same shape as Bunny's `ConsumerWorkPool`, the .NET client's consumer dispatcher
  and amqp091-go.

  That queue is deliberately unbounded. The dispatch loop is also what delivers this channel's
  confirms, close-ok and returns, so blocking it on a full queue would deadlock a handler that
  publishes and waits for confirms — which is precisely rabbitmq-amqp-java-client#328, where a
  bounded work pool parked the I/O thread in `LinkedBlockingQueue#put`. Memory is therefore bounded
  by prefetch on a manual-ack consumer and unbounded on an auto-ack one, as it is in every client,
  because automatic acknowledgement has no backpressure to offer.
- **The missing-prefetch warning no longer sends auto-ack consumers to a no-op.** RabbitMQ applies
  prefetch only to *unacknowledged* messages, so `basic_qos` does nothing at all on a
  `manual_ack: false` consumer. The warning told you to call it anyway, and then went quiet once you
  had — silence in the one case where the caller most believes the consumer is bounded and it is
  not. It now says which of the two cases you are in, and still warns an auto-ack consumer that has
  set a prefetch.

### Added

- `Channel#backlog` — deliveries handed to the handler workers but not yet picked up, the number
  Bunny reports as `ConsumerWorkPool#backlog`.
- `AsyncRabbitMQ.warn_unbounded_consumers` — a deliberate way to silence the unbounded-consumer
  warning for someone who has read it and accepted the trade, instead of turning the logger down
  and losing every other warning a channel raises (stale delivery tags, broker-cancelled consumers,
  handler exceptions). One process-wide setting, read when a consumer starts; defaults to `true`.

### Changed

- A backlog still queued when the channel goes away is now **finished on a graceful `close`** (as
  Bunny's `ConsumerWorkPool#shutdown` drains) and **discarded when the connection is lost or the
  channel is reopened**, where the broker requeues the same messages and running the queued handlers
  too would process each one twice.
- `Channel#pool_size=` resizes by adding or retiring workers rather than resizing a semaphore.
  Growing takes effect at once; shrinking retires a worker as soon as the queue drains to the
  sentinel — immediately when idle, after the current backlog when busy. A running handler is never
  interrupted.

## [0.4.0] - 2026-10-07

Reliability review against a payments workload. The two delivery-correctness items
(stale acks, empty bodies) are the reason to take this.

### Fixed

- **Acks are no longer applied to the wrong message after a reconnect.** Delivery tags are
  scoped to a channel on a connection: the broker restarts numbering at 1 when a channel is
  reopened. A handler still running when the connection dropped would ack that number against
  the new channel, acknowledging a message it never processed (a whole range of them with
  `multiple: true`), or draw a 406 that closed the channel and took its consumers with it.
  Deliveries now carry a `VersionedDeliveryTag`, and `basic_ack` / `basic_nack` / `basic_reject`
  drop a tag from an earlier generation and return `false`.
- **A message with an empty body no longer hangs the channel.** Neither RabbitMQ nor
  amq-protocol sends a body frame when the body is zero bytes, so the delivery never completed
  and the next method frame on that channel was taken as its content — a handler called with
  garbage and a lost delivery. Zero-length content is now routed on its header.
- **A half-dead connection is detected in heartbeat time rather than in TCP retransmit time.**
  The heartbeat task wrote before it checked liveness, and that write needs the socket lock, so
  a writer parked in `@socket.write` after a partition blocked the heartbeat too and nothing
  noticed for around 15 minutes. Liveness is checked first, the write is bounded, and
  `TCP_USER_TIMEOUT` is set where the platform has it.
- **TLS connections no longer strand a TCP socket per reconnect.** `sync_close` was never set,
  so closing the SSL socket left its transport open until the GC ran.
- **A second connection drop during recovery no longer closes channels permanently.**
  `@recovery_in_progress` was cleared before channels were reopened, so a drop in that window
  started a competing recovery while the first was still failing channels through `mark_closed!`.
  Channels are now left `:recovering` and reopened by the next attempt. A `frame_io` that a later
  attempt has replaced no longer reports its dying socket as the live connection's.
- **A consumer block that raises no longer stalls the consumer.** The delivery stayed unacked
  for the life of the connection, and once `prefetch` messages were stuck that way nothing more
  arrived. Handlers are wrapped: the delivery is nacked (`requeue: false`, so a poison message
  dead-letters instead of looping) and `Channel#on_handler_error` is called.
- **An RPC timeout no longer leaves the channel out of step.** AMQP replies carry nothing to
  match them to a request, so a reply that arrived after the caller gave up went to whoever
  asked next. The channel is closed and reopened, the late reply discarded, its unconfirmed
  publishes replayed (which is also what resets the client's confirm tag counter to match the
  broker, since it restarts at 1 on the reopened channel), and its consumers re-registered.
  The channel reports `resyncing?` for the duration and parks other fibers, so nothing publishes
  onto a channel that is closing — the broker would drop it silently — and no other request has
  its reply discarded. Confirms that arrive during the window are still applied.
- **A handler that acks and then raises no longer closes the channel.** The automatic nack was
  unconditional, so a delivery the handler had already settled was nacked a second time: a 406
  that closed the channel and took its consumers with it. Settled deliveries are tracked and
  skipped.
- **Background loops survive the task that started them.** Channel dispatch, the heartbeat, the
  channel-0 monitor, the frame reader and writer, the recovery loop and every `Cluster` node
  supervisor were children of whichever task opened the channel or called `connect`. A
  short-lived caller took them down with it while the session still reported itself open. They
  are parented at the reactor instead.
- **The frame writer starts recovery on a non-IO error** instead of exiting quietly and leaving
  publishers blocked once the write queue filled.
- **Recovery that has to retry tears down the half-built connection first.** A drop while
  channels were being reopened left the session reporting `:open` (so a `Cluster` would place new
  channels on a dead node), left the socket open, and leaked a channel-0 monitor fiber per flap.
- **`Cluster#update_secret` cannot leave nodes on different secrets.** One node raising skipped
  every node after it. The secret is stored everywhere first, each node is then attempted, and
  the failures are raised together as `ClusterError`.
- **OpenTelemetry no longer writes into the caller's headers hash** — a frozen hash raised, and
  one shared between fibers raced. The hash is copied before `traceparent` is injected.
- **Tracking unconfirmed publishes no longer costs a quarter of the send throughput.** Keeping
  the payload and routing for `unconfirmed_messages` built a keyword struct and copied the
  options hash for every confirmed publish, and put it in a second hash alongside
  `@pending_confirms`. Measured against 0.3.0 that cost 22% of publish throughput under simple
  confirms and 33% with `basic_publish_batch`. The routing is now shared by one publish call
  (a batch builds it once), the record is built when `unconfirmed_messages` is read rather than
  when the message is sent, and it lives in the hash that was already there. Back to 0.3.0
  throughput: batch +0.2%, simple confirms within run-to-run spread.
- **`Notifier#publish` iterates a snapshot**, so a subscriber that unsubscribes itself no longer
  causes the next subscriber to be skipped.

### Added

- `Channel#on_handler_error` — called when a consumer block raises, with the exception, the
  `Basic::Deliver` frame and the queue name.
- `Channel#wait_for_confirms(timeout:)` — raises `ConfirmTimeoutError` instead of waiting
  forever for a broker that accepts a publish and never confirms it. Unbounded by default.
- `Channel#unconfirmed_messages` — the publishes the broker has not resolved, as
  `UnconfirmedMessage` records carrying the payload, its routing and the publish options
  (`persistent`, `headers`, `message_id`, `correlation_id`, ...), so they can be republished on
  another connection without silently losing their properties. A `Cluster#on_node_down` block that declares a fourth parameter is
  handed them; under `:drop` that is the only chance to see them.
- `Session.new(tcp_user_timeout:)` — milliseconds, defaulting to twice the heartbeat.
- `Channel#delivery_generation` — how many times the channel has been opened on a connection.
  The broker restarts both delivery tags and publisher confirm tags at 1 on a reopened channel,
  so a tag only identifies a message together with its generation. Deliveries carry theirs on
  the tag itself; confirm tags from `basic_publish` are plain integers, so code keeping its own
  confirm bookkeeping across a reconnect reads this.
- `Channel#resyncing?` — true while the channel is being reopened, after an RPC timeout or
  through `Channel#reopen`. Operations park until it is back, as they do during connection
  recovery, and unconfirmed messages are replayed before it goes `:open` so a new publish
  cannot take a delivery tag that no longer matches its place on the wire.
- `TopologyRegistry#clear_transient`.

### Changed

- **A connection no longer keeps the reactor alive by itself.** The background loops are
  transient tasks now, so a program that calls `connect` and `basic_consume` and then lets its
  main task finish exits immediately instead of running the consumer — silently, with no error.
  This is correct Async behaviour (nothing was waiting), but it is a change from 0.3.0, where
  those tasks were children of the caller and held the reactor open. The caller has to block on
  something it owns:

  ```ruby
  Async do
    session.connect
    ch = session.open_channel
    ch.basic_qos(prefetch_count: 10)
    ch.each("q") { |delivery, header, body| ... }   # blocks until the consumer goes away
  end
  ```

  `Async::Condition#wait`, `sleep`, or anything else that parks the task will do.
- **`delivery_tag` is a `VersionedDeliveryTag`, not an `Integer`.** It converts (`to_int`),
  compares, sorts, hashes and prints as the integer it wraps, so `basic_ack(di.delivery_tag)`,
  comparisons and array membership are unaffected. Code calling `.is_a?(Integer)` on it, or
  serialising it directly, needs `.to_i`.
- **`clear_topology_on_drop:` now clears only what the lost connection owned** — exclusive,
  auto-delete and server-named queues, auto-delete exchanges, and the bindings that referred to
  them. Durable topology is kept, so a node that comes back from an empty data directory still
  has it re-declared. Previously the whole registry for that node was dropped.
- `basic_ack`, `basic_nack` and `basic_reject` return `true` when sent and `false` when the tag
  was stale, where they previously returned the write's result.
- `Channel#basic_consume` warns once per channel when called without a prior `basic_qos`: the
  broker sends the whole queue as fast as it can and one task is created per delivery, so memory
  tracks queue depth rather than concurrency.

## [0.3.0] - 2026-09-16

### Added

- `AsyncRabbitMQ::Cluster`: one connection to every node of a cluster behind the Session API,
  for a process with a channel per client. Each new channel opens on the node with the fewest;
  a node that is down is retried until it is back and then takes new channels until the counts
  are level. `on_node_down:` decides what a lost node's channels do: `:park` (wait, as a Session
  does), `:drop` (close them at once so their fibers move to another node) or a callable that
  closes the ones to drop. With `:drop` the lost connection's topology is forgotten
  (`clear_topology_on_drop: true`). `on_node_down` / `on_node_up` callbacks.
- `Session#on_connection_lost`, `Session#channels`, `Session#channel_count`,
  `Session#store_secret`, and a `notifier:` option to share one `Notifier` between sessions.
- `channel.closed` events for a channel given up during recovery: `reason: :dropped`, or `:user`
  when the application closed it.

## [0.2.0] - 2026-09-14

Review against Bunny 3.3 / amq-protocol 2.9 and RabbitMQ 4.3 (issues #21–#42).

### Fixed

- Channel ids are allocated from a bitset and released when a channel closes. They were a bare
  counter: a process opening a channel per unit of work walked it to 65535, after which the id
  wrapped to 0 and the broker dropped the connection. `Session#open_channel` also refuses to
  exceed the negotiated `channel_max` with `ChannelLimitError`, where before the broker closed
  the whole connection with a 530.
- The topology registry tracks consumers and forgets an auto-delete queue, with its bindings,
  when its last consumer goes (cancel, broker-side cancel or channel close), and an auto-delete
  exchange when its last binding goes. Before, a worker using a temporary queue per channel
  accumulated one dead queue per channel and re-declared them all on every reconnect.
- Publishing while the connection is being recovered no longer silently drops the message:
  channels enter a `:recovering` state and park new publishes and requests until they are
  reopened. Messages published under confirms are kept until acked and re-published after a
  reconnect instead of being cleared and reported as confirmed (#21).
- `basic_get` returned the previous message's body from the second call on (#42).
- `AsyncRabbitMQ::Pool.new` raised `NameError`; it now uses the `Async::Pool::Controller`
  constructor API (#22).
- `Session#connect` cleans up after a broker-refused handshake, tries the next address, and
  raises the broker's error; `connection.close` 403 raises `AuthenticationError`. The client
  advertises the standard capabilities (`authentication_failure_close` among them), so bad
  credentials get a 403 instead of a dropped socket; a connection dropped mid-handshake fails
  immediately; a failed connect no longer disables recovery on a reused session (#23).
- `Session#close` hung while the broker had `connection.blocked` active: only publishes are
  gated, and parked publishers are released with `ConnectionError` on close (#24).
- Request/reply operations and publish frames are serialised per channel, so a shared channel
  no longer hands replies to the wrong fiber or interleaves frames under backpressure (#25).
- Heartbeats are sent every T/2 instead of every T (#26).
- `Channel#each` returns when the channel is closed, the consumer is cancelled (by the client
  or the broker) or the broker closes the channel (#27).
- `Session.from_uri` percent-decodes credentials and vhost via `AMQ::URI` (a literal `+` is
  preserved) and honours the standard query parameters (#28).
- `wait_for_confirms` returns `false` when the broker nacked a message; `nacked_tags` and
  `unconfirmed_tags` expose the details (#29).
- The reader loop no longer logs a warning and re-enters recovery when the socket was closed
  locally.
- Closing a session whose write queue has not drained within the 5 s close handshake now logs
  how many queued writes are being discarded, instead of dropping them silently. Publishing
  without confirms is still fire-and-forget; the warning just makes the loss visible
  (`FrameIO#pending_writes` exposes the count).
- A failed TLS handshake no longer leaks the socket it was wrapping, which cost one file
  descriptor per connect attempt against a broker with a bad certificate.
- Unconfirmed messages are re-published after the topology has been replayed, not during
  channel reopen. Against a broker that lost the topology, a message re-published first hit a
  missing exchange (404, closing the channel again) or a missing binding (dropped while the
  broker acked it).
- Prefetch is restored after connection recovery.
- Test suite: RabbitMQ 4.2+ rejects transient non-exclusive queues; specs declare durable or
  exclusive queues (#41).

### Added

- TLS without OpenSSL plumbing: `tls_cert:`, `tls_key:`, `tls_ca_certificates:`, `verify_peer:`
  and `tls_min_version:` on `Session.new`, each taking a path or PEM text, with chains and CA
  bundles split as needed (`AsyncRabbitMQ::TLS`). Certificate material implies `tls: true`.
  `tls_context:` remains for anything the options do not cover.
- An `async-rabbitmq` command: `publish`, `consume`, `inspect` and `purge`, built on the gem's
  own API. Publishing uses confirms and the mandatory flag and reports a nack or an unroutable
  message with a non-zero exit; `consume --peek` prints without acknowledging.
- Optional OpenTelemetry tracing in `async_rabbitmq/telemetry/open_telemetry`, with the span
  names, attributes and W3C context propagation of `opentelemetry-instrumentation-bunny`.
  Not required by default and not a runtime dependency.
- Structured events for metrics and tracing: `Session#on_event(pattern) { |name, payload| }`
  and a `instrumenter:` constructor option. 18 events covering the connection, recovery,
  channels, consumers and messages, with durations on `channel.rpc`, `message.consumed`,
  `connection.open` and `recovery.succeeded`. Nothing is emitted, and no payload is built,
  until something subscribes; a subscriber that raises is logged and skipped.
- Confirm tracking: `confirm_select(tracking: true, outstanding_limit: 1000)` gives publishers
  backpressure and raises `MessageNacked` on a nack (#32).
- `Channel#basic_publish_batch` (#33).
- `Channel#reopen` after a broker-initiated channel close (#34).
- Topology recovery: exchanges, queues and bindings declared through the session are
  re-declared after a reconnect, with server-named queue renames propagated;
  `recover_topology:` and `topology_recovery_filter:` options; `Session#topology` (#35).
- `Session#update_secret` (`connection.update-secret`) (#36).
- Transactions: `tx_select`, `tx_commit`, `tx_rollback`, `using_tx?` (#37).
- `rpc_timeout:` (default 15 s) bounds synchronous channel operations with `RpcTimeoutError`
  (#38).
- `Channel#durable_queue`, `Channel#stream`, `Queue::Types`, `Exchange::TYPE_*` constants;
  `quorum_queue` and `stream` reject empty names (#39).
- `Queue#pop` (alias `get`) (#40).
- `ChannelError#close_method` with `delivery_ack_timeout?`, `unknown_delivery_tag?`,
  `message_too_large?`; `AuthenticationError#code` / `#text` (#30).
- `Session.new` options `connect_timeout:`, `channel_max:`, `rpc_timeout:`,
  `recover_topology:`, `topology_recovery_filter:`; `FrameIO#blocked?`.
- Since 0.1.1, before this review: `Session.from_uri`, multi-host failover (`hosts:`,
  `addresses:`, `hosts_shuffle_strategy:`), `auto_recover:` and configurable recovery with
  `on_recovery_attempt` / `on_recovery` / `on_recovery_exhausted`, `on_blocked` /
  `on_unblocked` / `Channel#on_error`, client properties and `connection_name:`, pluggable
  SASL mechanisms with `connection.secure` handling, per-channel consumer `pool_size`, the
  Bunny convenience API (`direct` / `fanout` / `topic` / `headers` / `default_exchange`,
  `temporary_queue`, `quorum_queue`, `Queue#status`, predicates, `with_channel`,
  `queue_exists?` / `exchange_exists?`, named message properties, `Exchange#on_return`).

### Changed

- No dependency on the `logger` gem, which stopped being a default gem in Ruby 4.0 and would
  otherwise have to be installed by every application using this one. `logger:` takes any
  object responding to `debug`, `info`, `warn` and `error`; the default is now
  `AsyncRabbitMQ::Log`, which writes warnings and errors to `$stderr` instead of the previous
  default of everything, including debug, to `$stdout`. `AsyncRabbitMQ::Log.silent` says
  nothing.
- `AsyncRabbitMQ::Pool` explains that `async-pool` has to be in your Gemfile instead of
  failing with `cannot load such file`.
- `basic_get` defaults to `manual_ack: true`, as in Bunny (#40).
- `amq-protocol` requirement raised to `~> 2.9` (#30).
- Requires Ruby 3.4 or later; CI runs Ruby 3.4 and 4.0.
- A message's frames are written as one buffer.

## [0.1.1] - 2026-04-02

### Added

- Expose `passive:` keyword argument on `Channel#queue` and `Channel#exchange` for defensive
  startup checks that assert a resource exists without creating it (AMQP 0-9-1 passive declare).
- Integration tests for passive declare (exists and 404 paths) for both queues and exchanges.
