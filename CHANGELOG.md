# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - Unreleased

Review against Bunny 3.3 / amq-protocol 2.9 and RabbitMQ 4.3 (issues #21–#42).

### Fixed

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

- `basic_get` defaults to `manual_ack: true`, as in Bunny (#40).
- `amq-protocol` requirement raised to `~> 2.9` (#30).
- Requires Ruby 3.4 or later; CI runs Ruby 3.4 and 4.0.
- A message's frames are written as one buffer.

## [0.1.1] - 2026-04-02

### Added

- Expose `passive:` keyword argument on `Channel#queue` and `Channel#exchange` for defensive
  startup checks that assert a resource exists without creating it (AMQP 0-9-1 passive declare).
- Integration tests for passive declare (exists and 404 paths) for both queues and exchanges.
