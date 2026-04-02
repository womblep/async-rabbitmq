# TODOS — async-rabbitmq

## P0 — Bugs (must fix before v1) — ✅ ALL RESOLVED

### ✅ BUG: re_register_consumers uses empty queue name — recovery is silently broken
**Fixed:** `@consumers` now stores `{queue_name:, block:, manual_ack:}`; `re_register_consumers`
uses the stored `queue_name` and `manual_ack`.

---

### ✅ BUG: wait_for_confirms hangs forever on disconnect
**Fixed:** `interrupt_wait!` now signals and clears `@confirm_condition`.

---

### ✅ BUG: Channel#each always fails — empty queue name
**Fixed:** `#each` now takes a required `queue_name` argument: `def each(queue_name, manual_ack: false, &block)`.

---

### ✅ BUG: @pending_confirms stores dead Async::Condition objects
**Fixed:** `basic_publish` now stores `tag` (the delivery tag integer) instead of
`Async::Condition.new` in `@pending_confirms`.

---

### ✅ BUG: connection.blocked / connection.unblocked not processed
**Fixed:** Session now starts a long-lived `channel0_monitor_loop` task after handshake.
It reads from the channel-0 queue and delegates `Connection::Blocked` / `Connection::Unblocked`
to new `FrameIO#set_blocked` / `#set_unblocked` methods. `write_frame` yields on a
condition while `@blocked` is true.

---

### ✅ BUG: Server-initiated basic.cancel not handled
**Fixed:** Added `when AMQ::Protocol::Basic::Cancel` in `handle_method`. Removes the consumer
from `@consumers` and invokes an optional `on_cancel` callback registered via `Channel#on_cancel`.

---

### ✅ BUG: Soft error codes 311/312/313 misclassified as hard errors
**Fixed:** `SOFT_ERROR_CODES` expanded to `[311, 312, 313, 403, 404, 405, 406]`.

---

## P1 — Spec Compliance Gaps (v1 scope)

### ✅ SPEC GAP: Server-initiated channel.flow not handled
**Fixed:** Added `when AMQ::Protocol::Channel::Flow` in `handle_method`. Sets `@flow_active`
and immediately sends back `Channel::FlowOk`.

---

### ✅ SPEC GAP: @confirm_condition not re-created on recovery
**Fixed:** `reopen_after_recovery` now resets `@confirm_condition`, `@pending_confirms`, and
`@delivery_tag` after successfully re-enabling confirms on the new connection.

---

### IMPL GAP: basic.recover not implemented
**File:** `lib/async_rabbitmq/channel.rb` (method missing entirely)
**What:** The AMQP 0-9-1 spec defines `basic.recover(requeue)` → `basic.recover-ok`. It tells
the broker to redeliver all unacknowledged messages on the channel. RabbitMQ supports
`requeue: true`; `requeue: false` is rejected with a channel error.
**Fix:** Add `Channel#basic_recover(requeue: true)` that sends `Basic::Recover.encode` and
waits for `Basic::RecoverOk`. Document that `requeue: false` raises `ChannelError` on RabbitMQ.
**Effort:** XS

---

### ~~IMPL GAP: queue.declare passive flag not exposed~~ DONE
**File:** `lib/async_rabbitmq/channel.rb` — `Channel#queue`
**What:** The spec defines `queue.declare(passive: true)` as a way to assert a queue exists
without creating it. If the queue does not exist the broker raises 404. This is a standard
pattern for defensive startup checks. The `passive:` keyword is not forwarded in the current
`queue` method.
**Fix:** Add `passive: false` keyword to `Channel#queue` and forward it to
`AMQ::Protocol::Queue::Declare.encode`.
**Effort:** XS

---

### ~~IMPL GAP: exchange.declare passive flag not exposed~~ DONE
**File:** `lib/async_rabbitmq/channel.rb` — `Channel#exchange`
**What:** Same pattern as queue passive — `exchange.declare(passive: true)` asserts existence
without creating the exchange. The `passive:` keyword is missing from `Channel#exchange`.
**Fix:** Add `passive: false` keyword to `Channel#exchange` and forward it to
`AMQ::Protocol::Exchange::Declare.encode`.
**Effort:** XS

---

### IMPL GAP: server-generated queue name undocumented / untested
**File:** `lib/async_rabbitmq/channel.rb` — `Channel#queue`
**What:** Passing an empty string as the queue name causes the broker to generate a unique name
and return it in `DeclareOk`. This works today but is untested and not mentioned in any doc or
comment. Easy to overlook.
**Fix:** Add an integration test in `11_queue_operations_spec.rb` verifying that
`ch.queue("")` returns a queue whose name is non-empty. Add a code comment in `Channel#queue`
noting this behaviour.
**Effort:** XS

---

## P1 — Test Coverage Gaps (v1 scope, code exists but no test)

### TEST GAP: Server-initiated basic.cancel not tested
**File:** `spec/integration/13_consumer_spec.rb`
**What:** The `when AMQ::Protocol::Basic::Cancel` handler was added in the P0-6 fix but there
is no integration test that exercises it. Without a test, a future refactor could silently
break consumer cleanup on HA failover or queue deletion.
**How to test:** Declare an auto-delete queue, start a consumer, then delete the queue via a
second channel → broker sends `basic.cancel` to the first channel. Assert the consumer tag is
removed from `@consumers` and the `on_cancel` callback fires.
**Effort:** XS

---

### TEST GAP: on_cancel callback not tested
**File:** `spec/integration/13_consumer_spec.rb`
**What:** `Channel#on_cancel` is new public API (P0-6) with no test.
**How to test:** Register `ch.on_cancel { |tag| ... }`, trigger a server-initiated cancel (see
above), verify the block was called with the correct consumer tag.
**Effort:** XS

---

### TEST GAP: write_frame blocking while connection.blocked not tested
**File:** `spec/integration/07_connection_blocked_spec.rb`
**What:** The `07` spec pushes frames directly into the channel-0 queue (logs a warning) but
never verifies that `write_frame` actually suspends while blocked and resumes after unblocked.
The actual backpressure gate in `FrameIO#write_frame` is untested.
**How to test:** Call `frame_io.set_blocked("test")`, spawn a fiber that calls
`frame_io.write_frame(some_data)`, assert it hasn't returned after a small yield, then call
`frame_io.set_unblocked` and assert the fiber completes.
**Effort:** XS

---

### TEST GAP: wait_for_confirms raises ConnectionError on disconnect
**File:** `spec/integration/15_publisher_confirms_spec.rb`
**What:** The P0-2 fix ensures `interrupt_wait!` signals `@confirm_condition`, but there is no
test that kills the connection while a fiber is blocked in `wait_for_confirms` and verifies it
raises `ConnectionError` rather than hanging.
**How to test:** Enable confirms, publish a message to a slow queue, call `wait_for_confirms`
concurrently in a fiber, kill the socket, assert the fiber raises `ConnectionError`.
**Effort:** S

---

### TEST GAP: publisher confirms work correctly after recovery
**File:** `spec/integration/18_coverage_spec.rb`
**What:** The existing recovery + confirms test only checks `session.open?` after reconnect. It
does not verify that `wait_for_confirms` actually succeeds for a publish made after recovery —
i.e. that `@confirm_condition`, `@pending_confirms`, and `@delivery_tag` were correctly reset
by the P1-2 fix.
**How to test:** After the socket-kill + sleep, call `ch.basic_publish` then `ch.wait_for_confirms`
and assert it returns `true`.
**Effort:** XS

---

### TEST GAP: server-initiated channel.flow not tested
**File:** `spec/integration/09_channel_flow_spec.rb`
**What:** The P1-1 fix handles `Channel::Flow` arriving from the broker but the spec only tests
the client→server direction (`ch.flow(false/true)`). Server→client flow is untested.
**How to test:** Inject a `Channel::Flow(active: false)` frame into the channel's dispatch queue
and verify `channel.instance_variable_get(:@flow_active)` becomes `false` and a `FlowOk` frame
was written.
**Effort:** XS

---

### TEST GAP: soft error codes 311/312/313 raise ChannelError not ConnectionError
**File:** `spec/integration/06_channel_errors_spec.rb`
**What:** The P0-7 fix added 311/312/313 to `SOFT_ERROR_CODES` but there is no test that
actually triggers one of these codes to confirm the classification.
**How to test:** Code 312 (no-route) can be triggered by publishing with `mandatory: true` to
a direct exchange that has no matching queue binding, then registering `on_return`. The channel
must stay open (soft error); if it closes, the fix is broken.
Alternatively: code 313 (no-consumers) if RabbitMQ emits it. Check which codes RabbitMQ 4.x
actually sends and add targeted cases.
**Effort:** S

---

### TEST GAP: server-generated queue name (queue(""))
**File:** `spec/integration/11_queue_operations_spec.rb`
**What:** Passing an empty name to `ch.queue("")` causes the broker to generate a unique name.
This is a valid AMQP 0-9-1 use-case (spec §3.1.2) and works today, but has no test.
**How to test:** `q = ch.queue(""); expect(q.name).not_to be_empty`.
**Effort:** XS

---

## P2 — v2 Work (after v1 stable)

### IMPL GAP: connection.secure / connection.secure-ok not handled
**File:** `lib/async_rabbitmq/session.rb` — `handshake`
**What:** The AMQP 0-9-1 handshake allows the broker to send `connection.secure` (SASL
challenge) between `connection.start-ok` and `connection.tune`. The current `wait_channel0_method`
loop skips unknown method types with `next`, so a `connection.secure` frame would cause the
handshake to loop indefinitely. Standard RabbitMQ with PLAIN auth never sends this, but
brokers using DIGEST-MD5 or GSSAPI would hang.
**Fix:** In `handshake`, after sending `start-ok`, loop handling `connection.secure` (log a
warning and send `connection.secure-ok` with empty response) until `connection.tune` arrives.
**Effort:** XS
**Depends on:** v1 stable (low urgency — PLAIN auth covers all current RabbitMQ deployments)

---

### IMPL GAP: Transactions (tx.select / tx.commit / tx.rollback) not implemented
**File:** new `lib/async_rabbitmq/channel.rb` methods
**What:** The AMQP 0-9-1 `tx` class is marked `ok` in the RabbitMQ 4.2 spec. Publisher
confirms are the recommended alternative, but some legacy workloads require transactions.
**Fix:** Add `Channel#tx_select`, `#tx_commit`, `#tx_rollback` — straightforward send +
wait_for_ok pattern. Add a `19_transactions_spec.rb` integration spec.
**Effort:** S
**Depends on:** v1 stable API

---

### Metrics / Instrumentation Hooks
**What:** Expose structured events (connection open/close, publish, consume, heartbeat,
reconnect) via a notification system (dry-monitor or AS::Notifications style).
**Why:** Makes the gem production-observable. Teams running in Falcon can hook into APM.
Zero cost when unsubscribed.
**Context:** v1 uses standard Logger only. Instrumentation hooks require a stable event
taxonomy, which should be defined after the v1 API stabilizes.
**Effort:** S (human: 2 days / CC+gstack: ~15 min)
**Depends on:** v1 stable API

---

### Bunny Compat Shim
**What:** `AsyncRabbitMQ::Bunny` module wrapping the native API with Bunny-compatible
method names. Separate require: `require 'async_rabbitmq/bunny'`. Includes curated
integration tests (one per in-scope Bunny method). Callbacks wrapped in Async::Task
with documented execution model change (no longer serial — concurrent deliveries).
**Why:** Migration path for existing Bunny users. Enables adoption by teams who can't
rewrite immediately.
**Context:** Deferred from v1 because the only v1 adopters are Async ecosystem users
who don't need the shim. Bunny users need a tested shim, not an untested one.
Shipping without tests was flagged as "theater" in the CEO review outside voice.
**In-scope Bunny surface:** Session#start/close, Channel#queue/exchange/basic_publish/
basic_get/ack/nack/reject/qos/confirm_select/wait_for_confirms, Queue#subscribe/
publish/bind/unbind/delete/purge, Exchange#publish/delete/bind.
**Effort:** M (human: 1 week / CC+gstack: ~45 min)
**Depends on:** v1 stable API

---

### Topology Re-Declaration on Recovery
**What:** `session.on_recovery { |session| ... }` callback that fires after each
successful reconnect. Callers use it to re-declare non-durable queues and exchanges.
**Why:** Without this, auto-recovery silently fails for non-durable topology after a
broker restart. v1 documents "caller must re-declare" but this is awkward to implement
correctly in user code.
**Context:** v1 recovery re-registers consumers but cannot know the topology the
application set up. A recovery callback is the clean solution.
**Effort:** S (human: 1 day / CC+gstack: ~15 min)
**Depends on:** v1 recovery implementation

---

### Metrics / Instrumentation Hooks
**What:** Expose structured events (connection open/close, publish, consume, heartbeat,
reconnect) via a notification system (dry-monitor or AS::Notifications style).
**Why:** Makes the gem production-observable. Teams running in Falcon can hook into APM.
Zero cost when unsubscribed.
**Context:** v1 uses standard Logger only. Instrumentation hooks require a stable event
taxonomy, which should be defined after the v1 API stabilizes.
**Effort:** S (human: 2 days / CC+gstack: ~15 min)
**Depends on:** v1 stable API

---

### Bunny Compat Shim
**What:** `AsyncRabbitMQ::Bunny` module wrapping the native API with Bunny-compatible
method names. Separate require: `require 'async_rabbitmq/bunny'`. Includes curated
integration tests (one per in-scope Bunny method). Callbacks wrapped in Async::Task
with documented execution model change (no longer serial — concurrent deliveries).
**Why:** Migration path for existing Bunny users. Enables adoption by teams who can't
rewrite immediately.
**Context:** Deferred from v1 because the only v1 adopters are Async ecosystem users
who don't need the shim. Bunny users need a tested shim, not an untested one.
Shipping without tests was flagged as "theater" in the CEO review outside voice.
**In-scope Bunny surface:** Session#start/close, Channel#queue/exchange/basic_publish/
basic_get/ack/nack/reject/qos/confirm_select/wait_for_confirms, Queue#subscribe/
publish/bind/unbind/delete/purge, Exchange#publish/delete/bind.
**Effort:** M (human: 1 week / CC+gstack: ~45 min)
**Depends on:** v1 stable API

---

### Topology Re-Declaration on Recovery
**What:** `session.on_recovery { |session| ... }` callback that fires after each
successful reconnect. Callers use it to re-declare non-durable queues and exchanges.
**Why:** Without this, auto-recovery silently fails for non-durable topology after a
broker restart. v1 documents "caller must re-declare" but this is awkward to implement
correctly in user code.
**Context:** v1 recovery re-registers consumers but cannot know the topology the
application set up. A recovery callback is the clean solution.
**Effort:** S (human: 1 day / CC+gstack: ~15 min)
**Depends on:** v1 recovery implementation

---

## P3 — Future

### CLI Dev Tools
**What:** `rabbitmq-async` CLI binary. Commands: `consume <queue>`, `publish <queue>
<payload>`, `inspect <queue>` (depth + consumer count), `purge <queue>`.
Self-implements on top of the gem — a real dogfooding exercise.
**Why:** Useful for development and debugging. Makes the gem tangible for new users
who want to kick the tires without writing Ruby.
**Effort:** S (human: 1 day / CC+gstack: ~15 min)
**Depends on:** stable v1 API

---

## Resolved Decisions

1. **Gem name:** `async-rabbitmq`
2. **Minimum Ruby version:** 3.2
