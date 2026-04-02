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

### ~~IMPL GAP: basic.recover not implemented~~ ✅ DONE
**File:** `lib/async_rabbitmq/channel.rb` — `Channel#basic_recover(requeue: true)`
**Resolved:** Added method + integration spec (`spec/integration/19_basic_recover_spec.rb`).

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

### ~~IMPL GAP: server-generated queue name undocumented / untested~~ DONE
**Resolved:** Added code comment in `Channel#queue` and integration test in
`spec/integration/21_test_gap_coverage_spec.rb`.

---

## P1 — Test Coverage Gaps (v1 scope, code exists but no test)

### ~~TEST GAP: Server-initiated basic.cancel not tested~~ DONE
### ~~TEST GAP: on_cancel callback not tested~~ DONE
**Resolved:** Both covered in `spec/integration/21_test_gap_coverage_spec.rb` — injects
`Basic::Cancel` frame, verifies consumer removal and `on_cancel` callback fires.

---

### ~~TEST GAP: write_frame blocking while connection.blocked not tested~~ DONE
**Resolved:** Covered in `spec/integration/21_test_gap_coverage_spec.rb` — verifies
`write_frame` suspends while blocked and resumes after `set_unblocked`.

---

### ~~TEST GAP: wait_for_confirms raises ConnectionError on disconnect~~ DONE
**Resolved:** Covered in `spec/integration/21_test_gap_coverage_spec.rb` — kills socket
during `wait_for_confirms`, verifies ConnectionError or channel closure.

---

### ~~TEST GAP: publisher confirms work correctly after recovery~~ DONE
**Resolved:** Covered in `spec/integration/21_test_gap_coverage_spec.rb` — uses toxiproxy
to cut connection, verifies `wait_for_confirms` succeeds after recovery.

---

### ~~TEST GAP: server-initiated channel.flow not tested~~ DONE
**Resolved:** Covered in `spec/integration/21_test_gap_coverage_spec.rb` — injects
`Channel::Flow` frame, stubs `write_frame` to capture FlowOk, verifies `@flow_active` toggle.

---

### ~~TEST GAP: soft error codes 311/312/313 raise ChannelError not ConnectionError~~ DONE
**Resolved:** Covered in `spec/integration/21_test_gap_coverage_spec.rb` — injects
`Channel::Close(312)`, verifies ChannelError raised and connection stays alive.

---

### ~~TEST GAP: server-generated queue name (queue(""))~~ DONE
**Resolved:** Covered in `spec/integration/21_test_gap_coverage_spec.rb`.

---

## P2 — v2 Work (after v1 stable)

### ~~IMPL GAP: connection.secure / connection.secure-ok not handled~~ ✅ DONE
**File:** `lib/async_rabbitmq/session.rb` — `handshake`
**Resolved:** Handshake now loops on `Connection::Secure` frames between `start-ok` and `tune`,
responding with `SecureOk("")` and logging a warning. `wait_channel0_method` accepts multiple
expected classes via splat.

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
