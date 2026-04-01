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

### SPEC GAP: Server-initiated channel.flow not handled
**File:** `lib/async_rabbitmq/channel.rb:425-431` (else clause)
**What:** The broker can send `channel.flow(false)` to throttle the client. The current
implementation only handles client→server direction. An inbound `Channel::Flow` falls into
the `else` branch and (incorrectly) signals `@reply_condition`.
**Fix:** Add `when AMQ::Protocol::Channel::Flow` case that sets `@flow_active = method.active`
and sends back `Channel::FlowOk` — same as the client-initiated path but reversed.
**Effort:** XS

---

### SPEC GAP: @confirm_condition not re-created on recovery
**File:** `lib/async_rabbitmq/channel.rb:258-266`, `channel.rb:334-348`
**What:** `@confirm_condition` is created once in `confirm_select` and reused across
reconnects. `reopen_after_recovery` re-enables confirms on the new connection but does not
reset `@confirm_condition`. A waiter holding a reference to the old condition object and
new acks signalling the same condition can cause subtle ordering bugs.
**Fix:** In `reopen_after_recovery`, after re-sending `Confirm::Select`, reset
`@confirm_condition = Async::Condition.new` and clear `@pending_confirms`.
**Effort:** XS

---

## P2 — v2 Work (after v1 stable)

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
