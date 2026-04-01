# TODOS — async-rabbitmq

## P0 — Bugs (must fix before v1)

### BUG: re_register_consumers uses empty queue name — recovery is silently broken
**File:** `lib/async_rabbitmq/channel.rb:522-525`
**What:** `re_register_consumers` calls `basic_consume("", ...)` — always passes an empty queue
name because the channel only stores `consumer_tag → block`, never the queue name. Every
recovery attempt will get a 404 NOT_FOUND from the broker and silently skip all consumers.
**Fix:** Store `consumer_tag → { queue_name:, block:, manual_ack: }` in `@consumers` in
`basic_consume`, then use the stored queue name in `re_register_consumers`.
**Effort:** XS

---

### BUG: wait_for_confirms hangs forever on disconnect
**File:** `lib/async_rabbitmq/channel.rb:270-277`, `channel.rb:319-328`
**What:** `interrupt_wait!` signals `@reply_condition` and `@content_condition` but never
signals `@confirm_condition`. A fiber blocked in `wait_for_confirms` when the connection
drops will hang indefinitely instead of raising `ConnectionError` as the design doc specifies.
**Fix:** In `interrupt_wait!`, call `@confirm_condition&.signal(error)` and reset it to nil.
**Effort:** XS

---

### BUG: Channel#each always fails — empty queue name
**File:** `lib/async_rabbitmq/channel.rb:306-312`
**What:** `Channel#each` calls `basic_consume("")` with an empty queue name. This is the
duck-typed stream interface advertised as a core v1 feature. It will always 404.
**Fix:** `#each` must take a queue name argument (e.g. `def each(queue_name, ...)`) or be
redesigned. Update design doc API surface accordingly.
**Effort:** XS

---

### BUG: @pending_confirms stores dead Async::Condition objects
**File:** `lib/async_rabbitmq/channel.rb:174-180`
**What:** `basic_publish` stores `@pending_confirms[tag] = Async::Condition.new` but these
per-tag conditions are never waited on or signalled. `wait_for_confirms` only checks
`@pending_confirms.empty?`. The conditions accumulate in memory until the hash is cleared
and serve no purpose.
**Fix:** Either remove the `Async::Condition` (just store `true` or the tag itself) or
implement per-tag waiting properly. For now, storing the tag is sufficient.
**Effort:** XS

---

### BUG: connection.blocked / connection.unblocked not processed
**File:** `lib/async_rabbitmq/channel.rb:418-426`, `lib/async_rabbitmq/frame_io.rb`
**What:** `connection.blocked` and `connection.unblocked` arrive on channel 0. Channel 0 has
no `Channel` object and no dispatch task — `wait_channel0_method` only runs during handshake
and close. These frames are routed to the channel-0 queue but never consumed. The design
doc requires publish to yield when blocked and resume when unblocked. Currently neither
happens. Test 07 passes only because it manually injects into the channel-0 queue; nothing
reads that queue during normal operation.
**Fix:** Dedicate a long-lived task to drain channel-0 frames after handshake completes.
Handle `Connection::Blocked` by setting a `@blocked` flag that causes `write_frame` to
yield until `Connection::Unblocked` clears it.
**Effort:** S

---

### BUG: Server-initiated basic.cancel not handled
**File:** `lib/async_rabbitmq/channel.rb:425-431` (else clause)
**What:** RabbitMQ sends `basic.cancel` server→client when a queue is deleted or during HA
failover. This lands in `handle_method`'s `else` branch and signals `@reply_condition`. If
a fiber is waiting in `wait_for`, it raises `ChannelError("Expected X but got Basic::Cancel")`.
If no fiber is waiting, the frame is silently dropped. Either way, the cancelled consumer
remains in `@consumers` pointing at a dead tag.
**Fix:** Add an explicit `when AMQ::Protocol::Basic::Cancel` case in `handle_method` that
removes the consumer from `@consumers` and optionally calls a user-supplied `on_cancel`
callback.
**Effort:** S

---

### BUG: Soft error codes 311/312/313 misclassified as hard errors
**File:** `lib/async_rabbitmq/frame_io.rb:26`
**What:** `SOFT_ERROR_CODES = [403, 404, 405, 406]` is incomplete. The AMQP spec defines
three additional channel-level soft errors: 311 (content-too-large), 312 (no-route), and
313 (no-consumers). If the broker closes a channel with one of these codes, the code
misclassifies it as a hard error and triggers full session recovery unnecessarily.
**Fix:** Add 311, 312, 313 to `SOFT_ERROR_CODES`.
**Effort:** XS

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
