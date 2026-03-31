# TODOS — async-rabbitmq

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
