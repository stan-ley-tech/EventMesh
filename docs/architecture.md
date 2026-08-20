# Architecture

EventMesh is a durable event bus: producers publish events to topics,
consumer groups read them back with ordering and fault-tolerance
guarantees modeled closely on Kafka's, and the broker persists
everything so replay and audit are just queries against history rather
than special cases.

## Components

- **broker** - the server. A FastAPI app over a SQLite-backed log.
  Owns topics, partitions, the schema registry, consumer group
  membership and offsets, in-flight delivery leases, and event history.
- **sdk** - a Python client library: `Producer` for publishing,
  `ConsumerGroupWorker` for consuming, and `EventSchema` for defining
  and evolving event payloads. Talks to the broker over its REST API;
  it holds no state of its own.

Both are plain Python packages in this one repository - there's no
reason to split languages when the server and the client are both best
written in the same one.

## Topics and partitions

A topic is created with a fixed number of partitions. Each partition is
an independent, strictly-ordered, append-only log - the same shape as a
Kafka partition. An event's partition is chosen by hashing its key
(`hash(key) % partition_count`), so every event with the same key always
lands in the same partition and is therefore always delivered in the
order it was published relative to other events with that key. Keyless
events are spread round-robin. Cross-partition ordering isn't
guaranteed, same tradeoff Kafka makes, for the same reason: it's what
makes partitions independently parallelizable.

Retention is unbounded - nothing is ever deleted from a partition. That
isn't a simplification so much as the thing that makes replay,
audit history, and "start a new consumer group from the beginning"
possible at all without a separate storage tier.

## Consumer groups, partition assignment, and fault tolerance

A consumer group reads a topic. Multiple workers can join the same
group; the broker divides the topic's partitions across whichever
workers in the group are currently alive, and each partition is owned
by exactly one worker at a time. Two workers in the same group never
process the same partition concurrently, which is what makes ordered,
in-order processing per partition possible even though the group as a
whole is parallel.

Ownership is heartbeat-leased, the same mechanism FlowForge (this
project's sibling) uses for step leases: a worker's assignment has a
lease that must be renewed periodically. If a worker stops
heartbeating - killed, hung, disconnected - its lease lapses and the
broker reassigns its partitions across the group's remaining live
workers on the next rebalance sweep. There's no special-cased failover
path; a rebalance triggered by a crash and a rebalance triggered by a
new worker joining run through the same code.

## Delivery, acking, and at-least-once

A worker polls its group for the next event. The broker looks at the
partitions that worker currently owns, finds the oldest unconsumed
event on one of them, and hands it out with a delivery lease (its own
short-lived heartbeat, separate from the partition-ownership lease
above). The worker must ack or nack before that lease expires.

- **Ack** advances the group's committed offset for that partition past
  the event and clears the lease.
- **Nack**, or a lease that simply expires without a response, is
  treated identically: the event's retry policy applies. If attempts
  remain, it becomes redeliverable again after a backoff delay -  the
  same exponential-backoff-with-jitter used throughout. If attempts are
  exhausted, the event is moved to the topic's dead-letter queue and
  *then* the offset advances - one poison message doesn't block its
  partition forever, but it also doesn't advance until the group has
  actually finished with it one way or another.

Because an ack only happens after a worker has both received and
processed an event, and a crash between those two things results in
redelivery rather than silent loss, this is at-least-once delivery:
never fewer than one delivery, occasionally more. Consumers are
expected to be idempotent about the effects of processing an event, not
about receiving it exactly once - the SDK's dedup helper exists for
exactly that reason (see below).

## Deduplication vs. idempotent consumers

These solve different halves of the same problem and EventMesh treats
them as genuinely separate features:

- **Deduplication** happens at publish time. A producer may attach a
  dedup key to an event; if an event with that key already exists on
  the topic, the broker returns the existing event instead of creating
  a second one. This is what stops a producer's own retries (network
  timeout, no ack received, producer retries the publish) from creating
  duplicate events in the first place.
- **Idempotent consumption** is what a consumer does about the
  at-least-once guarantee above: a redelivered event (after a crash, a
  nack, or a replay) must not double-apply its side effect. The SDK
  gives workers a helper that tracks which event IDs have already been
  processed and skips reprocessing side effects for ones it's seen,
  keyed off the event's own ID - which is stable across redeliveries of
  the same event, unlike the dedup key, which is about publish-time
  identity.

## Event replay

Replaying is nothing more than moving a consumer group's committed
offset for a partition backward - to a specific offset, or to the
earliest retained event. Since nothing is ever deleted, this doesn't
require a separate replay mechanism; it's the same delivery path every
other event goes through, just starting from an earlier point. A
replayed event increments its delivery attempt count like any other
redelivery, which is exactly why idempotent consumption matters here
too.

## Schemas and versioning

A topic's schema registry holds one or more versions, each a JSON
Schema (the SDK generates these from a Pydantic `EventSchema` model, but
the broker only ever sees plain JSON Schema - it doesn't know or care
that Pydantic was involved). A publish declares which version it's
written against; the broker validates the payload against that specific
version before accepting it.

Registering a new version is only accepted if it's backward compatible
with the immediately preceding one: it may add new properties (required
or optional), but it may not remove a property that was required
before, may not demote a previously-required property to optional, and
may not change the type of a property that exists in both versions.
This is deliberately one specific, checkable rule rather than a general
compatibility framework - it's exactly the rule that lets an old
consumer's assumptions about a topic keep holding after a new version
starts being published alongside events written against the old one.

## Filtering

A consumer group can be registered with a filter - an exact-match
condition over event headers. The broker applies it when deciding what
counts as "the next event" for that group's partitions: non-matching
events are skipped without ever being handed to a worker, but they
still advance nothing and are recorded as skipped in event history, so
the audit trail stays complete even for events a group never actually
saw.

## Delayed delivery

An event can declare `deliver_after`. Until that time passes, it isn't
eligible to be the "next event" delivered on its partition - which,
given strict per-partition ordering, means it blocks anything published
after it on the same partition until it becomes due. This is an
explicit tradeoff: EventMesh could deliver later-but-not-delayed events
out of order to avoid the block, but that would break the same
per-partition ordering guarantee everything else here is built around.
A delayed event is a deliberate pause in that partition's timeline, not
an exception to its ordering.

## Dead-letter queues

Each topic has one DLQ. An event lands there once its retry policy is
exhausted following a nack or a lease expiry. A dead-lettered event can
be redriven - reinserted at the head of delivery for its group - once
whatever caused it to fail has been fixed, without needing the original
producer to republish anything.

## History and monitoring

Every state an event passes through - published, delivered (to which
worker, which attempt), acked, nacked, retried, dead-lettered, skipped
by a filter, replayed - is appended to that event's history. It's the
same log that answers "what happened to this event" for debugging and
"how far behind is this consumer group" for monitoring; there's no
separate metrics pipeline computing lag or throughput from anything
other than this history and the current offsets.
