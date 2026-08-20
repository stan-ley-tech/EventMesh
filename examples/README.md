# Consumer group demo

This walks through the scenario EventMesh is built around: a producer,
a topic with three partitions, a consumer group with three workers -
one per partition - and then four things that break a naive event bus
but shouldn't break this one: a killed worker, a duplicate publish, a
replay, and a schema change.

Run everything from `examples/` after `pip install -e ./broker[dev]`
and `pip install -e ./sdk` from the repo root. Use `127.0.0.1`, not
`localhost`, in every URL - on Windows, Python's `urllib` resolves
`localhost` through a slow IPv6-then-IPv4 fallback that adds a couple
of seconds to every request, which is confusing to debug if you don't
know to expect it.

## 1. Start the broker

```
cd broker
python -m eventmesh_broker.main
```

Leave it running on `:8000`.

## 2. Register the topic, schema, and group

```
cd examples
python register.py
```

Creates `orders` with 3 partitions, registers schema version 1
(`order_id: str, amount: float`), and creates the `order_processors`
consumer group with short lease timeouts (8s) so the rest of this demo
doesn't require standing around waiting on defaults meant for
production.

## 3. Publish a batch of events

```
python produce.py http://127.0.0.1:8000 9
```

Nine events, keyed `order-0` through `order-8`, land across the three
partitions by key hash.

## 4. Start three workers - one per partition

In three separate terminals:

```
python worker.py http://127.0.0.1:8000 worker-1
python worker.py http://127.0.0.1:8000 worker-2
python worker.py http://127.0.0.1:8000 worker-3
```

Watch them print what they process. With three partitions and three
workers, each ends up owning exactly one - check
`curl http://127.0.0.1:8000/v1/groups/order_processors/stats` and
you'll see one `owner` per partition, `lag: 0` once they've caught up.

## 5. Kill a worker mid-delivery

Publish a few more events (`python produce.py http://127.0.0.1:8000 6`
again picks up from `order-9`), then `kill -9` (or Task Manager / End
Task on Windows) whichever worker is actively printing a "processing"
line - you want it dead while holding a delivery, not between polls.

Within `delivery_lease_seconds` (8s), that event stops belonging to
the dead worker. Within `worker_lease_seconds` (also 8s here), the
broker notices the worker's heartbeat lapsed and reassigns its
partition to one of the two survivors. Check stats again: the killed
worker is gone from `alive_workers`, its old partition now shows a
different `owner`, and the event it was holding gets redelivered -
`curl http://127.0.0.1:8000/v1/events/<event_id>/history` shows a
`NACKED` (reason: lease expired) followed by a second `DELIVERED` at
attempt 2.

## 6. Publish a duplicate

```python
from eventmesh import Client, Producer
from order_events import TOPIC, OrderCreated

client = Client("http://127.0.0.1:8000")
producer = Producer(client, TOPIC)

first = producer.publish(OrderCreated(order_id="order-x", amount=1.0), key="order-x", schema_version=1, dedup_key="order-x-attempt-1")
again = producer.publish(OrderCreated(order_id="order-x", amount=1.0), key="order-x", schema_version=1, dedup_key="order-x-attempt-1")

assert first.id == again.id  # same event, not a duplicate
```

The second publish returns the exact same event - same id, same
offset - because it reused the first one's dedup key. This is what
protects against a producer's own retries (a publish that succeeded
but whose response got lost, say) creating two events for one thing
that happened once.

## 7. Replay from the beginning

```
curl -X POST http://127.0.0.1:8000/v1/groups/order_processors/replay -d '{"earliest": true}'
```

Every event on every partition becomes redeliverable again, in order,
to whichever workers currently own those partitions. Nothing was ever
deleted, so this isn't a special "replay path" - it's the same
delivery mechanism running again from an earlier offset. Watch the
surviving workers' output: everything gets reprocessed, and each
event's `attempt` number in its history shows it's a genuine
redelivery, not a fresh one.

## 8. Evolve the schema

```python
from eventmesh import Client
from order_events import TOPIC, OrderCreatedV2

client = Client("http://127.0.0.1:8000")
client.register_schema(TOPIC, 2, OrderCreatedV2)  # adds currency, with a default - accepted

client.register_schema(TOPIC, 3, {
    "type": "object",
    "properties": {"order_id": {"type": "string"}},
    "required": ["order_id"],
})  # drops the required 'amount' field - rejected
```

Version 2 registers cleanly - it only adds a field with a default, so
anything that depended on version 1's shape still gets everything it
expects. The attempted version 3 gets a 409 back immediately, with the
specific reason: `required property 'amount' was removed`. Publish a
version-2 event afterward and the same workers keep consuming it
without any special handling - a consumer doesn't care which version
an event was written against unless it chooses to.
