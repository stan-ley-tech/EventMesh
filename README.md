# EventMesh

EventMesh is a durable event bus: producers publish events to topics,
consumer groups read them back with ordering, retries, and fault
tolerance modeled on Kafka's consumer-group semantics. Kill a worker
mid-delivery and its partitions get reassigned to the survivors. Publish
the same event twice and the broker dedupes it. Replay is just moving an
offset backward, because nothing is ever deleted. Evolve a schema and
old and new events keep validating against the version they were
actually written for.

It's a single-language project by design - both halves are Python:

- **`broker/`** - the server: FastAPI over a SQLite-backed log, owning
  topics, partitions, the schema registry, consumer group membership and
  offsets, delivery leases, and event history.
- **`sdk/`** - the client library: `Producer`, `ConsumerGroupWorker`, and
  `EventSchema`, talking to the broker over its REST API.

See [docs/architecture.md](docs/architecture.md) for how delivery,
partition assignment, replay, and schema compatibility actually work.

## Running the broker

```
cd broker
pip install -e ".[dev]"
python -m eventmesh_broker.main
```

Listens on `:8000` by default, persisting to `eventmesh.db`. Both
overridable, along with the background sweep interval that reclaims
expired delivery leases:

| Variable | Default | Purpose |
|---|---|---|
| `EVENTMESH_HOST` | `0.0.0.0` | HTTP bind address |
| `EVENTMESH_PORT` | `8000` | HTTP port |
| `EVENTMESH_DB_PATH` | `eventmesh.db` | SQLite file |
| `EVENTMESH_SWEEP_INTERVAL_SECONDS` | `1.0` | how often expired delivery leases are reclaimed |

Driven by hand:

```
curl -X POST 127.0.0.1:8000/v1/topics -d '{"name": "orders", "partition_count": 2}'
curl -X POST 127.0.0.1:8000/v1/topics/orders/schemas -d '{
  "version": 1,
  "json_schema": {"type": "object", "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}}, "required": ["order_id", "amount"]}
}'
curl -X POST 127.0.0.1:8000/v1/groups -d '{"name": "processors", "topic": "orders"}'

curl -X POST 127.0.0.1:8000/v1/topics/orders/events -d '{"payload": {"order_id": "o1", "amount": 42}, "schema_version": 1, "key": "o1"}'
curl -X POST 127.0.0.1:8000/v1/groups/processors/poll -d '{"worker_id": "worker-1"}'
curl -X POST 127.0.0.1:8000/v1/deliveries/<delivery_id>/ack -d '{"worker_id": "worker-1"}'

curl 127.0.0.1:8000/v1/events/<event_id>/history
curl 127.0.0.1:8000/v1/groups/processors/stats
```

(Use `127.0.0.1`, not `localhost` - on Windows, resolving `localhost`
through Python's `urllib` falls back from IPv6 to IPv4 and adds a
couple of seconds to every request. Harmless with curl, but it'll make
the SDK look far slower than it is if you hit it while testing.)

Run the broker's test suite with `pytest` from `broker/`.

## Using the Python SDK

```
pip install -e ./sdk
```

```python
from eventmesh import Client, EventSchema, Producer, ConsumerGroupWorker

class OrderCreated(EventSchema):
    order_id: str
    amount: float

client = Client("http://127.0.0.1:8000")
client.create_topic("orders", partition_count=3)
client.register_schema("orders", 1, OrderCreated)
client.create_group("processors", topic="orders")

producer = Producer(client, "orders")
producer.publish(OrderCreated(order_id="o1", amount=42.0), key="o1", schema_version=1)

worker = ConsumerGroupWorker(client, "processors", worker_id="worker-1")

@worker.handler
def handle(event, ctx):
    print(event.payload)  # returning normally acks; raising nacks

worker.run()
```

`EventSchema` is a thin Pydantic base class - `model_json_schema()` is
what gets registered, `model_dump()` is what gets published, so the
schema you validate against is the same class you construct events
with. `ConsumerGroupWorker` handles polling, heartbeating a delivery
lease from a background thread while the handler runs, and reporting
the result; a handler can also call `ctx.ack()` / `ctx.nack(reason)`
itself when the outcome is a business decision rather than something an
exception would naturally express.

`examples/` has a full producer-plus-three-workers demo and a
walkthrough for the scenario this project is built around - see
[examples/README.md](examples/README.md). It's not hypothetical:
killing a worker mid-delivery gets its partition reassigned and its
in-flight event redelivered to a survivor; publishing a duplicate with
a reused dedup key returns the original event instead of creating a
new one; replaying a group from the beginning redelivers everything
with correct attempt counts; an additive schema version registers
cleanly while a breaking one is rejected with the specific property
that broke compatibility - all verified against a live broker with
real worker processes, not simulated.

Run the SDK's test suite with `pytest` from `sdk/` (after
`pip install -e ".[dev]"`).

## REST API

| Method & path | Purpose |
|---|---|
| `POST /v1/topics` | create a topic |
| `GET /v1/topics/{name}` | topic info |
| `POST /v1/topics/{name}/schemas` | register a schema version |
| `GET /v1/topics/{name}/schemas` | list schema versions |
| `POST /v1/topics/{name}/events` | publish an event |
| `GET /v1/events/{id}` | fetch an event |
| `GET /v1/events/{id}/history` | an event's full state-transition history |
| `POST /v1/groups` | create a consumer group |
| `POST /v1/groups/{name}/poll` | a worker claims the next eligible event(s) |
| `POST /v1/deliveries/{id}/heartbeat` | extend a held delivery lease |
| `POST /v1/deliveries/{id}/ack` | report success, advance the offset |
| `POST /v1/deliveries/{id}/nack` | report failure, apply retry policy |
| `POST /v1/groups/{name}/replay` | reset a group's offset(s) backward |
| `GET /v1/groups/{name}/dlq` | list dead-lettered events |
| `POST /v1/groups/{name}/dlq/{id}/redrive` | republish a dead-lettered event |
| `GET /v1/groups/{name}/stats` | per-partition lag, owners, DLQ count |

## What's here

- Topics with hash-partitioned keys and unbounded, append-only retention
- Consumer groups with heartbeat-leased partition assignment across
  workers, rebalanced automatically on join, leave, or crash
- At-least-once delivery via per-event delivery leases with ack/nack
- Exponential backoff with jitter on nack, configurable per group
- Dead-letter queues with a redrive path
- Event replay - moving a committed offset backward, nothing special-cased
- Publish-time deduplication via a reusable dedup key
- An SDK helper for consumer-side idempotency, kept distinct from
  publish-time dedup because they solve different halves of
  at-least-once delivery
- A schema registry enforcing one specific, checkable backward-compatibility rule
- Header-based event filtering, applied broker-side before delivery
- Delayed delivery (`deliver_after`), which deliberately blocks its
  partition until due rather than silently reordering around it
- Full per-event history doubling as the audit trail and the source for
  the stats endpoint's lag and throughput numbers
- REST API and a Python SDK

## Status

This was built and committed in stages rather than all at once: domain
model and storage first, then the orchestration service and its test
suite, then the REST API, then the Python SDK, then the consumer-group
demo - each verified working before moving to the next. `git log` has
the full sequence.
