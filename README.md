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
curl -X POST localhost:8000/v1/topics -d '{"name": "orders", "partition_count": 2}'
curl -X POST localhost:8000/v1/topics/orders/schemas -d '{
  "version": 1,
  "json_schema": {"type": "object", "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}}, "required": ["order_id", "amount"]}
}'
curl -X POST localhost:8000/v1/groups -d '{"name": "processors", "topic": "orders"}'

curl -X POST localhost:8000/v1/topics/orders/events -d '{"payload": {"order_id": "o1", "amount": 42}, "schema_version": 1, "key": "o1"}'
curl -X POST localhost:8000/v1/groups/processors/poll -d '{"worker_id": "worker-1"}'
curl -X POST localhost:8000/v1/deliveries/<delivery_id>/ack -d '{"worker_id": "worker-1"}'

curl localhost:8000/v1/events/<event_id>/history
curl localhost:8000/v1/groups/processors/stats
```

Run the broker's test suite with `pytest` from `broker/`.

## Status

Under active development, built and committed in stages rather than all
at once - see `git log` for the sequence.

Built so far: the broker (topics, schema registry, publish/poll/ack/nack,
partition assignment with heartbeat-based failover, retries, DLQ with
redrive, replay, the REST API). Not yet built: the Python SDK.
