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

## Status

Under active development, built and committed in stages rather than all
at once - see `git log` for the sequence.
