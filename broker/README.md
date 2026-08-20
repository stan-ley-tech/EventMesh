# eventmesh-broker

The EventMesh server: a FastAPI app over a SQLite-backed event log,
handling topics, partitions, the schema registry, consumer group
membership and offsets, delivery leases, retries, dead-letter queues,
and event history.

```
pip install -e ".[dev]"
python -m eventmesh_broker.main
```

Listens on `:8000` by default. See the top-level project README and
[docs/architecture.md](../docs/architecture.md) for how it works.
