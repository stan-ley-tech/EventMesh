# eventmesh (Python SDK)

Client library for EventMesh: define event schemas, publish, and
consume with a worker that handles polling, heartbeating, and
idempotent processing for you. No dependency beyond Pydantic, which the
schema helper is built on.

```python
from eventmesh import EventSchema

class OrderCreated(EventSchema):
    order_id: str
    amount: float
```

```python
from eventmesh import Client, Producer

client = Client("http://localhost:8000")
client.create_topic("orders", partition_count=3)
client.register_schema("orders", 1, OrderCreated)

producer = Producer(client, "orders")
producer.publish(OrderCreated(order_id="o1", amount=42.0), key="o1", schema_version=1)
```

```python
from eventmesh import ConsumerGroupWorker

client.create_group("processors", topic="orders")
worker = ConsumerGroupWorker(client, "processors", worker_id="worker-1")

@worker.handler
def handle(event, ctx):
    print(event.payload)
    ctx.ack()

worker.run()
```

See the top-level project README and
[examples/](../examples) for a runnable multi-worker demo covering
crashes, duplicate publishes, replay, and schema evolution.
