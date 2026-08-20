import sys
import uuid

from eventmesh import Client, Producer
from order_events import TOPIC, OrderCreated


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else 9

    client = Client(base_url)
    producer = Producer(client, TOPIC)

    for i in range(count):
        order_id = f"order-{i}"
        event = producer.publish(
            OrderCreated(order_id=order_id, amount=10.0 + i),
            key=order_id,
            schema_version=1,
            dedup_key=str(uuid.uuid4()),
        )
        print(f"published {order_id} -> partition {event.partition} offset {event.offset}")


if __name__ == "__main__":
    main()
