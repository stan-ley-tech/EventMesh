import sys

from eventmesh import Client, RetryPolicy
from order_events import GROUP, TOPIC, OrderCreated


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    client = Client(base_url)

    topic = client.create_topic(TOPIC, partition_count=3)
    print(f"created topic {topic['name']!r} with {topic['partition_count']} partitions")

    schema = client.register_schema(TOPIC, 1, OrderCreated)
    print(f"registered {TOPIC!r} schema version {schema['version']}")

    group = client.create_group(
        GROUP,
        topic=TOPIC,
        retry_policy=RetryPolicy(max_attempts=3, backoff_base_ms=500, backoff_multiplier=2, backoff_max_ms=5000),
        worker_lease_seconds=8,
        delivery_lease_seconds=8,
    )
    print(f"created consumer group {group['name']!r} on topic {group['topic']!r}")


if __name__ == "__main__":
    main()
