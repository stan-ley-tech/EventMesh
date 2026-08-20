import time

import pytest

from eventmesh_broker.db import Database
from eventmesh_broker.core.service import EventMeshService
from eventmesh_broker.errors import (
    LeaseMismatch,
    SchemaConflict,
    SchemaValidationError,
)
from eventmesh_broker.models import RetryPolicy


ORDER_SCHEMA_V1 = {
    "type": "object",
    "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}},
    "required": ["order_id", "amount"],
}


@pytest.fixture()
def service():
    db = Database(":memory:")
    svc = EventMeshService(db)
    yield svc
    db.close()


def test_publish_and_poll_roundtrip(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("processors", topic="orders")

    service.publish("orders", {"order_id": "o1", "amount": 9.5}, schema_version=1, key="o1")

    delivered = service.poll("processors", "worker-1", max_events=1)
    assert len(delivered) == 1
    assert delivered[0].event.payload == {"order_id": "o1", "amount": 9.5}
    assert delivered[0].attempt == 1


def test_schema_validation_rejects_bad_payload(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)

    with pytest.raises(SchemaValidationError):
        service.publish("orders", {"order_id": "o1"}, schema_version=1)


def test_dedup_key_returns_existing_event_without_duplicating(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)

    e1 = service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1, dedup_key="dk1")
    e2 = service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1, dedup_key="dk1")

    assert e1.id == e2.id
    assert e1.offset == e2.offset


def test_ack_advances_offset_and_second_poll_gets_next_event(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("processors", topic="orders")
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)
    service.publish("orders", {"order_id": "o2", "amount": 2.0}, schema_version=1)

    first = service.poll("processors", "worker-1", max_events=1)[0]
    service.ack(first.delivery_id, "worker-1")

    second = service.poll("processors", "worker-1", max_events=1)[0]
    assert second.event.payload["order_id"] == "o2"


def test_worker_crash_reassigns_partition_to_survivor(service):
    service.create_topic("orders", partition_count=2)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("processors", topic="orders", worker_lease_seconds=1)

    for i in range(4):
        service.publish("orders", {"order_id": f"o{i}", "amount": 1.0}, schema_version=1, key=f"o{i}")

    # two workers join and each end up owning a partition
    service.heartbeat_worker("processors", "worker-a")
    owned_a = service.heartbeat_worker("processors", "worker-b")
    assert len(owned_a) >= 0  # worker-b's own partitions, sanity only

    owned_a_partitions = service.heartbeat_worker("processors", "worker-a")
    assert len(owned_a_partitions) == 1

    # worker-a "crashes": it stops heartbeating. Wait past its lease.
    time.sleep(1.2)

    # worker-b heartbeats again and should pick up worker-a's partition too
    owned_b_after = service.heartbeat_worker("processors", "worker-b")
    assert len(owned_b_after) == 2


def test_nack_retries_then_dead_letters_after_max_attempts(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group(
        "processors", topic="orders",
        retry_policy=RetryPolicy(max_attempts=2, backoff_base_ms=1, backoff_multiplier=1, backoff_max_ms=5, jitter_fraction=0),
    )
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)

    d1 = service.poll("processors", "worker-1", max_events=1)[0]
    service.nack(d1.delivery_id, "worker-1", "boom")

    time.sleep(0.05)
    d2 = service.poll("processors", "worker-1", max_events=1)[0]
    assert d2.attempt == 2
    service.nack(d2.delivery_id, "worker-1", "boom again")

    assert service.poll("processors", "worker-1", max_events=1) == []
    dlq = service.list_dead_letters("processors")
    assert len(dlq) == 1
    assert dlq[0].attempts == 2


def test_dead_letter_can_be_redriven_and_reprocessed(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group(
        "processors", topic="orders",
        retry_policy=RetryPolicy(max_attempts=1, backoff_base_ms=1, backoff_multiplier=1, backoff_max_ms=5, jitter_fraction=0),
    )
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)

    d1 = service.poll("processors", "worker-1", max_events=1)[0]
    service.nack(d1.delivery_id, "worker-1", "boom")
    dlq = service.list_dead_letters("processors")
    assert len(dlq) == 1

    redriven_event = service.redrive("processors", dlq[0].id)
    assert redriven_event.payload == {"order_id": "o1", "amount": 1.0}

    delivered = service.poll("processors", "worker-1", max_events=1)
    assert len(delivered) == 1
    assert delivered[0].event.id == redriven_event.id


def test_replay_resets_offset_and_redelivers(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("processors", topic="orders")
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)
    service.publish("orders", {"order_id": "o2", "amount": 2.0}, schema_version=1)

    d1 = service.poll("processors", "worker-1", max_events=1)[0]
    service.ack(d1.delivery_id, "worker-1")
    d2 = service.poll("processors", "worker-1", max_events=1)[0]
    service.ack(d2.delivery_id, "worker-1")

    assert service.poll("processors", "worker-1", max_events=1) == []

    service.replay("processors", earliest=True)

    replayed = service.poll("processors", "worker-1", max_events=1)[0]
    assert replayed.event.payload["order_id"] == "o1"
    assert replayed.attempt == 2  # this event has now been delivered twice


def test_schema_evolution_accepts_additive_version_and_rejects_breaking_one(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)

    v2 = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "number"},
            "currency": {"type": "string"},
        },
        "required": ["order_id", "amount"],
    }
    service.register_schema("orders", 2, v2)

    old_event = service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)
    new_event = service.publish("orders", {"order_id": "o2", "amount": 2.0, "currency": "USD"}, schema_version=2)
    assert old_event.schema_version == 1
    assert new_event.schema_version == 2

    breaking_v3 = {
        "type": "object",
        "properties": {"order_id": {"type": "string"}},
        "required": ["order_id"],
    }
    with pytest.raises(SchemaConflict):
        service.register_schema("orders", 3, breaking_v3)


def test_ack_with_wrong_worker_raises_lease_mismatch(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("processors", topic="orders")
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)

    d1 = service.poll("processors", "worker-1", max_events=1)[0]
    with pytest.raises(LeaseMismatch):
        service.ack(d1.delivery_id, "worker-2")


def test_delivery_heartbeat_prevents_lease_reclaim(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("processors", topic="orders", delivery_lease_seconds=1)
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)

    d1 = service.poll("processors", "worker-1", max_events=1)[0]

    # heartbeat keeps extending the lease past its original 1s window
    for _ in range(3):
        time.sleep(0.6)
        service.heartbeat_delivery(d1.delivery_id, "worker-1")

    service.reclaim_expired_deliveries()
    service.ack(d1.delivery_id, "worker-1")  # still the same, un-reclaimed delivery

    dlq = service.list_dead_letters("processors")
    assert dlq == []


def test_expired_delivery_without_heartbeat_gets_reclaimed(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group(
        "processors", topic="orders", delivery_lease_seconds=1,
        retry_policy=RetryPolicy(max_attempts=2, backoff_base_ms=1, backoff_multiplier=1, backoff_max_ms=5, jitter_fraction=0),
    )
    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1)

    d1 = service.poll("processors", "worker-1", max_events=1)[0]
    time.sleep(1.2)
    service.reclaim_expired_deliveries()

    # worker-1's partition-ownership lease is untouched (default 30s) even
    # though its delivery lease lapsed, so it's still the one that gets the
    # redelivery - this is the "handler stalled on one event" case, distinct
    # from a full worker crash which is covered by the reassignment test.
    time.sleep(0.05)
    redelivered = service.poll("processors", "worker-1", max_events=1)[0]
    assert redelivered.attempt == 2
    assert redelivered.event.id == d1.event.id


def test_filter_skips_non_matching_events(service):
    service.create_topic("orders", partition_count=1)
    service.register_schema("orders", 1, ORDER_SCHEMA_V1)
    service.create_group("us_only", topic="orders", filter_headers={"region": "us"})

    service.publish("orders", {"order_id": "o1", "amount": 1.0}, schema_version=1, headers={"region": "eu"})
    service.publish("orders", {"order_id": "o2", "amount": 2.0}, schema_version=1, headers={"region": "us"})

    delivered = service.poll("us_only", "worker-1", max_events=1)
    assert len(delivered) == 1
    assert delivered[0].event.payload["order_id"] == "o2"
