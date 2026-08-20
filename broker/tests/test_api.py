import pytest
from fastapi.testclient import TestClient

from eventmesh_broker.api.app import create_app
from eventmesh_broker.core.service import EventMeshService
from eventmesh_broker.db import Database

ORDER_SCHEMA = {
    "type": "object",
    "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}},
    "required": ["order_id", "amount"],
}


@pytest.fixture()
def client():
    db = Database(":memory:")
    service = EventMeshService(db)
    app = create_app(service)
    with TestClient(app) as c:
        yield c
    db.close()


def test_full_publish_poll_ack_cycle(client):
    r = client.post("/v1/topics", json={"name": "orders", "partition_count": 1})
    assert r.status_code == 201

    r = client.post("/v1/topics/orders/schemas", json={"version": 1, "json_schema": ORDER_SCHEMA})
    assert r.status_code == 201

    r = client.post("/v1/groups", json={"name": "processors", "topic": "orders"})
    assert r.status_code == 201

    r = client.post("/v1/topics/orders/events", json={"payload": {"order_id": "o1", "amount": 5.0}, "schema_version": 1})
    assert r.status_code == 201
    event_id = r.json()["id"]

    r = client.post("/v1/groups/processors/poll", json={"worker_id": "worker-1"})
    assert r.status_code == 200
    delivered = r.json()
    assert len(delivered) == 1
    assert delivered[0]["event"]["id"] == event_id
    delivery_id = delivered[0]["delivery_id"]

    r = client.post(f"/v1/deliveries/{delivery_id}/ack", json={"worker_id": "worker-1"})
    assert r.status_code == 200

    history = client.get(f"/v1/events/{event_id}/history").json()
    types = [h["type"] for h in history]
    assert types == ["PUBLISHED", "DELIVERED", "ACKED"]


def test_publish_with_invalid_payload_returns_422(client):
    client.post("/v1/topics", json={"name": "orders", "partition_count": 1})
    client.post("/v1/topics/orders/schemas", json={"version": 1, "json_schema": ORDER_SCHEMA})

    r = client.post("/v1/topics/orders/events", json={"payload": {"order_id": "o1"}, "schema_version": 1})
    assert r.status_code == 422


def test_unknown_topic_returns_404(client):
    r = client.get("/v1/topics/missing")
    assert r.status_code == 404


def test_nack_wrong_worker_returns_409(client):
    client.post("/v1/topics", json={"name": "orders", "partition_count": 1})
    client.post("/v1/topics/orders/schemas", json={"version": 1, "json_schema": ORDER_SCHEMA})
    client.post("/v1/groups", json={"name": "processors", "topic": "orders"})
    client.post("/v1/topics/orders/events", json={"payload": {"order_id": "o1", "amount": 1.0}, "schema_version": 1})

    delivered = client.post("/v1/groups/processors/poll", json={"worker_id": "worker-1"}).json()
    delivery_id = delivered[0]["delivery_id"]

    r = client.post(f"/v1/deliveries/{delivery_id}/nack", json={"worker_id": "someone-else", "reason": "x"})
    assert r.status_code == 409
