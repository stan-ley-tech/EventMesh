import time

from eventmesh.client import Delivery, Event
from eventmesh.consumer import ConsumerGroupWorker


class FakeClient:
    def __init__(self):
        self.acked = []
        self.nacked = []
        self.heartbeats = []

    def ack(self, delivery_id, worker_id):
        self.acked.append((delivery_id, worker_id))

    def nack(self, delivery_id, worker_id, reason=""):
        self.nacked.append((delivery_id, worker_id, reason))

    def heartbeat_delivery(self, delivery_id, worker_id):
        self.heartbeats.append((delivery_id, worker_id))


def make_delivery(event_id="evt-1", delivery_id="d1", attempt=1):
    event = Event(
        id=event_id, topic="orders", partition=0, offset=0, key=None,
        payload={"order_id": "o1"}, schema_version=1, dedup_key=None,
        headers={}, deliver_after=None, published_at="2026-01-01T00:00:00+00:00",
    )
    return Delivery(delivery_id=delivery_id, attempt=attempt, event=event)


def make_worker(client, handler, **kwargs):
    worker = ConsumerGroupWorker(client, "processors", worker_id="worker-1", **kwargs)
    worker.handler(handler)
    return worker


def test_handler_returning_normally_auto_acks():
    client = FakeClient()
    calls = []

    def handle(event, ctx):
        calls.append(event.id)

    worker = make_worker(client, handle)
    worker._process(make_delivery())

    assert calls == ["evt-1"]
    assert client.acked == [("d1", "worker-1")]
    assert client.nacked == []


def test_handler_raising_auto_nacks_with_message():
    client = FakeClient()

    def handle(event, ctx):
        raise ValueError("boom")

    worker = make_worker(client, handle)
    worker._process(make_delivery())

    assert client.nacked == [("d1", "worker-1", "boom")]
    assert client.acked == []


def test_explicit_ack_inside_handler_is_not_duplicated():
    client = FakeClient()

    def handle(event, ctx):
        ctx.ack()

    worker = make_worker(client, handle)
    worker._process(make_delivery())

    assert client.acked == [("d1", "worker-1")]


def test_explicit_nack_inside_handler_prevents_auto_ack():
    client = FakeClient()

    def handle(event, ctx):
        ctx.nack("business reason")

    worker = make_worker(client, handle)
    worker._process(make_delivery())

    assert client.nacked == [("d1", "worker-1", "business reason")]
    assert client.acked == []


def test_idempotent_worker_skips_reprocessing_seen_event():
    client = FakeClient()
    calls = []

    def handle(event, ctx):
        calls.append(event.id)

    worker = make_worker(client, handle, idempotent=True)
    worker._process(make_delivery(delivery_id="d1"))
    worker._process(make_delivery(delivery_id="d2"))  # redelivery of the same event.id

    assert calls == ["evt-1"]  # handler only ran once
    assert client.acked == [("d1", "worker-1"), ("d2", "worker-1")]  # both still acked


def test_heartbeat_fires_while_handler_is_running():
    client = FakeClient()

    def handle(event, ctx):
        time.sleep(0.25)

    worker = make_worker(client, handle, heartbeat_interval=0.05)
    worker._process(make_delivery())

    assert len(client.heartbeats) >= 2
    assert all(hb == ("d1", "worker-1") for hb in client.heartbeats)
