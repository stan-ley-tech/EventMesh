import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from eventmesh import Client, LeaseMismatch, TopicNotFound


class FakeBrokerHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b""
        return json.loads(raw) if raw else None

    def _respond(self, status, body=None):
        payload = json.dumps(body).encode("utf-8") if body is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def do_POST(self):
        body = self._read_json()

        if self.path == "/v1/topics":
            self._respond(201, {"name": body["name"], "partition_count": body["partition_count"], "created_at": "2026-01-01T00:00:00+00:00"})
            return

        if self.path == "/v1/topics/orders/events":
            self._respond(201, {
                "id": "evt-1", "topic": "orders", "partition": 0, "offset": 0,
                "key": body.get("key"), "payload": body["payload"], "schema_version": body.get("schema_version") or 1,
                "dedup_key": body.get("dedup_key"), "headers": body.get("headers") or {},
                "deliver_after": None, "published_at": "2026-01-01T00:00:00+00:00",
            })
            return

        if self.path == "/v1/deliveries/d1/ack":
            if body.get("worker_id") == "worker-1":
                self._respond(200, {"status": "acked"})
            else:
                self._respond(409, {"error": "lease mismatch", "type": "LeaseMismatch"})
            return

        self._respond(404, {"error": "not found", "type": "EventNotFound"})

    def do_GET(self):
        if self.path == "/v1/topics/missing":
            self._respond(404, {"error": "topic not found", "type": "TopicNotFound"})
            return
        self._respond(404, {"error": "not found"})


@pytest.fixture()
def server():
    httpd = HTTPServer(("127.0.0.1", 0), FakeBrokerHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()
    thread.join()


@pytest.fixture()
def client(server):
    port = server.server_address[1]
    return Client(f"http://127.0.0.1:{port}")


def test_create_topic_round_trips(client):
    topic = client.create_topic("orders", partition_count=4)
    assert topic["partition_count"] == 4


def test_publish_parses_event_fields(client):
    event = client.publish("orders", {"order_id": "o1"}, schema_version=1, key="o1")
    assert event.id == "evt-1"
    assert event.payload == {"order_id": "o1"}
    assert event.key == "o1"


def test_get_topic_not_found_raises_typed_error(client):
    with pytest.raises(TopicNotFound):
        client.get_topic("missing")


def test_ack_with_wrong_worker_raises_lease_mismatch(client):
    with pytest.raises(LeaseMismatch):
        client.ack("d1", "someone-else")


def test_ack_with_right_worker_succeeds(client):
    client.ack("d1", "worker-1")
