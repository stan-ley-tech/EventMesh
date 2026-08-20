import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Union

from .errors import from_response
from .retry import RetryPolicy
from .schema import EventSchema


@dataclass
class Event:
    id: str
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    payload: dict
    schema_version: int
    dedup_key: Optional[str]
    headers: dict
    deliver_after: Optional[str]
    published_at: str


@dataclass
class Delivery:
    delivery_id: str
    attempt: int
    event: Event


def _event_from_json(body: dict) -> Event:
    return Event(
        id=body["id"], topic=body["topic"], partition=body["partition"], offset=body["offset"],
        key=body.get("key"), payload=body["payload"], schema_version=body["schema_version"],
        dedup_key=body.get("dedup_key"), headers=body.get("headers") or {},
        deliver_after=body.get("deliver_after"), published_at=body["published_at"],
    )


def _delivery_from_json(body: dict) -> Delivery:
    return Delivery(delivery_id=body["delivery_id"], attempt=body["attempt"], event=_event_from_json(body["event"]))


class Client:
    """Thin HTTP client for the EventMesh broker's REST API. Standard
    library only, same reasoning as FlowForge's Python SDK: no
    third-party dependency needed just to make HTTP requests."""

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: Optional[dict] = None):
        url = f"{self.base_url}{path}"
        data = json.dumps(body, default=str).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                parsed = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                parsed = {"error": raw.decode("utf-8", "replace")}
            raise from_response(exc.code, parsed) from None

    def create_topic(self, name: str, partition_count: int = 3) -> dict:
        return self._request("POST", "/v1/topics", {"name": name, "partition_count": partition_count})

    def get_topic(self, name: str) -> dict:
        return self._request("GET", f"/v1/topics/{name}")

    def list_topics(self) -> list[dict]:
        return self._request("GET", "/v1/topics")

    def register_schema(self, topic: str, version: int, json_schema: Union[dict, type[EventSchema]]) -> dict:
        if isinstance(json_schema, type) and issubclass(json_schema, EventSchema):
            json_schema = json_schema.json_schema()
        return self._request("POST", f"/v1/topics/{topic}/schemas", {"version": version, "json_schema": json_schema})

    def list_schemas(self, topic: str) -> list[dict]:
        return self._request("GET", f"/v1/topics/{topic}/schemas")

    def get_schema(self, topic: str, version: int) -> dict:
        return self._request("GET", f"/v1/topics/{topic}/schemas/{version}")

    def publish(
        self,
        topic: str,
        payload: Union[dict, EventSchema],
        schema_version: Optional[int] = None,
        key: Optional[str] = None,
        dedup_key: Optional[str] = None,
        headers: Optional[dict] = None,
        deliver_after: Optional[datetime] = None,
    ) -> Event:
        if isinstance(payload, EventSchema):
            payload = payload.model_dump(mode="json")
        body = {
            "payload": payload,
            "schema_version": schema_version,
            "key": key,
            "dedup_key": dedup_key,
            "headers": headers or {},
            "deliver_after": deliver_after.isoformat() if deliver_after else None,
        }
        return _event_from_json(self._request("POST", f"/v1/topics/{topic}/events", body))

    def get_event(self, event_id: str) -> Event:
        return _event_from_json(self._request("GET", f"/v1/events/{event_id}"))

    def get_history(self, event_id: str) -> list[dict]:
        return self._request("GET", f"/v1/events/{event_id}/history")

    def create_group(
        self,
        name: str,
        topic: str,
        filter_headers: Optional[dict] = None,
        retry_policy: Optional[RetryPolicy] = None,
        delivery_lease_seconds: int = 30,
        worker_lease_seconds: int = 30,
        start_from: str = "earliest",
    ) -> dict:
        body = {
            "name": name,
            "topic": topic,
            "filter_headers": filter_headers or {},
            "retry_policy": retry_policy.to_dict() if retry_policy else None,
            "delivery_lease_seconds": delivery_lease_seconds,
            "worker_lease_seconds": worker_lease_seconds,
            "start_from": start_from,
        }
        return self._request("POST", "/v1/groups", body)

    def get_group(self, name: str) -> dict:
        return self._request("GET", f"/v1/groups/{name}")

    def poll(self, group: str, worker_id: str, max_events: int = 1) -> list[Delivery]:
        body = self._request("POST", f"/v1/groups/{group}/poll", {"worker_id": worker_id, "max_events": max_events})
        return [_delivery_from_json(d) for d in body]

    def heartbeat_delivery(self, delivery_id: str, worker_id: str) -> None:
        self._request("POST", f"/v1/deliveries/{delivery_id}/heartbeat", {"worker_id": worker_id})

    def ack(self, delivery_id: str, worker_id: str) -> None:
        self._request("POST", f"/v1/deliveries/{delivery_id}/ack", {"worker_id": worker_id})

    def nack(self, delivery_id: str, worker_id: str, reason: str = "") -> None:
        self._request("POST", f"/v1/deliveries/{delivery_id}/nack", {"worker_id": worker_id, "reason": reason})

    def replay(self, group: str, partition: Optional[int] = None, to_offset: Optional[int] = None, earliest: bool = False) -> None:
        self._request("POST", f"/v1/groups/{group}/replay", {"partition": partition, "to_offset": to_offset, "earliest": earliest})

    def list_dead_letters(self, group: str) -> list[dict]:
        return self._request("GET", f"/v1/groups/{group}/dlq")

    def redrive(self, group: str, dead_letter_id: str) -> Event:
        return _event_from_json(self._request("POST", f"/v1/groups/{group}/dlq/{dead_letter_id}/redrive"))

    def get_stats(self, group: str) -> dict:
        return self._request("GET", f"/v1/groups/{group}/stats")
