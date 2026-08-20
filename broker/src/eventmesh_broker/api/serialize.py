from __future__ import annotations

from .. import timeutil
from ..core.service import Delivered
from ..models import ConsumerGroup, DeadLetter, Event, HistoryEntry, SchemaVersion, Topic


def topic(t: Topic) -> dict:
    return {"name": t.name, "partition_count": t.partition_count, "created_at": timeutil.fmt(t.created_at)}


def schema_version(sv: SchemaVersion) -> dict:
    return {
        "topic": sv.topic,
        "version": sv.version,
        "json_schema": sv.json_schema,
        "created_at": timeutil.fmt(sv.created_at),
    }


def event(e: Event) -> dict:
    return {
        "id": e.id,
        "topic": e.topic,
        "partition": e.partition,
        "offset": e.offset,
        "key": e.key,
        "payload": e.payload,
        "schema_version": e.schema_version,
        "dedup_key": e.dedup_key,
        "headers": e.headers,
        "deliver_after": timeutil.fmt(e.deliver_after) if e.deliver_after else None,
        "published_at": timeutil.fmt(e.published_at),
    }


def group(g: ConsumerGroup) -> dict:
    return {
        "name": g.name,
        "topic": g.topic,
        "filter_headers": g.filter_headers,
        "retry_policy": g.retry_policy.to_dict(),
        "delivery_lease_seconds": g.delivery_lease_seconds,
        "worker_lease_seconds": g.worker_lease_seconds,
        "created_at": timeutil.fmt(g.created_at),
    }


def delivered(d: Delivered) -> dict:
    return {"delivery_id": d.delivery_id, "attempt": d.attempt, "event": event(d.event)}


def dead_letter(dl: DeadLetter) -> dict:
    return {
        "id": dl.id,
        "group": dl.group,
        "topic": dl.topic,
        "partition": dl.partition,
        "event_id": dl.event_id,
        "event_offset": dl.event_offset,
        "reason": dl.reason,
        "attempts": dl.attempts,
        "failed_at": timeutil.fmt(dl.failed_at),
        "redriven": dl.redriven,
        "redriven_as_event_id": dl.redriven_as_event_id,
    }


def history_entry(h: HistoryEntry) -> dict:
    return {"seq": h.seq, "type": h.type, "detail": h.detail, "created_at": timeutil.fmt(h.created_at)}
