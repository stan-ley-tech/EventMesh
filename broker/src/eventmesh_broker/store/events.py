from __future__ import annotations

import json
import sqlite3

from .. import timeutil
from ..models import Event


def next_offset(conn: sqlite3.Connection, topic: str, partition: int) -> int:
    row = conn.execute(
        "SELECT MAX(offset) AS max_offset FROM events WHERE topic = ? AND partition = ?",
        (topic, partition),
    ).fetchone()
    if row["max_offset"] is None:
        return 0
    return row["max_offset"] + 1


def latest_offset(conn: sqlite3.Connection, topic: str, partition: int) -> int | None:
    row = conn.execute(
        "SELECT MAX(offset) AS max_offset FROM events WHERE topic = ? AND partition = ?",
        (topic, partition),
    ).fetchone()
    return row["max_offset"]


def get_by_dedup_key(conn: sqlite3.Connection, topic: str, dedup_key: str) -> Event | None:
    row = conn.execute(
        "SELECT * FROM events WHERE topic = ? AND dedup_key = ?", (topic, dedup_key)
    ).fetchone()
    return _row_to_event(row) if row else None


def insert_event(conn: sqlite3.Connection, event: Event) -> None:
    conn.execute(
        """INSERT INTO events
            (id, topic, partition, offset, key, payload, schema_version, dedup_key, headers, deliver_after, published_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            event.id,
            event.topic,
            event.partition,
            event.offset,
            event.key,
            json.dumps(event.payload),
            event.schema_version,
            event.dedup_key,
            json.dumps(event.headers),
            timeutil.fmt(event.deliver_after) if event.deliver_after else None,
            timeutil.fmt(event.published_at),
        ),
    )


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        id=row["id"],
        topic=row["topic"],
        partition=row["partition"],
        offset=row["offset"],
        key=row["key"],
        payload=json.loads(row["payload"]),
        schema_version=row["schema_version"],
        dedup_key=row["dedup_key"],
        headers=json.loads(row["headers"]),
        deliver_after=timeutil.parse(row["deliver_after"]),
        published_at=timeutil.parse(row["published_at"]),
    )


def get_event(conn: sqlite3.Connection, event_id: str) -> Event | None:
    row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
    return _row_to_event(row) if row else None


def get_event_by_offset(conn: sqlite3.Connection, topic: str, partition: int, offset: int) -> Event | None:
    row = conn.execute(
        "SELECT * FROM events WHERE topic = ? AND partition = ? AND offset = ?",
        (topic, partition, offset),
    ).fetchone()
    return _row_to_event(row) if row else None
