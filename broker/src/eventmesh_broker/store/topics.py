from __future__ import annotations

import json
import sqlite3

from .. import timeutil
from ..models import SchemaVersion, Topic


def create_topic(conn: sqlite3.Connection, topic: Topic) -> None:
    conn.execute(
        "INSERT INTO topics (name, partition_count, created_at) VALUES (?, ?, ?)",
        (topic.name, topic.partition_count, timeutil.fmt(topic.created_at)),
    )


def _row_to_topic(row: sqlite3.Row) -> Topic:
    return Topic(
        name=row["name"],
        partition_count=row["partition_count"],
        created_at=timeutil.parse(row["created_at"]),
    )


def get_topic(conn: sqlite3.Connection, name: str) -> Topic | None:
    row = conn.execute("SELECT * FROM topics WHERE name = ?", (name,)).fetchone()
    return _row_to_topic(row) if row else None


def list_topics(conn: sqlite3.Connection) -> list[Topic]:
    rows = conn.execute("SELECT * FROM topics ORDER BY name").fetchall()
    return [_row_to_topic(r) for r in rows]


def register_schema_version(conn: sqlite3.Connection, sv: SchemaVersion) -> None:
    conn.execute(
        "INSERT INTO schema_versions (topic, version, json_schema, created_at) VALUES (?, ?, ?, ?)",
        (sv.topic, sv.version, json.dumps(sv.json_schema), timeutil.fmt(sv.created_at)),
    )


def _row_to_schema_version(row: sqlite3.Row) -> SchemaVersion:
    return SchemaVersion(
        topic=row["topic"],
        version=row["version"],
        json_schema=json.loads(row["json_schema"]),
        created_at=timeutil.parse(row["created_at"]),
    )


def get_schema_version(conn: sqlite3.Connection, topic: str, version: int) -> SchemaVersion | None:
    row = conn.execute(
        "SELECT * FROM schema_versions WHERE topic = ? AND version = ?", (topic, version)
    ).fetchone()
    return _row_to_schema_version(row) if row else None


def get_latest_schema_version(conn: sqlite3.Connection, topic: str) -> SchemaVersion | None:
    row = conn.execute(
        "SELECT * FROM schema_versions WHERE topic = ? ORDER BY version DESC LIMIT 1", (topic,)
    ).fetchone()
    return _row_to_schema_version(row) if row else None


def list_schema_versions(conn: sqlite3.Connection, topic: str) -> list[SchemaVersion]:
    rows = conn.execute(
        "SELECT * FROM schema_versions WHERE topic = ? ORDER BY version ASC", (topic,)
    ).fetchall()
    return [_row_to_schema_version(r) for r in rows]
