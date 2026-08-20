"""SQLite persistence for the broker. A single connection shared behind
a lock, the same choice FlowForge's engine makes and for the same
reason: it turns every sequence of statements issued from the service
layer inside `atomic()` into something fully serialized against every
other such sequence, which is what keeps partition assignment, offset
commits, and delivery leasing correct without hand-rolled locking.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    name TEXT PRIMARY KEY,
    partition_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_versions (
    topic TEXT NOT NULL,
    version INTEGER NOT NULL,
    json_schema TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (topic, version)
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    key TEXT,
    payload TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    dedup_key TEXT,
    headers TEXT NOT NULL DEFAULT '{}',
    deliver_after TEXT,
    published_at TEXT NOT NULL,
    UNIQUE (topic, partition, offset),
    UNIQUE (topic, dedup_key)
);
CREATE INDEX IF NOT EXISTS idx_events_topic_partition_offset ON events(topic, partition, offset);

CREATE TABLE IF NOT EXISTS consumer_groups (
    name TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    filter_headers TEXT NOT NULL DEFAULT '{}',
    retry_policy TEXT NOT NULL,
    delivery_lease_seconds INTEGER NOT NULL,
    worker_lease_seconds INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    group_name TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    PRIMARY KEY (group_name, worker_id)
);

CREATE TABLE IF NOT EXISTS partition_assignments (
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    assigned_at TEXT NOT NULL,
    PRIMARY KEY (group_name, partition)
);

CREATE TABLE IF NOT EXISTS offsets (
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    committed_offset INTEGER NOT NULL,
    PRIMARY KEY (group_name, partition)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    event_offset INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    delivered_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_active_partition
    ON deliveries(group_name, partition) WHERE status = 'DELIVERED';
CREATE INDEX IF NOT EXISTS idx_deliveries_lease ON deliveries(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS dead_letters (
    id TEXT PRIMARY KEY,
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    event_offset INTEGER NOT NULL,
    reason TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    failed_at TEXT NOT NULL,
    redriven INTEGER NOT NULL DEFAULT 0,
    redriven_as_event_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_dead_letters_group ON dead_letters(group_name, redriven);

CREATE TABLE IF NOT EXISTS retry_backoff (
    group_name TEXT NOT NULL,
    partition INTEGER NOT NULL,
    not_before TEXT NOT NULL,
    PRIMARY KEY (group_name, partition)
);

CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    type TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_event ON history(event_id, seq);
"""


class Database:
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        self._conn.executescript(SCHEMA)
        self._lock = threading.RLock()

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def atomic(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @contextmanager
    def read(self):
        with self._lock:
            yield self._conn
