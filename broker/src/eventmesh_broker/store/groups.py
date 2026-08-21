from __future__ import annotations

import json
import sqlite3
from datetime import datetime

from .. import timeutil
from ..models import ConsumerGroup, PartitionAssignment, RetryPolicy


def create_group(conn: sqlite3.Connection, group: ConsumerGroup) -> None:
    conn.execute(
        """INSERT INTO consumer_groups
            (name, topic, filter_headers, retry_policy, delivery_lease_seconds, worker_lease_seconds, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            group.name,
            group.topic,
            json.dumps(group.filter_headers),
            json.dumps(group.retry_policy.to_dict()),
            group.delivery_lease_seconds,
            group.worker_lease_seconds,
            timeutil.fmt(group.created_at),
        ),
    )


def _row_to_group(row: sqlite3.Row) -> ConsumerGroup:
    return ConsumerGroup(
        name=row["name"],
        topic=row["topic"],
        filter_headers=json.loads(row["filter_headers"]),
        retry_policy=RetryPolicy.from_dict(json.loads(row["retry_policy"])),
        delivery_lease_seconds=row["delivery_lease_seconds"],
        worker_lease_seconds=row["worker_lease_seconds"],
        created_at=timeutil.parse(row["created_at"]),
    )


def get_group(conn: sqlite3.Connection, name: str) -> ConsumerGroup | None:
    row = conn.execute("SELECT * FROM consumer_groups WHERE name = ?", (name,)).fetchone()
    return _row_to_group(row) if row else None


def list_groups(conn: sqlite3.Connection) -> list[ConsumerGroup]:
    rows = conn.execute("SELECT * FROM consumer_groups ORDER BY name").fetchall()
    return [_row_to_group(r) for r in rows]


def heartbeat_worker(conn: sqlite3.Connection, group: str, worker_id: str, lease_expires_at: datetime) -> None:
    conn.execute(
        """INSERT INTO worker_heartbeats (group_name, worker_id, lease_expires_at) VALUES (?, ?, ?)
           ON CONFLICT(group_name, worker_id) DO UPDATE SET lease_expires_at = excluded.lease_expires_at""",
        (group, worker_id, timeutil.fmt(lease_expires_at)),
    )


def list_alive_workers(conn: sqlite3.Connection, group: str, now: datetime) -> list[str]:
    rows = conn.execute(
        "SELECT worker_id FROM worker_heartbeats WHERE group_name = ? AND lease_expires_at > ? ORDER BY worker_id",
        (group, timeutil.fmt(now)),
    ).fetchall()
    return [r["worker_id"] for r in rows]


def delete_expired_workers(conn: sqlite3.Connection, group: str, now: datetime) -> list[str]:
    rows = conn.execute(
        "SELECT worker_id FROM worker_heartbeats WHERE group_name = ? AND lease_expires_at <= ?",
        (group, timeutil.fmt(now)),
    ).fetchall()
    expired = [r["worker_id"] for r in rows]
    if expired:
        conn.execute(
            "DELETE FROM worker_heartbeats WHERE group_name = ? AND lease_expires_at <= ?",
            (group, timeutil.fmt(now)),
        )
    return expired


def _row_to_assignment(row: sqlite3.Row) -> PartitionAssignment:
    return PartitionAssignment(
        group=row["group_name"],
        topic=row["topic"],
        partition=row["partition"],
        worker_id=row["worker_id"],
        assigned_at=timeutil.parse(row["assigned_at"]),
    )


def get_assignments(conn: sqlite3.Connection, group: str) -> list[PartitionAssignment]:
    rows = conn.execute(
        "SELECT * FROM partition_assignments WHERE group_name = ? ORDER BY partition", (group,)
    ).fetchall()
    return [_row_to_assignment(r) for r in rows]


def set_assignment(conn: sqlite3.Connection, assignment: PartitionAssignment) -> None:
    conn.execute(
        """INSERT INTO partition_assignments (group_name, topic, partition, worker_id, assigned_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(group_name, partition) DO UPDATE SET
                worker_id = excluded.worker_id, assigned_at = excluded.assigned_at""",
        (
            assignment.group,
            assignment.topic,
            assignment.partition,
            assignment.worker_id,
            timeutil.fmt(assignment.assigned_at),
        ),
    )


def get_partitions_for_worker(conn: sqlite3.Connection, group: str, worker_id: str) -> list[int]:
    rows = conn.execute(
        "SELECT partition FROM partition_assignments WHERE group_name = ? AND worker_id = ? ORDER BY partition",
        (group, worker_id),
    ).fetchall()
    return [r["partition"] for r in rows]


def get_committed_offset(conn: sqlite3.Connection, group: str, partition: int) -> int:
    row = conn.execute(
        "SELECT committed_offset FROM offsets WHERE group_name = ? AND partition = ?",
        (group, partition),
    ).fetchone()
    return row["committed_offset"] if row else -1


def set_committed_offset(conn: sqlite3.Connection, group: str, topic: str, partition: int, offset: int) -> None:
    conn.execute(
        """INSERT INTO offsets (group_name, topic, partition, committed_offset) VALUES (?, ?, ?, ?)
           ON CONFLICT(group_name, partition) DO UPDATE SET committed_offset = excluded.committed_offset""",
        (group, topic, partition, offset),
    )


def get_backoff(conn: sqlite3.Connection, group: str, partition: int) -> datetime | None:
    row = conn.execute(
        "SELECT not_before FROM retry_backoff WHERE group_name = ? AND partition = ?",
        (group, partition),
    ).fetchone()
    return timeutil.parse(row["not_before"]) if row else None


def set_backoff(conn: sqlite3.Connection, group: str, partition: int, not_before: datetime) -> None:
    conn.execute(
        """INSERT INTO retry_backoff (group_name, partition, not_before) VALUES (?, ?, ?)
           ON CONFLICT(group_name, partition) DO UPDATE SET not_before = excluded.not_before""",
        (group, partition, timeutil.fmt(not_before)),
    )


def clear_backoff(conn: sqlite3.Connection, group: str, partition: int) -> None:
    conn.execute("DELETE FROM retry_backoff WHERE group_name = ? AND partition = ?", (group, partition))


def get_epoch(conn: sqlite3.Connection, group: str, partition: int) -> int:
    row = conn.execute(
        "SELECT epoch FROM partition_epoch WHERE group_name = ? AND partition = ?", (group, partition)
    ).fetchone()
    return row["epoch"] if row else 0


def bump_epoch(conn: sqlite3.Connection, group: str, partition: int) -> int:
    next_epoch = get_epoch(conn, group, partition) + 1
    conn.execute(
        """INSERT INTO partition_epoch (group_name, partition, epoch) VALUES (?, ?, ?)
           ON CONFLICT(group_name, partition) DO UPDATE SET epoch = excluded.epoch""",
        (group, partition, next_epoch),
    )
    return next_epoch
