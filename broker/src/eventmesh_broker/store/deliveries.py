from __future__ import annotations

import sqlite3
from datetime import datetime

from .. import timeutil
from ..models import DeadLetter, Delivery, DeliveryStatus


def _row_to_delivery(row: sqlite3.Row) -> Delivery:
    return Delivery(
        id=row["id"],
        group=row["group_name"],
        topic=row["topic"],
        partition=row["partition"],
        event_id=row["event_id"],
        event_offset=row["event_offset"],
        worker_id=row["worker_id"],
        attempt=row["attempt"],
        epoch=row["epoch"],
        status=DeliveryStatus(row["status"]),
        lease_expires_at=timeutil.parse(row["lease_expires_at"]),
        delivered_at=timeutil.parse(row["delivered_at"]),
    )


def create_delivery(conn: sqlite3.Connection, delivery: Delivery) -> None:
    conn.execute(
        """INSERT INTO deliveries
            (id, group_name, topic, partition, event_id, event_offset, worker_id, attempt, epoch, status, lease_expires_at, delivered_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            delivery.id,
            delivery.group,
            delivery.topic,
            delivery.partition,
            delivery.event_id,
            delivery.event_offset,
            delivery.worker_id,
            delivery.attempt,
            delivery.epoch,
            delivery.status.value,
            timeutil.fmt(delivery.lease_expires_at),
            timeutil.fmt(delivery.delivered_at),
        ),
    )


def get_delivery(conn: sqlite3.Connection, delivery_id: str) -> Delivery | None:
    row = conn.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
    return _row_to_delivery(row) if row else None


def get_active_delivery_for_partition(conn: sqlite3.Connection, group: str, partition: int) -> Delivery | None:
    row = conn.execute(
        "SELECT * FROM deliveries WHERE group_name = ? AND partition = ? AND status = ?",
        (group, partition, DeliveryStatus.DELIVERED.value),
    ).fetchone()
    return _row_to_delivery(row) if row else None


def set_status(conn: sqlite3.Connection, delivery_id: str, status: DeliveryStatus) -> None:
    conn.execute("UPDATE deliveries SET status = ? WHERE id = ?", (status.value, delivery_id))


def renew_lease(conn: sqlite3.Connection, delivery_id: str, new_expiry: datetime) -> bool:
    cur = conn.execute(
        "UPDATE deliveries SET lease_expires_at = ? WHERE id = ? AND status = ?",
        (timeutil.fmt(new_expiry), delivery_id, DeliveryStatus.DELIVERED.value),
    )
    return cur.rowcount > 0


def find_expired_deliveries(conn: sqlite3.Connection, now: datetime) -> list[Delivery]:
    rows = conn.execute(
        "SELECT * FROM deliveries WHERE status = ? AND lease_expires_at <= ?",
        (DeliveryStatus.DELIVERED.value, timeutil.fmt(now)),
    ).fetchall()
    return [_row_to_delivery(r) for r in rows]


def _row_to_dead_letter(row: sqlite3.Row) -> DeadLetter:
    return DeadLetter(
        id=row["id"],
        group=row["group_name"],
        topic=row["topic"],
        partition=row["partition"],
        event_id=row["event_id"],
        event_offset=row["event_offset"],
        reason=row["reason"],
        attempts=row["attempts"],
        failed_at=timeutil.parse(row["failed_at"]),
        redriven=bool(row["redriven"]),
        redriven_as_event_id=row["redriven_as_event_id"],
    )


def insert_dead_letter(conn: sqlite3.Connection, dl: DeadLetter) -> None:
    conn.execute(
        """INSERT INTO dead_letters
            (id, group_name, topic, partition, event_id, event_offset, reason, attempts, failed_at, redriven)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
        (dl.id, dl.group, dl.topic, dl.partition, dl.event_id, dl.event_offset, dl.reason, dl.attempts, timeutil.fmt(dl.failed_at)),
    )


def get_dead_letter(conn: sqlite3.Connection, dl_id: str) -> DeadLetter | None:
    row = conn.execute("SELECT * FROM dead_letters WHERE id = ?", (dl_id,)).fetchone()
    return _row_to_dead_letter(row) if row else None


def list_dead_letters(conn: sqlite3.Connection, group: str, include_redriven: bool = False) -> list[DeadLetter]:
    if include_redriven:
        rows = conn.execute(
            "SELECT * FROM dead_letters WHERE group_name = ? ORDER BY failed_at", (group,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM dead_letters WHERE group_name = ? AND redriven = 0 ORDER BY failed_at", (group,)
        ).fetchall()
    return [_row_to_dead_letter(r) for r in rows]


def mark_redriven(conn: sqlite3.Connection, dl_id: str, redriven_as_event_id: str) -> None:
    conn.execute(
        "UPDATE dead_letters SET redriven = 1, redriven_as_event_id = ? WHERE id = ?",
        (redriven_as_event_id, dl_id),
    )


def count_deliveries_for_event_in_epoch(conn: sqlite3.Connection, group: str, event_id: str, epoch: int) -> int:
    # Scoped to the partition's current epoch, not every delivery this
    # event has ever had: a replay bumps the epoch specifically so an
    # event that already burned its retry budget before being
    # dead-lettered gets a fresh one on redelivery, rather than being
    # sent right back to the DLQ on its first re-poll.
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM deliveries WHERE group_name = ? AND event_id = ? AND epoch = ?",
        (group, event_id, epoch),
    ).fetchone()
    return row["n"]
