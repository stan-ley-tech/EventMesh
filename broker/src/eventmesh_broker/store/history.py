from __future__ import annotations

import json
import sqlite3

from .. import timeutil
from ..models import HistoryEntry


def append(conn: sqlite3.Connection, event_id: str, entry_type: str, detail: dict | None = None) -> None:
    row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM history WHERE event_id = ?", (event_id,)).fetchone()
    conn.execute(
        "INSERT INTO history (event_id, seq, type, detail, created_at) VALUES (?, ?, ?, ?, ?)",
        (event_id, row["next_seq"], entry_type, json.dumps(detail or {}), timeutil.fmt(timeutil.now())),
    )


def _row_to_entry(row: sqlite3.Row) -> HistoryEntry:
    return HistoryEntry(
        id=row["id"],
        event_id=row["event_id"],
        seq=row["seq"],
        type=row["type"],
        detail=json.loads(row["detail"]),
        created_at=timeutil.parse(row["created_at"]),
    )


def list_for_event(conn: sqlite3.Connection, event_id: str) -> list[HistoryEntry]:
    rows = conn.execute(
        "SELECT * FROM history WHERE event_id = ? ORDER BY seq ASC", (event_id,)
    ).fetchall()
    return [_row_to_entry(r) for r in rows]
