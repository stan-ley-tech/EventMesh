from __future__ import annotations

from datetime import datetime, timezone


def now() -> datetime:
    return datetime.now(timezone.utc)


def fmt(t: datetime) -> str:
    # A naive datetime (no tzinfo) is treated as already being UTC, not
    # converted from the host's local timezone - astimezone() would
    # otherwise silently assume local time for naive input, which is
    # almost never what a caller means and shifts deliver_after by
    # whatever the server's UTC offset happens to be.
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc).isoformat()


def parse(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s)
