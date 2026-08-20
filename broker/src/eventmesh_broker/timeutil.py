from __future__ import annotations

from datetime import datetime, timezone


def now() -> datetime:
    return datetime.now(timezone.utc)


def fmt(t: datetime) -> str:
    return t.astimezone(timezone.utc).isoformat()


def parse(s: str | None) -> datetime | None:
    if not s:
        return None
    return datetime.fromisoformat(s)
