"""Domain types the broker persists. Nothing here talks to storage or
the network - it's the vocabulary the store and service layers share.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class DeliveryStatus(str, Enum):
    DELIVERED = "DELIVERED"
    ACKED = "ACKED"
    NACKED = "NACKED"
    EXPIRED = "EXPIRED"


@dataclass
class RetryPolicy:
    max_attempts: int = 5
    backoff_base_ms: int = 1000
    backoff_multiplier: float = 2.0
    backoff_max_ms: int = 60_000
    jitter_fraction: float = 0.2

    def to_dict(self) -> dict:
        return {
            "max_attempts": self.max_attempts,
            "backoff_base_ms": self.backoff_base_ms,
            "backoff_multiplier": self.backoff_multiplier,
            "backoff_max_ms": self.backoff_max_ms,
            "jitter_fraction": self.jitter_fraction,
        }

    @staticmethod
    def from_dict(d: dict) -> "RetryPolicy":
        defaults = RetryPolicy()
        return RetryPolicy(
            max_attempts=d.get("max_attempts", defaults.max_attempts),
            backoff_base_ms=d.get("backoff_base_ms", defaults.backoff_base_ms),
            backoff_multiplier=d.get("backoff_multiplier", defaults.backoff_multiplier),
            backoff_max_ms=d.get("backoff_max_ms", defaults.backoff_max_ms),
            jitter_fraction=d.get("jitter_fraction", defaults.jitter_fraction),
        )


@dataclass
class Topic:
    name: str
    partition_count: int
    created_at: datetime


@dataclass
class SchemaVersion:
    topic: str
    version: int
    json_schema: dict
    created_at: datetime


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
    deliver_after: Optional[datetime]
    published_at: datetime


@dataclass
class ConsumerGroup:
    name: str
    topic: str
    filter_headers: dict
    retry_policy: RetryPolicy
    delivery_lease_seconds: int
    worker_lease_seconds: int
    created_at: datetime


@dataclass
class WorkerHeartbeat:
    group: str
    worker_id: str
    lease_expires_at: datetime


@dataclass
class PartitionAssignment:
    group: str
    topic: str
    partition: int
    worker_id: str
    assigned_at: datetime


@dataclass
class Offset:
    group: str
    topic: str
    partition: int
    committed_offset: int


@dataclass
class Delivery:
    id: str
    group: str
    topic: str
    partition: int
    event_id: str
    event_offset: int
    worker_id: str
    attempt: int
    status: DeliveryStatus
    lease_expires_at: datetime
    delivered_at: datetime


@dataclass
class DeadLetter:
    id: str
    group: str
    topic: str
    partition: int
    event_id: str
    event_offset: int
    reason: str
    attempts: int
    failed_at: datetime
    redriven: bool = False
    redriven_as_event_id: Optional[str] = None


@dataclass
class HistoryEntry:
    id: int
    event_id: str
    seq: int
    type: str
    detail: dict
    created_at: datetime


EVENT_PUBLISHED = "PUBLISHED"
EVENT_DELIVERED = "DELIVERED"
EVENT_ACKED = "ACKED"
EVENT_NACKED = "NACKED"
EVENT_RETRY_SCHEDULED = "RETRY_SCHEDULED"
EVENT_LEASE_EXPIRED = "LEASE_EXPIRED"
EVENT_DEAD_LETTERED = "DEAD_LETTERED"
EVENT_REDRIVEN = "REDRIVEN"
EVENT_REPLAYED = "REPLAYED"
EVENT_SKIPPED_BY_FILTER = "SKIPPED_BY_FILTER"
