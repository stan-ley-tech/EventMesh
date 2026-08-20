from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class CreateTopicRequest(BaseModel):
    name: str
    partition_count: int = 3


class RegisterSchemaRequest(BaseModel):
    version: int
    json_schema: dict


class PublishRequest(BaseModel):
    payload: dict
    schema_version: Optional[int] = None
    key: Optional[str] = None
    dedup_key: Optional[str] = None
    headers: dict = {}
    deliver_after: Optional[datetime] = None


class RetryPolicyRequest(BaseModel):
    max_attempts: int = 5
    backoff_base_ms: int = 1000
    backoff_multiplier: float = 2.0
    backoff_max_ms: int = 60_000
    jitter_fraction: float = 0.2


class CreateGroupRequest(BaseModel):
    name: str
    topic: str
    filter_headers: dict = {}
    retry_policy: Optional[RetryPolicyRequest] = None
    delivery_lease_seconds: int = 30
    worker_lease_seconds: int = 30
    start_from: str = "earliest"


class PollRequest(BaseModel):
    worker_id: str
    max_events: int = 1


class AckRequest(BaseModel):
    worker_id: str


class NackRequest(BaseModel):
    worker_id: str
    reason: str = ""


class ReplayRequest(BaseModel):
    partition: Optional[int] = None
    to_offset: Optional[int] = None
    earliest: bool = False
