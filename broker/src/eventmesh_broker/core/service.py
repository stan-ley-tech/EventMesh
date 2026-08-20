"""Orchestration layer: every rule about what's allowed to happen next
lives here, composed from the store modules (persistence) and the pure
helpers in this package (partitioning, retry backoff, assignment,
schema compatibility). Nothing in here holds state beyond a database
handle and a small in-memory round-robin counter used only to spread
keyless publishes across partitions.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime
from typing import Optional

import jsonschema

from .. import timeutil
from ..db import Database
from ..errors import (
    AlreadyRedriven,
    DeadLetterNotFound,
    DeliveryNotFound,
    EventNotFound,
    GroupAlreadyExists,
    GroupNotFound,
    LeaseMismatch,
    SchemaConflict,
    SchemaNotFound,
    SchemaValidationError,
    TopicAlreadyExists,
    TopicNotFound,
)
from ..models import (
    ConsumerGroup,
    DeadLetter,
    Delivery,
    DeliveryStatus,
    Event,
    HistoryEntry,
    PartitionAssignment,
    RetryPolicy,
    SchemaVersion,
    Topic,
)
from ..models import (
    EVENT_ACKED,
    EVENT_DEAD_LETTERED,
    EVENT_DELIVERED,
    EVENT_NACKED,
    EVENT_PUBLISHED,
    EVENT_REDRIVEN,
    EVENT_REPLAYED,
    EVENT_RETRY_SCHEDULED,
    EVENT_SKIPPED_BY_FILTER,
)
from ..store import deliveries as deliveries_store
from ..store import events as events_store
from ..store import groups as groups_store
from ..store import history as history_store
from ..store import topics as topics_store
from . import retry as retry_calc
from .assignment import compute_assignment
from .partitioning import partition_for_key
from .schema_compat import check_backward_compatible

DEFAULT_DELIVERY_LEASE_SECONDS = 30
DEFAULT_WORKER_LEASE_SECONDS = 30


class Delivered:
    def __init__(self, delivery_id: str, event: Event, attempt: int):
        self.delivery_id = delivery_id
        self.event = event
        self.attempt = attempt


class EventMeshService:
    def __init__(self, db: Database):
        self.db = db
        self._round_robin: dict[str, int] = defaultdict(int)

    def create_topic(self, name: str, partition_count: int) -> Topic:
        with self.db.atomic() as conn:
            if topics_store.get_topic(conn, name) is not None:
                raise TopicAlreadyExists(name)
            topic = Topic(name=name, partition_count=partition_count, created_at=timeutil.now())
            topics_store.create_topic(conn, topic)
            return topic

    def get_topic(self, name: str) -> Topic:
        with self.db.read() as conn:
            topic = topics_store.get_topic(conn, name)
        if topic is None:
            raise TopicNotFound(name)
        return topic

    def list_topics(self) -> list[Topic]:
        with self.db.read() as conn:
            return topics_store.list_topics(conn)

    def register_schema(self, topic: str, version: int, json_schema: dict) -> SchemaVersion:
        jsonschema.Draft202012Validator.check_schema(json_schema)

        with self.db.atomic() as conn:
            if topics_store.get_topic(conn, topic) is None:
                raise TopicNotFound(topic)

            existing = topics_store.get_schema_version(conn, topic, version)
            if existing is not None:
                if existing.json_schema == json_schema:
                    return existing
                raise SchemaConflict(f"{topic} version {version} already registered with a different schema")

            latest = topics_store.get_latest_schema_version(conn, topic)
            expected_version = 1 if latest is None else latest.version + 1
            if version != expected_version:
                raise SchemaConflict(f"expected next version {expected_version} for {topic!r}, got {version}")

            if latest is not None:
                compatible, reason = check_backward_compatible(latest.json_schema, json_schema)
                if not compatible:
                    raise SchemaConflict(f"version {version} is not backward compatible with version {latest.version}: {reason}")

            sv = SchemaVersion(topic=topic, version=version, json_schema=json_schema, created_at=timeutil.now())
            topics_store.register_schema_version(conn, sv)
            return sv

    def get_schema(self, topic: str, version: int) -> SchemaVersion:
        with self.db.read() as conn:
            sv = topics_store.get_schema_version(conn, topic, version)
        if sv is None:
            raise SchemaNotFound(f"{topic} version {version}")
        return sv

    def list_schemas(self, topic: str) -> list[SchemaVersion]:
        with self.db.read() as conn:
            return topics_store.list_schema_versions(conn, topic)

    def publish(
        self,
        topic: str,
        payload: dict,
        schema_version: Optional[int] = None,
        key: Optional[str] = None,
        dedup_key: Optional[str] = None,
        headers: Optional[dict] = None,
        deliver_after: Optional[datetime] = None,
    ) -> Event:
        headers = headers or {}

        with self.db.atomic() as conn:
            if dedup_key is not None:
                existing = events_store.get_by_dedup_key(conn, topic, dedup_key)
                if existing is not None:
                    return existing

            t = topics_store.get_topic(conn, topic)
            if t is None:
                raise TopicNotFound(topic)

            if schema_version is None:
                latest = topics_store.get_latest_schema_version(conn, topic)
                if latest is None:
                    raise SchemaNotFound(f"{topic} has no registered schema")
                schema_version = latest.version
            sv = topics_store.get_schema_version(conn, topic, schema_version)
            if sv is None:
                raise SchemaNotFound(f"{topic} version {schema_version}")

            try:
                jsonschema.validate(payload, sv.json_schema)
            except jsonschema.ValidationError as exc:
                raise SchemaValidationError(str(exc.message)) from exc

            if key is not None:
                partition = partition_for_key(key, t.partition_count)
            else:
                partition = self._round_robin[topic] % t.partition_count
                self._round_robin[topic] += 1

            offset = events_store.next_offset(conn, topic, partition)
            event = Event(
                id=str(uuid.uuid4()),
                topic=topic,
                partition=partition,
                offset=offset,
                key=key,
                payload=payload,
                schema_version=schema_version,
                dedup_key=dedup_key,
                headers=headers,
                deliver_after=deliver_after,
                published_at=timeutil.now(),
            )
            events_store.insert_event(conn, event)
            history_store.append(conn, event.id, EVENT_PUBLISHED, {"topic": topic, "partition": partition, "offset": offset})
            return event

    def create_group(
        self,
        name: str,
        topic: str,
        filter_headers: Optional[dict] = None,
        retry_policy: Optional[RetryPolicy] = None,
        delivery_lease_seconds: int = DEFAULT_DELIVERY_LEASE_SECONDS,
        worker_lease_seconds: int = DEFAULT_WORKER_LEASE_SECONDS,
        start_from: str = "earliest",
    ) -> ConsumerGroup:
        with self.db.atomic() as conn:
            if groups_store.get_group(conn, name) is not None:
                raise GroupAlreadyExists(name)
            t = topics_store.get_topic(conn, topic)
            if t is None:
                raise TopicNotFound(topic)

            group = ConsumerGroup(
                name=name,
                topic=topic,
                filter_headers=filter_headers or {},
                retry_policy=retry_policy or RetryPolicy(),
                delivery_lease_seconds=delivery_lease_seconds,
                worker_lease_seconds=worker_lease_seconds,
                created_at=timeutil.now(),
            )
            groups_store.create_group(conn, group)

            if start_from == "latest":
                for partition in range(t.partition_count):
                    latest = events_store.latest_offset(conn, topic, partition)
                    if latest is not None:
                        groups_store.set_committed_offset(conn, name, topic, partition, latest)

            return group

    def get_group(self, name: str) -> ConsumerGroup:
        with self.db.read() as conn:
            group = groups_store.get_group(conn, name)
        if group is None:
            raise GroupNotFound(name)
        return group

    def heartbeat_worker(self, group_name: str, worker_id: str) -> list[int]:
        with self.db.atomic() as conn:
            group = groups_store.get_group(conn, group_name)
            if group is None:
                raise GroupNotFound(group_name)
            topic = topics_store.get_topic(conn, group.topic)

            now = timeutil.now()
            groups_store.delete_expired_workers(conn, group_name, now)
            lease_expiry = timeutil.now()
            groups_store.heartbeat_worker(
                conn, group_name, worker_id, self._add_seconds(lease_expiry, group.worker_lease_seconds)
            )

            alive = groups_store.list_alive_workers(conn, group_name, now)
            assignment = compute_assignment(topic.partition_count, alive)
            current = {a.partition: a.worker_id for a in groups_store.get_assignments(conn, group_name)}
            for partition, owner in assignment.items():
                if current.get(partition) != owner:
                    groups_store.set_assignment(
                        conn,
                        PartitionAssignment(group=group_name, topic=group.topic, partition=partition, worker_id=owner, assigned_at=now),
                    )

            return groups_store.get_partitions_for_worker(conn, group_name, worker_id)

    @staticmethod
    def _add_seconds(t: datetime, seconds: float) -> datetime:
        from datetime import timedelta

        return t + timedelta(seconds=seconds)

    def poll(self, group_name: str, worker_id: str, max_events: int = 1) -> list[Delivered]:
        with self.db.atomic() as conn:
            group = groups_store.get_group(conn, group_name)
            if group is None:
                raise GroupNotFound(group_name)

            now = timeutil.now()
            groups_store.delete_expired_workers(conn, group_name, now)
            groups_store.heartbeat_worker(conn, group_name, worker_id, self._add_seconds(now, group.worker_lease_seconds))

            t = topics_store.get_topic(conn, group.topic)
            alive = groups_store.list_alive_workers(conn, group_name, now)
            assignment = compute_assignment(t.partition_count, alive)
            current = {a.partition: a.worker_id for a in groups_store.get_assignments(conn, group_name)}
            for partition, owner in assignment.items():
                if current.get(partition) != owner:
                    groups_store.set_assignment(
                        conn,
                        PartitionAssignment(group=group_name, topic=group.topic, partition=partition, worker_id=owner, assigned_at=now),
                    )

            owned = groups_store.get_partitions_for_worker(conn, group_name, worker_id)
            delivered: list[Delivered] = []

            for partition in owned:
                if len(delivered) >= max_events:
                    break
                if deliveries_store.get_active_delivery_for_partition(conn, group_name, partition) is not None:
                    continue
                backoff_until = groups_store.get_backoff(conn, group_name, partition)
                if backoff_until is not None and backoff_until > now:
                    continue

                event = self._next_deliverable_event(conn, group, partition, now)
                if event is None:
                    continue

                attempt = deliveries_store.count_deliveries_for_event(conn, group_name, event.id) + 1
                delivery = Delivery(
                    id=str(uuid.uuid4()),
                    group=group_name,
                    topic=group.topic,
                    partition=partition,
                    event_id=event.id,
                    event_offset=event.offset,
                    worker_id=worker_id,
                    attempt=attempt,
                    status=DeliveryStatus.DELIVERED,
                    lease_expires_at=self._add_seconds(now, group.delivery_lease_seconds),
                    delivered_at=now,
                )
                deliveries_store.create_delivery(conn, delivery)
                history_store.append(conn, event.id, EVENT_DELIVERED, {"group": group_name, "worker": worker_id, "attempt": attempt})
                delivered.append(Delivered(delivery.id, event, attempt))

            return delivered

    def _next_deliverable_event(self, conn, group: ConsumerGroup, partition: int, now: datetime) -> Optional[Event]:
        offset = groups_store.get_committed_offset(conn, group.name, partition) + 1
        while True:
            event = events_store.get_event_by_offset(conn, group.topic, partition, offset)
            if event is None:
                return None
            if event.deliver_after is not None and event.deliver_after > now:
                return None
            if self._matches_filter(event, group.filter_headers):
                return event
            groups_store.set_committed_offset(conn, group.name, group.topic, partition, offset)
            history_store.append(conn, event.id, EVENT_SKIPPED_BY_FILTER, {"group": group.name})
            offset += 1

    @staticmethod
    def _matches_filter(event: Event, filter_headers: dict) -> bool:
        return all(event.headers.get(k) == v for k, v in filter_headers.items())

    def ack(self, delivery_id: str, worker_id: str) -> None:
        with self.db.atomic() as conn:
            delivery = deliveries_store.get_delivery(conn, delivery_id)
            if delivery is None:
                raise DeliveryNotFound(delivery_id)
            if delivery.status != DeliveryStatus.DELIVERED or delivery.worker_id != worker_id:
                raise LeaseMismatch(delivery_id)

            deliveries_store.set_status(conn, delivery_id, DeliveryStatus.ACKED)
            groups_store.set_committed_offset(conn, delivery.group, delivery.topic, delivery.partition, delivery.event_offset)
            groups_store.clear_backoff(conn, delivery.group, delivery.partition)
            history_store.append(conn, delivery.event_id, EVENT_ACKED, {"group": delivery.group, "worker": worker_id})

    def nack(self, delivery_id: str, worker_id: str, reason: str) -> None:
        with self.db.atomic() as conn:
            delivery = deliveries_store.get_delivery(conn, delivery_id)
            if delivery is None:
                raise DeliveryNotFound(delivery_id)
            if delivery.status != DeliveryStatus.DELIVERED or delivery.worker_id != worker_id:
                raise LeaseMismatch(delivery_id)

            deliveries_store.set_status(conn, delivery_id, DeliveryStatus.NACKED)
            history_store.append(conn, delivery.event_id, EVENT_NACKED, {"group": delivery.group, "worker": worker_id, "reason": reason})
            self._apply_retry_or_dlq(conn, delivery, reason, timeutil.now())

    def reclaim_expired_deliveries(self) -> None:
        with self.db.read() as conn:
            expired = deliveries_store.find_expired_deliveries(conn, timeutil.now())
        for delivery in expired:
            with self.db.atomic() as conn:
                fresh = deliveries_store.get_delivery(conn, delivery.id)
                if fresh is None or fresh.status != DeliveryStatus.DELIVERED:
                    continue
                now = timeutil.now()
                if fresh.lease_expires_at > now:
                    continue
                deliveries_store.set_status(conn, delivery.id, DeliveryStatus.EXPIRED)
                history_store.append(conn, fresh.event_id, EVENT_NACKED, {"group": fresh.group, "worker": fresh.worker_id, "reason": "delivery lease expired"})
                self._apply_retry_or_dlq(conn, fresh, "delivery lease expired", now)

    def _apply_retry_or_dlq(self, conn, delivery: Delivery, reason: str, now: datetime) -> None:
        group = groups_store.get_group(conn, delivery.group)
        if delivery.attempt < group.retry_policy.max_attempts:
            delay_ms = retry_calc.backoff_ms(group.retry_policy, delivery.attempt)
            not_before = self._add_seconds(now, delay_ms / 1000)
            groups_store.set_backoff(conn, delivery.group, delivery.partition, not_before)
            history_store.append(
                conn, delivery.event_id, EVENT_RETRY_SCHEDULED,
                {"group": delivery.group, "attempt": delivery.attempt, "max_attempts": group.retry_policy.max_attempts, "backoff_ms": delay_ms},
            )
            return

        dl = DeadLetter(
            id=str(uuid.uuid4()),
            group=delivery.group,
            topic=delivery.topic,
            partition=delivery.partition,
            event_id=delivery.event_id,
            event_offset=delivery.event_offset,
            reason=reason,
            attempts=delivery.attempt,
            failed_at=now,
        )
        deliveries_store.insert_dead_letter(conn, dl)
        groups_store.set_committed_offset(conn, delivery.group, delivery.topic, delivery.partition, delivery.event_offset)
        groups_store.clear_backoff(conn, delivery.group, delivery.partition)
        history_store.append(conn, delivery.event_id, EVENT_DEAD_LETTERED, {"group": delivery.group, "attempts": delivery.attempt, "reason": reason})

    def list_dead_letters(self, group_name: str) -> list[DeadLetter]:
        with self.db.read() as conn:
            if groups_store.get_group(conn, group_name) is None:
                raise GroupNotFound(group_name)
            return deliveries_store.list_dead_letters(conn, group_name)

    def redrive(self, group_name: str, dead_letter_id: str) -> Event:
        with self.db.atomic() as conn:
            dl = deliveries_store.get_dead_letter(conn, dead_letter_id)
            if dl is None or dl.group != group_name:
                raise DeadLetterNotFound(dead_letter_id)
            if dl.redriven:
                raise AlreadyRedriven(dead_letter_id)

            original = events_store.get_event(conn, dl.event_id)
            if original is None:
                raise EventNotFound(dl.event_id)

            t = topics_store.get_topic(conn, dl.topic)
            partition = original.partition
            offset = events_store.next_offset(conn, dl.topic, partition)
            new_event = Event(
                id=str(uuid.uuid4()),
                topic=dl.topic,
                partition=partition,
                offset=offset,
                key=original.key,
                payload=original.payload,
                schema_version=original.schema_version,
                dedup_key=None,
                headers=original.headers,
                deliver_after=None,
                published_at=timeutil.now(),
            )
            events_store.insert_event(conn, new_event)
            history_store.append(conn, new_event.id, EVENT_PUBLISHED, {"topic": dl.topic, "partition": partition, "offset": offset, "redrive_of": dl.event_id})

            deliveries_store.mark_redriven(conn, dead_letter_id, new_event.id)
            history_store.append(conn, dl.event_id, EVENT_REDRIVEN, {"group": group_name, "redriven_as": new_event.id})
            return new_event

    def replay(self, group_name: str, partition: Optional[int] = None, to_offset: Optional[int] = None, earliest: bool = False) -> None:
        with self.db.atomic() as conn:
            group = groups_store.get_group(conn, group_name)
            if group is None:
                raise GroupNotFound(group_name)
            t = topics_store.get_topic(conn, group.topic)

            partitions = [partition] if partition is not None else list(range(t.partition_count))
            for p in partitions:
                active = deliveries_store.get_active_delivery_for_partition(conn, group_name, p)
                if active is not None:
                    deliveries_store.set_status(conn, active.id, DeliveryStatus.EXPIRED)

                new_committed = -1 if earliest else (to_offset - 1 if to_offset is not None else -1)
                groups_store.set_committed_offset(conn, group_name, group.topic, p, new_committed)
                groups_store.clear_backoff(conn, group_name, p)

                first = events_store.get_event_by_offset(conn, group.topic, p, new_committed + 1)
                if first is not None:
                    history_store.append(conn, first.id, EVENT_REPLAYED, {"group": group_name, "partition": p, "from_offset": new_committed + 1})

    def get_history(self, event_id: str) -> list[HistoryEntry]:
        with self.db.read() as conn:
            if events_store.get_event(conn, event_id) is None:
                raise EventNotFound(event_id)
            return history_store.list_for_event(conn, event_id)

    def get_group_stats(self, group_name: str) -> dict:
        with self.db.read() as conn:
            group = groups_store.get_group(conn, group_name)
            if group is None:
                raise GroupNotFound(group_name)
            t = topics_store.get_topic(conn, group.topic)
            now = timeutil.now()
            alive_workers = set(groups_store.list_alive_workers(conn, group_name, now))
            assignments = {a.partition: a.worker_id for a in groups_store.get_assignments(conn, group_name)}
            dlq_count = len(deliveries_store.list_dead_letters(conn, group_name))

            partitions = []
            for p in range(t.partition_count):
                committed = groups_store.get_committed_offset(conn, group_name, p)
                latest = events_store.latest_offset(conn, group.topic, p)
                latest_offset = latest if latest is not None else -1
                partitions.append({
                    "partition": p,
                    "committed_offset": committed,
                    "latest_offset": latest_offset,
                    "lag": max(latest_offset - committed, 0),
                    "owner": assignments.get(p),
                    "owner_alive": assignments.get(p) in alive_workers,
                })

            return {
                "group": group_name,
                "topic": group.topic,
                "alive_workers": sorted(alive_workers),
                "dead_letter_count": dlq_count,
                "partitions": partitions,
            }
