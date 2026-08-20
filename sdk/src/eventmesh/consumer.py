import logging
import os
import socket
import threading
import uuid
from collections import OrderedDict
from typing import Callable, Optional

from .client import Client, Delivery
from .errors import LeaseMismatch

logger = logging.getLogger("eventmesh.consumer")


class Context:
    """Passed to the handler alongside the event. A handler that just
    processes the event and returns gets an automatic ack; one that
    raises gets an automatic nack with the exception message. Calling
    ctx.ack() / ctx.nack() explicitly inside the handler is also fine
    and takes precedence - useful when "did this succeed" is a business
    decision rather than something an exception would naturally signal.
    """

    def __init__(self, worker: "ConsumerGroupWorker", delivery: Delivery):
        self.event = delivery.event
        self.attempt = delivery.attempt
        self._worker = worker
        self._delivery = delivery
        self._resolved = False

    def ack(self) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._worker._ack(self._delivery)

    def nack(self, reason: str = "") -> None:
        if self._resolved:
            return
        self._resolved = True
        self._worker._nack(self._delivery, reason)


class _SeenEvents:
    """Bounded in-memory record of event IDs this worker process has
    already handled. It lets a handler skip reprocessing its side
    effect on a same-process redelivery, but it holds nothing across a
    restart - a consumer that needs idempotency to survive a crash has
    to keep its own persisted record keyed by event.id, same as it
    would need to protect any other at-least-once delivery system.
    """

    def __init__(self, max_size: int = 10_000):
        self._max_size = max_size
        self._seen: "OrderedDict[str, None]" = OrderedDict()

    def already_processed(self, event_id: str) -> bool:
        return event_id in self._seen

    def mark_processed(self, event_id: str) -> None:
        self._seen[event_id] = None
        self._seen.move_to_end(event_id)
        while len(self._seen) > self._max_size:
            self._seen.popitem(last=False)


class ConsumerGroupWorker:
    """One worker in a consumer group. Polls for events on whichever
    partitions the broker currently assigns it, runs the registered
    handler, and reports the result back. Handles heartbeating a
    delivery lease in the background while the handler runs, so a slow
    handler doesn't get its event reclaimed out from under it.
    """

    def __init__(
        self,
        client: Client,
        group: str,
        worker_id: Optional[str] = None,
        poll_interval: float = 1.0,
        heartbeat_interval: float = 10.0,
        idempotent: bool = True,
    ):
        self.client = client
        self.group = group
        self.worker_id = worker_id or self._default_worker_id()
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self._handler: Optional[Callable] = None
        self._stop = threading.Event()
        self._seen = _SeenEvents() if idempotent else None

    @staticmethod
    def _default_worker_id() -> str:
        return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    def handler(self, fn: Callable):
        self._handler = fn
        return fn

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        if self._handler is None:
            raise RuntimeError("no handler registered - use @worker.handler before calling run()")
        logger.info("worker %s starting for group %s", self.worker_id, self.group)
        try:
            while not self._stop.is_set():
                deliveries = self.client.poll(self.group, self.worker_id, max_events=1)
                if not deliveries:
                    self._stop.wait(self.poll_interval)
                    continue
                for delivery in deliveries:
                    self._process(delivery)
        except KeyboardInterrupt:
            logger.info("worker %s stopping", self.worker_id)

    def _process(self, delivery: Delivery) -> None:
        if self._seen is not None and self._seen.already_processed(delivery.event.id):
            logger.info("event %s already processed by this worker, skipping and acking", delivery.event.id)
            self._ack(delivery)
            return

        stop_heartbeat = threading.Event()
        hb_thread = threading.Thread(target=self._heartbeat_loop, args=(delivery, stop_heartbeat), daemon=True)
        hb_thread.start()

        ctx = Context(self, delivery)
        try:
            self._handler(delivery.event, ctx)
        except Exception as exc:
            stop_heartbeat.set()
            hb_thread.join()
            logger.warning("handler failed for event %s (attempt %d): %s", delivery.event.id, delivery.attempt, exc)
            ctx.nack(str(exc))
            return

        stop_heartbeat.set()
        hb_thread.join()
        ctx.ack()

    def _heartbeat_loop(self, delivery: Delivery, stop_event: threading.Event) -> None:
        while not stop_event.wait(self.heartbeat_interval):
            try:
                self.client.heartbeat_delivery(delivery.delivery_id, self.worker_id)
            except LeaseMismatch:
                return

    def _ack(self, delivery: Delivery) -> None:
        try:
            self.client.ack(delivery.delivery_id, self.worker_id)
        except LeaseMismatch:
            logger.warning("ack for delivery %s failed: lease already reclaimed", delivery.delivery_id)
        if self._seen is not None:
            self._seen.mark_processed(delivery.event.id)

    def _nack(self, delivery: Delivery, reason: str) -> None:
        try:
            self.client.nack(delivery.delivery_id, self.worker_id, reason)
        except LeaseMismatch:
            logger.warning("nack for delivery %s failed: lease already reclaimed", delivery.delivery_id)
