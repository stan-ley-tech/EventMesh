"""Background sweep that reclaims delivery leases a worker never
released. It's the entire crash-recovery mechanism for consumers: if a
worker is killed mid-processing, it simply stops heartbeating and
acking, its delivery lease lapses, and this loop notices and applies
the same retry-or-dead-letter path a worker's own nack would have taken.
"""

from __future__ import annotations

import logging
import threading

from .service import EventMeshService

logger = logging.getLogger("eventmesh_broker.scheduler")


class Scheduler:
    def __init__(self, service: EventMeshService, interval_seconds: float = 1.0):
        self.service = service
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.service.reclaim_expired_deliveries()
            except Exception:
                logger.exception("scheduler tick failed")
