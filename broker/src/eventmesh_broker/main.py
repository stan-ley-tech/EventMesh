from __future__ import annotations

import logging
import os

import uvicorn

from .api.app import create_app
from .core.scheduler import Scheduler
from .core.service import EventMeshService
from .db import Database


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    db_path = os.environ.get("EVENTMESH_DB_PATH", "eventmesh.db")
    host = os.environ.get("EVENTMESH_HOST", "0.0.0.0")
    port = int(os.environ.get("EVENTMESH_PORT", "8000"))
    sweep_interval = float(os.environ.get("EVENTMESH_SWEEP_INTERVAL_SECONDS", "1.0"))

    db = Database(db_path)
    service = EventMeshService(db)
    scheduler = Scheduler(service, interval_seconds=sweep_interval)
    scheduler.start()

    app = create_app(service)

    try:
        logging.info("eventmesh-broker listening on %s:%s (db=%s)", host, port, db_path)
        uvicorn.run(app, host=host, port=port)
    finally:
        scheduler.stop()
        db.close()


if __name__ == "__main__":
    main()
