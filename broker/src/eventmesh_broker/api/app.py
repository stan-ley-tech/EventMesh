"""FastAPI app wiring. Route handlers do request parsing and status-code
mapping only; every rule about what's allowed to happen next lives in
the service layer. Handlers are plain `def`, not `async def` - the
service's SQLite calls are synchronous, and Starlette runs sync route
handlers in a thread pool, which is what keeps a slow database call from
blocking the event loop.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .. import errors
from ..core.service import EventMeshService
from ..models import RetryPolicy
from . import schemas, serialize

NOT_FOUND = (
    errors.TopicNotFound,
    errors.SchemaNotFound,
    errors.GroupNotFound,
    errors.DeliveryNotFound,
    errors.EventNotFound,
    errors.DeadLetterNotFound,
)
CONFLICT = (
    errors.TopicAlreadyExists,
    errors.GroupAlreadyExists,
    errors.SchemaConflict,
    errors.LeaseMismatch,
    errors.AlreadyRedriven,
)


def _status_for(exc: errors.EventMeshError) -> int:
    if isinstance(exc, NOT_FOUND):
        return 404
    if isinstance(exc, CONFLICT):
        return 409
    if isinstance(exc, errors.SchemaValidationError):
        return 422
    if isinstance(exc, errors.InvalidArgument):
        return 400
    return 500


def create_app(service: EventMeshService) -> FastAPI:
    app = FastAPI(title="EventMesh")

    @app.exception_handler(errors.EventMeshError)
    def handle_eventmesh_error(request: Request, exc: errors.EventMeshError):
        return JSONResponse(
            status_code=_status_for(exc),
            content={"error": str(exc), "type": type(exc).__name__},
        )

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.post("/v1/topics", status_code=201)
    def create_topic(req: schemas.CreateTopicRequest):
        return serialize.topic(service.create_topic(req.name, req.partition_count))

    @app.get("/v1/topics")
    def list_topics():
        return [serialize.topic(t) for t in service.list_topics()]

    @app.get("/v1/topics/{name}")
    def get_topic(name: str):
        return serialize.topic(service.get_topic(name))

    @app.post("/v1/topics/{name}/schemas", status_code=201)
    def register_schema(name: str, req: schemas.RegisterSchemaRequest):
        return serialize.schema_version(service.register_schema(name, req.version, req.json_schema))

    @app.get("/v1/topics/{name}/schemas")
    def list_schemas(name: str):
        return [serialize.schema_version(sv) for sv in service.list_schemas(name)]

    @app.get("/v1/topics/{name}/schemas/{version}")
    def get_schema(name: str, version: int):
        return serialize.schema_version(service.get_schema(name, version))

    @app.post("/v1/topics/{name}/events", status_code=201)
    def publish(name: str, req: schemas.PublishRequest):
        event = service.publish(
            name,
            req.payload,
            schema_version=req.schema_version,
            key=req.key,
            dedup_key=req.dedup_key,
            headers=req.headers,
            deliver_after=req.deliver_after,
        )
        return serialize.event(event)

    @app.get("/v1/events/{event_id}")
    def get_event(event_id: str):
        return serialize.event(service.get_event(event_id))

    @app.get("/v1/events/{event_id}/history")
    def event_history(event_id: str):
        return [serialize.history_entry(h) for h in service.get_history(event_id)]

    @app.post("/v1/groups", status_code=201)
    def create_group(req: schemas.CreateGroupRequest):
        retry_policy = RetryPolicy(**req.retry_policy.model_dump()) if req.retry_policy else None
        group = service.create_group(
            req.name,
            req.topic,
            filter_headers=req.filter_headers,
            retry_policy=retry_policy,
            delivery_lease_seconds=req.delivery_lease_seconds,
            worker_lease_seconds=req.worker_lease_seconds,
            start_from=req.start_from,
        )
        return serialize.group(group)

    @app.get("/v1/groups/{name}")
    def get_group(name: str):
        return serialize.group(service.get_group(name))

    @app.post("/v1/groups/{name}/poll")
    def poll(name: str, req: schemas.PollRequest):
        return [serialize.delivered(d) for d in service.poll(name, req.worker_id, max_events=req.max_events)]

    @app.post("/v1/deliveries/{delivery_id}/heartbeat")
    def heartbeat_delivery(delivery_id: str, req: schemas.AckRequest):
        service.heartbeat_delivery(delivery_id, req.worker_id)
        return {"status": "extended"}

    @app.post("/v1/deliveries/{delivery_id}/ack")
    def ack(delivery_id: str, req: schemas.AckRequest):
        service.ack(delivery_id, req.worker_id)
        return {"status": "acked"}

    @app.post("/v1/deliveries/{delivery_id}/nack")
    def nack(delivery_id: str, req: schemas.NackRequest):
        service.nack(delivery_id, req.worker_id, req.reason)
        return {"status": "nacked"}

    @app.post("/v1/groups/{name}/replay")
    def replay(name: str, req: schemas.ReplayRequest):
        service.replay(name, partition=req.partition, to_offset=req.to_offset, earliest=req.earliest)
        return {"status": "replayed"}

    @app.get("/v1/groups/{name}/dlq")
    def list_dlq(name: str):
        return [serialize.dead_letter(dl) for dl in service.list_dead_letters(name)]

    @app.post("/v1/groups/{name}/dlq/{dead_letter_id}/redrive")
    def redrive(name: str, dead_letter_id: str):
        return serialize.event(service.redrive(name, dead_letter_id))

    @app.get("/v1/groups/{name}/stats")
    def stats(name: str):
        return service.get_group_stats(name)

    return app
