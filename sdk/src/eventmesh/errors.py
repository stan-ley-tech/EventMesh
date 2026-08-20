class EventMeshError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class TopicNotFound(EventMeshError):
    pass


class TopicAlreadyExists(EventMeshError):
    pass


class SchemaNotFound(EventMeshError):
    pass


class SchemaConflict(EventMeshError):
    pass


class SchemaValidationError(EventMeshError):
    pass


class GroupNotFound(EventMeshError):
    pass


class GroupAlreadyExists(EventMeshError):
    pass


class EventNotFound(EventMeshError):
    pass


class DeliveryNotFound(EventMeshError):
    pass


class LeaseMismatch(EventMeshError):
    pass


class DeadLetterNotFound(EventMeshError):
    pass


class AlreadyRedriven(EventMeshError):
    pass


_BY_TYPE = {
    "TopicNotFound": TopicNotFound,
    "TopicAlreadyExists": TopicAlreadyExists,
    "SchemaNotFound": SchemaNotFound,
    "SchemaConflict": SchemaConflict,
    "SchemaValidationError": SchemaValidationError,
    "GroupNotFound": GroupNotFound,
    "GroupAlreadyExists": GroupAlreadyExists,
    "EventNotFound": EventNotFound,
    "DeliveryNotFound": DeliveryNotFound,
    "LeaseMismatch": LeaseMismatch,
    "DeadLetterNotFound": DeadLetterNotFound,
    "AlreadyRedriven": AlreadyRedriven,
}


def from_response(status_code: int, body: dict) -> EventMeshError:
    message = body.get("error", f"request failed with status {status_code}")
    error_type = body.get("type")
    cls = _BY_TYPE.get(error_type, EventMeshError)
    return cls(message, status_code)
