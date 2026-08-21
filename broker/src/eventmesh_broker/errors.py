class EventMeshError(Exception):
    pass


class InvalidArgument(EventMeshError):
    pass


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
