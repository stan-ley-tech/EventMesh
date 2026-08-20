from pydantic import BaseModel


class EventSchema(BaseModel):
    """Base class for defining an event's payload shape. Subclass it
    like any Pydantic model; `json_schema()` is what the SDK sends to
    the broker when registering a version, and an instance's
    `model_dump(mode="json")` is what gets published as the payload.
    """

    @classmethod
    def json_schema(cls) -> dict:
        return cls.model_json_schema()
