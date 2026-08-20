from datetime import datetime
from typing import Optional, Union

from .client import Client, Event
from .schema import EventSchema


class Producer:
    """Publishes to one topic. Accepts either a plain dict payload or an
    EventSchema instance - the latter is serialized with
    `model_dump(mode="json")` before being sent.
    """

    def __init__(self, client: Client, topic: str):
        self.client = client
        self.topic = topic

    def publish(
        self,
        event: Union[dict, EventSchema],
        *,
        schema_version: Optional[int] = None,
        key: Optional[str] = None,
        dedup_key: Optional[str] = None,
        headers: Optional[dict] = None,
        deliver_after: Optional[datetime] = None,
    ) -> Event:
        return self.client.publish(
            self.topic,
            event,
            schema_version=schema_version,
            key=key,
            dedup_key=dedup_key,
            headers=headers,
            deliver_after=deliver_after,
        )
