"""Shared definitions for the order-events demo: the topic and group
names, and two versions of the event schema used to demonstrate schema
evolution. v2 adds `currency` with a default - additive, so it stays
backward compatible with v1 under the broker's compatibility rule.
"""

from eventmesh import EventSchema

TOPIC = "orders"
GROUP = "order_processors"


class OrderCreated(EventSchema):
    order_id: str
    amount: float


class OrderCreatedV2(EventSchema):
    order_id: str
    amount: float
    currency: str = "USD"
