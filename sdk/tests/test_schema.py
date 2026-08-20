from eventmesh import EventSchema


class OrderCreated(EventSchema):
    order_id: str
    amount: float


class OrderCreatedV2(EventSchema):
    order_id: str
    amount: float
    currency: str = "USD"


def test_json_schema_has_required_fields():
    schema = OrderCreated.json_schema()
    assert schema["type"] == "object"
    assert set(schema["required"]) == {"order_id", "amount"}
    assert schema["properties"]["order_id"]["type"] == "string"
    assert schema["properties"]["amount"]["type"] == "number"


def test_additive_field_with_default_is_not_required():
    schema = OrderCreatedV2.json_schema()
    assert "currency" in schema["properties"]
    assert "currency" not in schema["required"]


def test_instance_serializes_to_plain_dict_payload():
    event = OrderCreated(order_id="o1", amount=9.99)
    payload = event.model_dump(mode="json")
    assert payload == {"order_id": "o1", "amount": 9.99}
