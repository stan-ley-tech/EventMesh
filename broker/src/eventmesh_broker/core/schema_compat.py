from __future__ import annotations


def check_backward_compatible(old_schema: dict, new_schema: dict) -> tuple[bool, str]:
    """A new schema version is backward compatible with the one before
    it if every consumer reading against the old schema's expectations
    still finds what it's looking for in events written against the
    new one. Concretely: nothing required before may be removed or
    demoted to optional, and no shared property may change type. New
    properties, required or not, are always fine to add - a consumer
    that doesn't know about them simply doesn't look for them.
    """
    old_props = old_schema.get("properties", {})
    new_props = new_schema.get("properties", {})
    old_required = set(old_schema.get("required", []))
    new_required = set(new_schema.get("required", []))

    for name in old_required:
        if name not in new_props:
            return False, f"required property {name!r} was removed"
        if name not in new_required:
            return False, f"property {name!r} is no longer required"

    for name, old_prop in old_props.items():
        new_prop = new_props.get(name)
        if new_prop is None:
            continue
        old_type = old_prop.get("type")
        new_type = new_prop.get("type")
        if old_type and new_type and old_type != new_type:
            return False, f"property {name!r} changed type from {old_type!r} to {new_type!r}"

    return True, ""
