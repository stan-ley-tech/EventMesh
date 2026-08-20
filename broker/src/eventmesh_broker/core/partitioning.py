from __future__ import annotations

import hashlib


def partition_for_key(key: str, partition_count: int) -> int:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % partition_count
