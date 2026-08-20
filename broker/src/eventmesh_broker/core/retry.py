from __future__ import annotations

import random

from ..models import RetryPolicy


def backoff_ms(policy: RetryPolicy, attempt: int) -> float:
    """Delay before retrying after `attempt` (1-indexed) has failed:
    base * multiplier^(attempt-1), capped, then jittered by
    +/- jitter_fraction.
    """
    delay = policy.backoff_base_ms * (policy.backoff_multiplier ** (attempt - 1))
    delay = min(delay, policy.backoff_max_ms)

    if policy.jitter_fraction > 0:
        jitter = delay * policy.jitter_fraction
        delay += random.uniform(-jitter, jitter)
        delay = max(delay, 0)

    return delay
