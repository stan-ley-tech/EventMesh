from __future__ import annotations


def compute_assignment(partition_count: int, worker_ids: list[str]) -> dict[int, str]:
    """Deterministic partition -> worker_id mapping given the set of
    currently alive workers. Sorting worker_ids before assigning means
    the same alive set always produces the same assignment, so a
    rebalance triggered by an unrelated event (a worker's heartbeat
    landing a moment later) doesn't reshuffle ownership unnecessarily.
    """
    if not worker_ids:
        return {}
    workers = sorted(worker_ids)
    return {partition: workers[partition % len(workers)] for partition in range(partition_count)}
