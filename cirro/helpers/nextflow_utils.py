import re
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from cirro.sdk.task import DataPortalTask


def find_primary_failed_task(  # NOSONAR
    tasks: List['DataPortalTask'],
    execution_log: str
) -> Optional['DataPortalTask']:
    """
    Identify the root-cause failed task in a Nextflow workflow execution.

    Strategy:
    1. Filter tasks where status == "FAILED" and exit_code is not None and != 0.
    2. If none, fall back to any task with status == "FAILED".
    3. Parse execution_log for "Error executing process > 'TASK_NAME'" to cross-reference
       the task list (exact match first, then substring match).
    4. Fall back to the FAILED task with the lowest task_id (ran earliest).

    Returns None if no failed task is found.
    """
    # Step 1: tasks that actually failed with a non-zero exit code
    hard_failed = [
        t for t in tasks
        if t.status == "FAILED" and t.exit_code is not None and t.exit_code != 0
    ]

    # Step 2: fall back to any FAILED task if the above is empty
    candidate_pool = hard_failed if hard_failed else [t for t in tasks if t.status == "FAILED"]

    if not candidate_pool:
        return None

    if len(candidate_pool) == 1:
        return candidate_pool[0]

    # Step 3: try to cross-reference the execution log
    log_match = re.search(r"Error executing process > '([^']+)'", execution_log)
    if log_match:
        log_task_name = log_match.group(1)
        # Exact match first
        for task in candidate_pool:
            if task.name == log_task_name:
                return task
        # Partial match
        for task in candidate_pool:
            if log_task_name in task.name or task.name in log_task_name:
                return task

    # Step 4: fall back to earliest failing task
    return min(candidate_pool, key=lambda t: t.task_id)
