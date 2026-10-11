"""Tool: retry_pipeline - Bounded, idempotent retry execution for transient errors."""

from typing import Any, Dict, Optional
import time

MAX_ALLOWED_RETRIES = 2


def retry_pipeline(
    pipeline_name: str,
    incident_id: str,
    current_retry_count: int = 0,
    max_retries: int = MAX_ALLOWED_RETRIES
) -> Dict[str, Any]:
    """Retries a transient pipeline execution within strictly bounded limits."""
    if current_retry_count >= max_retries:
        return {
            "status": "failed",
            "retried": False,
            "reason": f"Retry limit reached ({current_retry_count}/{max_retries})",
            "escalation_required": True,
        }

    new_retry_count = current_retry_count + 1
    # Simulate bounded backoff
    backoff_seconds = 2 ** new_retry_count

    return {
        "status": "success",
        "retried": True,
        "pipeline_name": pipeline_name,
        "incident_id": incident_id,
        "previous_retry_count": current_retry_count,
        "new_retry_count": new_retry_count,
        "backoff_applied_seconds": backoff_seconds,
        "confirmation": f"Dispatched idempotent retry attempt {new_retry_count} of {max_retries} for {pipeline_name}.",
    }

