"""Retry gating tool for failed pipeline incidents."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from botocore.client import BaseClient

from config.settings import Settings, get_settings
from dataops_agent.incident_state import DataOpsIncident, IncidentRepository
from dataops_agent.tools.base import ToolResult, failure_result, require_context


def execute(
    incident: Any,
    context: Mapping[str, Any] | None = None,
    *,
    settings: Settings | None = None,
    s3_client: BaseClient | None = None,
    repository: IncidentRepository | None = None,
) -> ToolResult:
    """Permit a deterministic retry only when retry attempts are not exhausted."""

    del s3_client
    tool_context = require_context(context)
    try:
        resolved_settings = settings or get_settings()
        max_retry_count = _max_retry_count(tool_context, resolved_settings)
        repo = repository or IncidentRepository()

        incident_id = str(_field(incident, "incident_id"))
        current = repo.get_incident(incident_id) or incident
        current_existing_result = getattr(current, "action_result", None)
        if _is_current_retry_result(current_existing_result):
            return ToolResult(
                success=bool(current_existing_result.get("success")),
                data=dict(current_existing_result.get("data") or {}),
                error=current_existing_result.get("error"),
            )

        related_incidents = repo.list_incidents(
            pipeline_name=str(_field(current, "pipeline_name")),
            limit=1000,
        )
        attempts = count_retry_attempts(current, related_incidents)
        next_attempt = attempts + 1
        data = {
            "tool": "retry_pipeline",
            "retry_permitted": attempts < max_retry_count,
            "retry_attempt": next_attempt,
            "prior_retry_attempts": attempts,
            "max_retry_count": max_retry_count,
            "pipeline_name": str(_field(current, "pipeline_name")),
            "source_uri": getattr(current, "source_uri", None),
            "incident_type": str(_field(current, "incident_type")),
            "dagster_run_id": getattr(current, "dagster_run_id", None),
            "rerun_config": {
                "incident_id": incident_id,
                "source_uri": getattr(current, "source_uri", None),
                "retry_attempt": next_attempt,
            },
        }
    except Exception as exc:
        return failure_result(str(exc), {"tool": "retry_pipeline"})

    if attempts >= max_retry_count:
        return ToolResult(
            success=False,
            data=data,
            error="retry exhaustion reached",
        )
    return ToolResult(success=True, data=data)


def count_retry_attempts(
    current: Any,
    related_incidents: Sequence[DataOpsIncident],
) -> int:
    """Count completed retry actions for incidents matching the current failure key."""

    pipeline_name = str(_field(current, "pipeline_name"))
    source_uri = getattr(current, "source_uri", None)
    incident_type = str(_field(current, "incident_type"))
    current_id = str(_field(current, "incident_id"))

    attempts = 0
    for candidate in related_incidents:
        if str(getattr(candidate, "pipeline_name", "")) != pipeline_name:
            continue
        if getattr(candidate, "source_uri", None) != source_uri:
            continue
        if str(getattr(candidate, "incident_type", "")) != incident_type:
            continue
        action_result = getattr(candidate, "action_result", None)
        if str(getattr(candidate, "incident_id", "")) == current_id:
            if not _is_current_retry_result(action_result):
                continue
        if _is_successful_retry_action(candidate, action_result):
            attempts += 1
    return attempts


def _is_successful_retry_action(candidate: Any, action_result: Any) -> bool:
    if not isinstance(action_result, Mapping):
        return False
    data = action_result.get("data")
    tool_name = action_result.get("tool_name")
    if tool_name is None and isinstance(data, Mapping):
        tool_name = data.get("tool")
    success = action_result.get("success")
    return (
        getattr(candidate, "selected_action", None) == "retry_pipeline"
        and tool_name == "retry_pipeline"
        and success is True
    )


def _is_current_retry_result(action_result: Any) -> bool:
    if not isinstance(action_result, Mapping):
        return False
    data = action_result.get("data")
    tool_name = action_result.get("tool_name")
    if tool_name is None and isinstance(data, Mapping):
        tool_name = data.get("tool")
    return tool_name == "retry_pipeline" and isinstance(action_result.get("data"), Mapping)


def _max_retry_count(context: Mapping[str, Any], settings: Settings) -> int:
    raw_value = context.get("max_retry_count", settings.MAX_RETRY_COUNT)
    if isinstance(raw_value, bool):
        raise TypeError("max_retry_count must be an integer")
    parsed = int(raw_value)
    if parsed < 0:
        raise ValueError("max_retry_count must be greater than or equal to 0")
    return parsed


def _field(incident: Any, name: str) -> Any:
    if not hasattr(incident, name):
        raise TypeError(f"incident is missing required field {name}")
    value = getattr(incident, name)
    if value is None:
        raise ValueError(f"incident field {name} is required")
    return value
