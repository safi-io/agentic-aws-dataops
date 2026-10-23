"""Shared contracts and helpers for allowlisted DataOps tools."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


JsonObject = dict[str, Any]


class ToolExecutionError(RuntimeError):
    """Raised when a tool cannot complete due to invalid input or infrastructure."""


class ToolResult(BaseModel):
    """Structured result returned by every allowlisted tool."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    success: bool
    data: JsonObject = Field(default_factory=dict)
    error: str | None = None

    @model_validator(mode="after")
    def _validate_error_contract(self) -> "ToolResult":
        if self.success and self.error is not None:
            raise ValueError("successful tool results must not include error")
        if not self.success and (self.error is None or self.error.strip() == ""):
            raise ValueError("failed tool results must include error")
        object.__setattr__(self, "data", ensure_json_object(self.data, "data"))
        return self

    def repository_payload(self, *, phase: str, tool_name: str) -> JsonObject:
        """Return a JSON-safe payload for durable persistence."""

        return ensure_json_object(
            {
                "phase": require_non_empty(phase, "phase"),
                "tool_name": require_non_empty(tool_name, "tool_name"),
                "success": self.success,
                "data": self.data,
                "error": self.error,
                "recorded_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
            },
            "tool_result",
        )


def success_result(data: Mapping[str, Any] | None = None) -> ToolResult:
    """Build a successful tool result."""

    return ToolResult(success=True, data=dict(data or {}), error=None)


def failure_result(
    error: str,
    data: Mapping[str, Any] | None = None,
) -> ToolResult:
    """Build a failed tool result."""

    return ToolResult(success=False, data=dict(data or {}), error=error)


def require_context(context: Mapping[str, Any] | None) -> JsonObject:
    """Normalize optional tool context into a plain dictionary."""

    if context is None:
        return {}
    if not isinstance(context, Mapping):
        raise TypeError("tool context must be a mapping")
    normalized: JsonObject = {}
    for key, value in context.items():
        if not isinstance(key, str) or key.strip() == "":
            raise ValueError("tool context keys must be non-empty strings")
        normalized[key] = value
    return normalized


def require_non_empty(value: str | None, field_name: str) -> str:
    """Return a stripped non-empty string or raise a precise validation error."""

    if value is None:
        raise ValueError(f"{field_name} is required")
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if normalized == "":
        raise ValueError(f"{field_name} cannot be empty")
    return normalized


def optional_string(value: Any, field_name: str) -> str | None:
    """Normalize an optional string field."""

    if value is None:
        return None
    return require_non_empty(value, field_name)


def incident_field(incident: Any, field_name: str) -> Any:
    """Fetch an incident attribute with a clear failure when the object is invalid."""

    if not hasattr(incident, field_name):
        raise TypeError(f"incident is missing required field {field_name}")
    return getattr(incident, field_name)


def incident_snapshot(incident: Any) -> JsonObject:
    """Return a compact JSON-safe incident snapshot."""

    fields = (
        "incident_id",
        "pipeline_name",
        "dagster_run_id",
        "source_uri",
        "incident_type",
        "severity",
        "error_summary",
        "status",
        "selected_action",
        "decision_confidence",
        "created_at",
        "updated_at",
    )
    snapshot: JsonObject = {}
    for field_name in fields:
        value = getattr(incident, field_name, None)
        if isinstance(value, datetime):
            snapshot[field_name] = value.isoformat()
        elif isinstance(value, date):
            snapshot[field_name] = value.isoformat()
        else:
            snapshot[field_name] = value
    return ensure_json_object(snapshot, "incident")


def ensure_json_object(value: Mapping[str, Any], path: str) -> JsonObject:
    """Validate and normalize a JSON object."""

    if not isinstance(value, Mapping):
        raise TypeError(f"{path} must be a JSON object")
    normalized: JsonObject = {}
    for key, nested_value in value.items():
        if not isinstance(key, str) or key.strip() == "":
            raise ValueError(f"{path} contains a non-string or empty object key")
        normalized[key] = ensure_json_value(nested_value, f"{path}.{key}")
    return normalized


def ensure_json_value(value: Any, path: str) -> Any:
    """Validate and normalize a JSON-serializable value."""

    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return ensure_json_object(value, path)
    if isinstance(value, list | tuple):
        return [
            ensure_json_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise TypeError(f"{path} contains a value that is not JSON serializable")
