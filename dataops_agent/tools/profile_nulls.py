"""Null profiling tool for CSV batch incidents."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from botocore.client import BaseClient

from config.settings import Settings, get_settings
from dataops_agent.tools.base import ToolResult, failure_result, require_context
from dataops_agent.tools.csv_source import (
    read_csv_sample,
    source_uri_from_context_or_incident,
)
from dataops_agent.tools.inspect_schema import NULL_MARKERS


def execute(
    incident: Any,
    context: Mapping[str, Any] | None = None,
    *,
    settings: Settings | None = None,
    s3_client: BaseClient | None = None,
    repository: Any | None = None,
) -> ToolResult:
    """Compute null counts and ratios per CSV column."""

    del repository
    tool_context = require_context(context)
    try:
        resolved_settings = settings or (
            get_settings() if "null_threshold" not in tool_context else None
        )
        threshold = _threshold_from_context(tool_context, resolved_settings)
        max_rows = _positive_int(tool_context.get("max_rows", 10_000), "max_rows")
        max_bytes = _positive_int(
            tool_context.get("max_bytes", 50 * 1024 * 1024),
            "max_bytes",
        )
        csv_content = _optional_text(tool_context.get("csv_content"), "csv_content")
        source_uri = source_uri_from_context_or_incident(incident, tool_context)
        sample = read_csv_sample(
            source_uri=source_uri,
            csv_content=csv_content,
            settings=resolved_settings,
            s3_client=s3_client,
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
        profile = profile_nulls(sample.rows, sample.headers)
        breaches = [
            {
                "column": column,
                "null_count": metrics["null_count"],
                "null_ratio": metrics["null_ratio"],
                "threshold": threshold,
            }
            for column, metrics in profile.items()
            if metrics["null_ratio"] > threshold
        ]
        data = {
            "tool": "profile_nulls",
            "source_uri": sample.source_uri,
            "total_rows": sample.total_rows,
            "sampled_rows": sample.sampled_rows,
            "truncated": sample.truncated,
            "null_threshold": threshold,
            "threshold_breached": len(breaches) > 0,
            "columns": profile,
            "threshold_breaches": breaches,
        }
    except Exception as exc:
        return failure_result(str(exc), {"tool": "profile_nulls"})

    if breaches:
        return ToolResult(
            success=False,
            data=data,
            error="null threshold breaches detected",
        )
    return ToolResult(success=True, data=data)


def profile_nulls(
    rows: list[Mapping[str, str]],
    headers: list[str],
) -> dict[str, dict[str, int | float]]:
    """Return null counts and ratios for each header."""

    total_rows = len(rows)
    profile: dict[str, dict[str, int | float]] = {}
    for header in headers:
        null_count = sum(1 for row in rows if _is_null(row.get(header, "")))
        ratio = 0.0 if total_rows == 0 else null_count / total_rows
        profile[header] = {
            "null_count": null_count,
            "non_null_count": total_rows - null_count,
            "null_ratio": round(ratio, 6),
        }
    return profile


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in NULL_MARKERS


def _threshold_from_context(
    context: Mapping[str, Any],
    settings: Settings | None,
) -> float:
    default_threshold = settings.MAX_NULL_PERCENTAGE if settings is not None else 0.15
    raw_threshold = context.get("null_threshold", default_threshold)
    if isinstance(raw_threshold, bool):
        raise TypeError("null_threshold must be numeric")
    threshold = float(raw_threshold)
    if threshold < 0.0 or threshold > 1.0:
        raise ValueError("null_threshold must be between 0.0 and 1.0")
    return threshold


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"{field_name} must be greater than 0")
    return parsed


def _optional_text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    return value
