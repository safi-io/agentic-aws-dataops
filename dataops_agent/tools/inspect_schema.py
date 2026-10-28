"""Schema inspection tool for CSV batch incidents."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from botocore.client import BaseClient

from config.settings import Settings
from dataops_agent.tools.base import ToolResult, failure_result, require_context
from dataops_agent.tools.csv_source import (
    read_csv_sample,
    source_uri_from_context_or_incident,
)


DEFAULT_ORDER_SCHEMA: dict[str, str] = {
    "order_id": "integer",
    "customer_id": "integer",
    "order_date": "date",
    "status": "string",
    "amount": "float",
    "currency": "string",
}

NULL_MARKERS = {"", "null", "none", "nan", "na", "n/a"}
INTEGER_PATTERN = re.compile(r"^[+-]?\d+$")
TYPE_ALIASES: dict[str, str] = {
    "bigint": "integer",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "date",
    "datetime": "datetime",
    "decimal": "float",
    "double": "float",
    "float": "float",
    "int": "integer",
    "integer": "integer",
    "long": "integer",
    "number": "float",
    "numeric": "float",
    "str": "string",
    "string": "string",
    "text": "string",
    "timestamp": "datetime",
    "timestamp with time zone": "datetime",
    "varchar": "string",
}


def execute(
    incident: Any,
    context: Mapping[str, Any] | None = None,
    *,
    settings: Settings | None = None,
    s3_client: BaseClient | None = None,
    repository: Any | None = None,
) -> ToolResult:
    """Compare observed CSV schema against a target schema contract."""

    del repository
    tool_context = require_context(context)
    try:
        target_schema = normalize_schema_contract(
            tool_context.get("target_schema")
            or _incident_evidence_value(incident, "target_schema")
            or DEFAULT_ORDER_SCHEMA
        )
        observed_schema = _observed_schema(incident, tool_context, settings, s3_client)
        comparison = compare_schemas(target_schema, observed_schema)
    except Exception as exc:
        return failure_result(str(exc), {"tool": "inspect_schema"})

    schema_valid = (
        len(comparison["missing_columns"]) == 0
        and len(comparison["extra_columns"]) == 0
        and len(comparison["type_drift_columns"]) == 0
    )
    data = {
        "tool": "inspect_schema",
        "schema_valid": schema_valid,
        "target_schema": target_schema,
        "observed_schema": observed_schema,
        **comparison,
    }
    if schema_valid:
        return ToolResult(success=True, data=data)
    return ToolResult(
        success=False,
        data=data,
        error="schema contract violations detected",
    )


def normalize_schema_contract(value: Any) -> dict[str, str]:
    """Normalize supported schema contract formats into column -> type."""

    if isinstance(value, Mapping):
        normalized: dict[str, str] = {}
        for raw_name, raw_spec in value.items():
            column_name = _normalize_column_name(raw_name)
            normalized[column_name] = _extract_column_type(raw_spec, column_name)
        if not normalized:
            raise ValueError("target_schema cannot be empty")
        return normalized

    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        normalized = {}
        for index, item in enumerate(value):
            if not isinstance(item, Mapping):
                raise TypeError(f"target_schema[{index}] must be an object")
            if "name" not in item:
                raise ValueError(f"target_schema[{index}] is missing name")
            if "type" not in item:
                raise ValueError(f"target_schema[{index}] is missing type")
            column_name = _normalize_column_name(item["name"])
            normalized[column_name] = normalize_type(str(item["type"]))
        if not normalized:
            raise ValueError("target_schema cannot be empty")
        return normalized

    raise TypeError("target_schema must be a mapping or a list of column specs")


def compare_schemas(
    target_schema: Mapping[str, str],
    observed_schema: Mapping[str, str],
) -> dict[str, list[dict[str, str]] | list[str]]:
    """Return deterministic schema differences."""

    target_columns = list(target_schema.keys())
    observed_columns = list(observed_schema.keys())
    target_set = set(target_columns)
    observed_set = set(observed_columns)

    missing_columns = [column for column in target_columns if column not in observed_set]
    extra_columns = [column for column in observed_columns if column not in target_set]
    type_drift_columns = []

    for column in target_columns:
        if column not in observed_schema:
            continue
        expected_type = normalize_type(target_schema[column])
        observed_type = normalize_type(observed_schema[column])
        if not types_compatible(expected_type, observed_type):
            type_drift_columns.append(
                {
                    "column": column,
                    "expected_type": expected_type,
                    "observed_type": observed_type,
                }
            )

    return {
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "type_drift_columns": type_drift_columns,
    }


def infer_csv_schema(rows: Sequence[Mapping[str, str]], headers: Sequence[str]) -> dict[str, str]:
    """Infer simple logical types from sampled CSV values."""

    return {
        header: infer_column_type([row.get(header, "") for row in rows])
        for header in headers
    }


def infer_column_type(values: Sequence[str]) -> str:
    """Infer a stable logical type for one CSV column."""

    observed_types: set[str] = set()
    for value in values:
        cell_type = infer_cell_type(value)
        if cell_type != "null":
            observed_types.add(cell_type)

    if not observed_types:
        return "null"
    if observed_types == {"integer"}:
        return "integer"
    if observed_types <= {"integer", "float"}:
        return "float"
    if observed_types == {"boolean"}:
        return "boolean"
    if observed_types == {"date"}:
        return "date"
    if observed_types <= {"date", "datetime"}:
        return "datetime"
    if observed_types == {"string"}:
        return "string"
    return "mixed"


def infer_cell_type(value: str) -> str:
    """Infer the logical type of one CSV cell."""

    normalized = "" if value is None else str(value).strip()
    if normalized.lower() in NULL_MARKERS:
        return "null"
    lowered = normalized.lower()
    if lowered in {"true", "false"}:
        return "boolean"
    if INTEGER_PATTERN.match(normalized):
        return "integer"
    try:
        decimal_value = Decimal(normalized)
        if decimal_value.is_finite():
            return "float"
    except InvalidOperation:
        return _temporal_or_string(normalized)
    return _temporal_or_string(normalized)


def _temporal_or_string(value: str) -> str:
    if _is_datetime(value):
        return "datetime"
    if _is_date(value):
        return "date"
    return "string"


def normalize_type(value: str) -> str:
    """Normalize a user-provided or inferred type name."""

    normalized = value.strip().lower().replace("_", " ")
    if normalized == "":
        raise ValueError("schema type cannot be empty")
    return TYPE_ALIASES.get(normalized, normalized)


def types_compatible(expected_type: str, observed_type: str) -> bool:
    """Return whether an observed type satisfies the target contract."""

    expected = normalize_type(expected_type)
    observed = normalize_type(observed_type)
    if expected == observed:
        return True
    if expected == "float" and observed == "integer":
        return True
    if expected == "datetime" and observed == "date":
        return True
    return False


def _observed_schema(
    incident: Any,
    context: Mapping[str, Any],
    settings: Settings | None,
    s3_client: BaseClient | None,
) -> dict[str, str]:
    raw_observed_schema = context.get("observed_schema")
    if raw_observed_schema is not None:
        return normalize_schema_contract(raw_observed_schema)

    max_rows = _positive_int(context.get("max_rows", 10_000), "max_rows")
    max_bytes = _positive_int(context.get("max_bytes", 50 * 1024 * 1024), "max_bytes")
    csv_content = _optional_text(context.get("csv_content"), "csv_content")
    source_uri = source_uri_from_context_or_incident(incident, context)
    sample = read_csv_sample(
        source_uri=source_uri,
        csv_content=csv_content,
        settings=settings,
        s3_client=s3_client,
        max_rows=max_rows,
        max_bytes=max_bytes,
    )
    return infer_csv_schema(sample.rows, sample.headers)


def _extract_column_type(raw_spec: Any, column_name: str) -> str:
    if isinstance(raw_spec, str):
        return normalize_type(raw_spec)
    if isinstance(raw_spec, Mapping):
        if "type" not in raw_spec:
            raise ValueError(f"target_schema.{column_name} is missing type")
        return normalize_type(str(raw_spec["type"]))
    raise TypeError(f"target_schema.{column_name} must be a type string or object")


def _normalize_column_name(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("schema column names must be strings")
    normalized = value.strip()
    if normalized == "":
        raise ValueError("schema column names cannot be empty")
    return normalized


def _incident_evidence_value(incident: Any, key: str) -> Any:
    evidence = getattr(incident, "evidence_json", None)
    if isinstance(evidence, Mapping):
        return evidence.get(key)
    return None


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


def _is_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _is_datetime(value: str) -> bool:
    if "T" not in value and " " not in value:
        return False
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        datetime.fromisoformat(candidate)
        return True
    except ValueError:
        return False
