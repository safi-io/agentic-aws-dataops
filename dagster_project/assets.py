"""Dagster assets for Bronze-to-Silver order processing."""

import hashlib
import posixpath
from collections.abc import Mapping
from datetime import UTC, datetime
from io import BytesIO
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from dagster import (
    AssetExecutionContext,
    Failure,
    Field as DagsterField,
    MetadataValue,
    Permissive,
    asset,
)
from pandas.api import types as pandas_types
from pydantic import BaseModel, ConfigDict, Field, model_validator

from config.settings import Settings, get_s3_client, get_settings
from dataops_agent.incident_state import IncidentRepository
from dataops_agent.tools.csv_source import S3Location
from dataops_agent.tools.inspect_schema import (
    DEFAULT_ORDER_SCHEMA,
    compare_schemas,
    infer_column_type,
    normalize_schema_contract,
)


PIPELINE_NAME = "orders_data_quality"
INCIDENT_TYPE_SCHEMA_DRIFT = "schema_drift"
INCIDENT_TYPE_NULL_THRESHOLD = "null_threshold_breach"
INCIDENT_TYPE_DATA_QUALITY = "data_quality_failure"
NULL_MARKERS = {"", "null", "none", "nan", "na", "n/a"}


class OrdersAssetConfig(BaseModel):
    """Runtime configuration shared by the Bronze and Silver assets."""

    model_config = ConfigDict(extra="forbid")

    source_uri: str | None = Field(
        default=None,
        description="S3 URI or object key for the Bronze orders CSV.",
    )
    source_key: str | None = Field(
        default=None,
        description="Object key under the configured S3 bucket. Used when source_uri is not set.",
    )
    target_schema: dict[str, str] = Field(default_factory=lambda: dict(DEFAULT_ORDER_SCHEMA))
    max_null_percentage: float | None = Field(default=None, ge=0.0, le=1.0)
    silver_key_prefix: str | None = Field(
        default=None,
        description="Optional key prefix for clean Silver Parquet output.",
    )

    @model_validator(mode="after")
    def _normalize_empty_strings(self) -> "OrdersAssetConfig":
        for field_name in ("source_uri", "source_key", "silver_key_prefix"):
            value = getattr(self, field_name)
            if isinstance(value, str) and value.strip() == "":
                object.__setattr__(self, field_name, None)
        return self


ORDERS_ASSET_CONFIG_SCHEMA = {
    "source_uri": DagsterField(str, is_required=False),
    "source_key": DagsterField(str, is_required=False),
    "target_schema": DagsterField(Permissive(), is_required=False),
    "max_null_percentage": DagsterField(float, is_required=False),
    "silver_key_prefix": DagsterField(str, is_required=False),
}


@asset(config_schema=ORDERS_ASSET_CONFIG_SCHEMA)
def bronze_orders(
    context: AssetExecutionContext,
) -> pd.DataFrame:
    """Read the immutable Bronze orders CSV from S3."""

    config = _orders_config_from_context(context)
    settings = get_settings()
    source = _resolve_source_location(settings, config)
    s3_client = get_s3_client(settings)
    payload = _read_s3_object_bytes(s3_client, source)

    try:
        frame = pd.read_csv(BytesIO(payload))
    except Exception as exc:
        validation = _validation_payload(
            target_schema=normalize_schema_contract(config.target_schema),
            observed_schema={},
            schema_comparison={
                "missing_columns": [], "extra_columns": [], "type_drift_columns": [],
            },
            null_profile={},
            threshold=(settings.MAX_NULL_PERCENTAGE if config.max_null_percentage is None
                       else config.max_null_percentage),
            errors=[f"CSV parsing failed: {exc}"],
        )
        metadata = {
            "source_uri": MetadataValue.text(source.uri),
            "validation": MetadataValue.json(validation),
        }
        try:
            incident = _record_validation_incident(
                context=context, settings=settings, source_uri=source.uri,
                validation=validation, asset_key="bronze_orders",
            )
        except Exception as persistence_error:
            raise Failure(
                description=(
                    f"Failed to parse Bronze CSV at {source.uri}: {exc}; "
                    f"incident persistence also failed: {persistence_error}"
                ),
                metadata=metadata,
            ) from persistence_error
        metadata["incident_id"] = MetadataValue.text(incident.incident_id)
        raise Failure(
            description=(
                f"Failed to parse Bronze CSV at {source.uri}: {exc}; "
                f"pending incident recorded as {incident.incident_id}"
            ),
            metadata=metadata,
        ) from exc

    if frame.columns.empty:
        raise Failure(
            description=f"Bronze CSV at {source.uri} does not contain columns",
            metadata={"source_uri": MetadataValue.text(source.uri)},
        )

    frame.attrs["source_uri"] = source.uri
    frame.attrs["source_bucket"] = source.bucket
    frame.attrs["source_key"] = source.key
    frame.attrs["source_etag"] = _etag_from_head(s3_client, source)
    context.add_output_metadata(
        {
            "source_uri": MetadataValue.text(source.uri),
            "row_count": MetadataValue.int(int(len(frame))),
            "column_count": MetadataValue.int(int(len(frame.columns))),
            "columns": MetadataValue.json([str(column) for column in frame.columns]),
        }
    )
    return frame


@asset(config_schema=ORDERS_ASSET_CONFIG_SCHEMA)
def silver_orders(
    context: AssetExecutionContext,
    bronze_orders: pd.DataFrame,
) -> str:
    """Validate orders data and write clean Parquet output to the Silver prefix."""

    config = _orders_config_from_context(context)
    settings = get_settings()
    source_uri = _source_uri_from_frame_or_config(bronze_orders, settings, config)
    target_schema = normalize_schema_contract(config.target_schema)
    threshold = (
        settings.MAX_NULL_PERCENTAGE
        if config.max_null_percentage is None
        else float(config.max_null_percentage)
    )

    validation = validate_orders_frame(
        bronze_orders,
        target_schema=target_schema,
        max_null_percentage=threshold,
    )
    if not validation["valid"]:
        try:
            incident = _record_validation_incident(
                context=context,
                settings=settings,
                source_uri=source_uri,
                validation=validation,
            )
        except Exception as exc:
            raise Failure(
                description=(
                    "Orders Silver validation failed and incident persistence failed: "
                    f"{exc}"
                ),
                metadata={
                    "source_uri": MetadataValue.text(source_uri),
                    "validation": MetadataValue.json(validation),
                },
            ) from exc
        raise Failure(
            description=(
                "Orders Silver validation failed; pending incident recorded as "
                f"{incident.incident_id}"
            ),
            metadata={
                "incident_id": MetadataValue.text(incident.incident_id),
                "source_uri": MetadataValue.text(source_uri),
                "validation": MetadataValue.json(validation),
            },
        )

    output = _silver_destination(
        settings=settings,
        source_uri=source_uri,
        run_id=context.run_id,
        silver_key_prefix=config.silver_key_prefix,
    )
    parquet_payload = _frame_to_parquet_bytes(bronze_orders)
    s3_client = get_s3_client(settings)
    try:
        s3_client.put_object(
            Bucket=output.bucket,
            Key=output.key,
            Body=parquet_payload,
            ContentType="application/vnd.apache.parquet",
            Metadata={
                "source-uri-sha256": hashlib.sha256(
                    source_uri.encode("utf-8")
                ).hexdigest(),
                "dagster-run-id": _metadata_value(context.run_id),
                "pipeline-name": PIPELINE_NAME,
            },
        )
    except Exception as exc:
        raise Failure(
            description=f"Failed to write Silver Parquet object {output.uri}: {exc}",
            metadata={"silver_uri": MetadataValue.text(output.uri)},
        ) from exc

    context.add_output_metadata(
        {
            "silver_uri": MetadataValue.text(output.uri),
            "row_count": MetadataValue.int(int(len(bronze_orders))),
            "null_threshold": MetadataValue.float(threshold),
            "schema_valid": MetadataValue.bool(True),
        }
    )
    return output.uri


def validate_orders_frame(
    frame: pd.DataFrame,
    *,
    target_schema: Mapping[str, str],
    max_null_percentage: float,
) -> dict[str, Any]:
    """Validate schema and null ratios for an orders DataFrame."""

    if max_null_percentage < 0.0 or max_null_percentage > 1.0:
        raise ValueError("max_null_percentage must be between 0.0 and 1.0")
    duplicate_columns = _duplicate_columns(frame)
    if duplicate_columns:
        return _validation_payload(
            target_schema=target_schema,
            observed_schema={str(column): "unknown" for column in frame.columns},
            schema_comparison={
                "missing_columns": [],
                "extra_columns": [],
                "type_drift_columns": [],
            },
            null_profile={},
            threshold=max_null_percentage,
            errors=[
                "duplicate columns detected: " + ", ".join(duplicate_columns)
            ],
        )
    if frame.empty:
        return _validation_payload(
            target_schema=target_schema,
            observed_schema={},
            schema_comparison={
                "missing_columns": list(target_schema.keys()),
                "extra_columns": [],
                "type_drift_columns": [],
            },
            null_profile={},
            threshold=max_null_percentage,
            errors=["orders frame is empty"],
        )

    observed_schema = _infer_dataframe_schema(frame)
    schema_comparison = compare_schemas(target_schema, observed_schema)
    null_profile = _profile_dataframe_nulls(frame)
    threshold_breaches = [
        {
            "column": column,
            "null_count": metrics["null_count"],
            "null_ratio": metrics["null_ratio"],
            "threshold": max_null_percentage,
        }
        for column, metrics in null_profile.items()
        if metrics["null_ratio"] > max_null_percentage
    ]

    errors: list[str] = []
    if schema_comparison["missing_columns"]:
        errors.append("missing required columns")
    if schema_comparison["extra_columns"]:
        errors.append("unexpected extra columns")
    if schema_comparison["type_drift_columns"]:
        errors.append("column type drift detected")
    if threshold_breaches:
        errors.append("null threshold breaches detected")

    payload = _validation_payload(
        target_schema=target_schema,
        observed_schema=observed_schema,
        schema_comparison=schema_comparison,
        null_profile=null_profile,
        threshold=max_null_percentage,
        errors=errors,
    )
    payload["threshold_breaches"] = threshold_breaches
    return payload


def _duplicate_columns(frame: pd.DataFrame) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for column in frame.columns:
        column_name = str(column)
        if column_name in seen:
            duplicates.add(column_name)
        seen.add(column_name)
    return sorted(duplicates)


def _validation_payload(
    *,
    target_schema: Mapping[str, str],
    observed_schema: Mapping[str, str],
    schema_comparison: Mapping[str, Any],
    null_profile: Mapping[str, Any],
    threshold: float,
    errors: list[str],
) -> dict[str, Any]:
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "target_schema": dict(target_schema),
        "observed_schema": dict(observed_schema),
        "schema_comparison": dict(schema_comparison),
        "null_profile": dict(null_profile),
        "max_null_percentage": threshold,
    }


def _orders_config_from_context(context: AssetExecutionContext) -> OrdersAssetConfig:
    raw_config = dict(context.op_config or {})
    if "target_schema" not in raw_config:
        raw_config["target_schema"] = dict(DEFAULT_ORDER_SCHEMA)
    return OrdersAssetConfig.model_validate(raw_config)


def _resolve_source_location(settings: Settings, config: OrdersAssetConfig) -> S3Location:
    source_uri = config.source_uri
    if source_uri is None and config.source_key is not None:
        source_uri = config.source_key
    if source_uri is None:
        source_uri = posixpath.join(settings.S3_BRONZE_PREFIX, "orders.csv")
    normalized = source_uri.strip().replace("\\", "/")
    if normalized == "":
        raise Failure(description="Bronze source URI cannot be empty")
    if normalized.lower().startswith("s3://"):
        parsed = urlparse(normalized)
        bucket = parsed.netloc.strip()
        key = parsed.path.lstrip("/")
    else:
        bucket = settings.S3_BUCKET_NAME
        key = normalized.lstrip("/")
    if bucket == "" or key == "":
        raise Failure(description=f"Invalid Bronze S3 source URI: {source_uri}")
    if not key.startswith(settings.S3_BRONZE_PREFIX):
        raise Failure(
            description=(
                "Bronze source must be under the configured immutable Bronze prefix"
            ),
            metadata={
                "source_key": MetadataValue.text(key),
                "bronze_prefix": MetadataValue.text(settings.S3_BRONZE_PREFIX),
            },
        )
    return S3Location(bucket=bucket, key=key)


def _read_s3_object_bytes(s3_client: BaseClient, source: S3Location) -> bytes:
    try:
        response = s3_client.get_object(Bucket=source.bucket, Key=source.key)
        body = response["Body"]
        try:
            return body.read()
        finally:
            body.close()
    except ClientError as exc:
        raise Failure(
            description=f"Failed to read Bronze S3 object {source.uri}: {exc}",
            metadata={"source_uri": MetadataValue.text(source.uri)},
        ) from exc


def _etag_from_head(s3_client: BaseClient, source: S3Location) -> str | None:
    try:
        response = s3_client.head_object(Bucket=source.bucket, Key=source.key)
    except ClientError:
        return None
    etag = response.get("ETag")
    return str(etag).strip('"') if etag is not None else None


def _source_uri_from_frame_or_config(
    frame: pd.DataFrame,
    settings: Settings,
    config: OrdersAssetConfig,
) -> str:
    source_uri = frame.attrs.get("source_uri")
    if isinstance(source_uri, str) and source_uri.strip() != "":
        return source_uri.strip()
    return _resolve_source_location(settings, config).uri


def _infer_dataframe_schema(frame: pd.DataFrame) -> dict[str, str]:
    schema: dict[str, str] = {}
    for column in frame.columns:
        column_name = str(column).strip()
        if column_name == "":
            raise ValueError("DataFrame contains an empty column name")
        series = frame[column]
        if pandas_types.is_bool_dtype(series):
            schema[column_name] = "boolean"
        elif pandas_types.is_integer_dtype(series):
            schema[column_name] = "integer"
        elif pandas_types.is_float_dtype(series):
            schema[column_name] = "float"
        elif pandas_types.is_datetime64_any_dtype(series):
            schema[column_name] = "datetime"
        else:
            values = [
                str(value)
                for value in series.dropna().tolist()
            ]
            schema[column_name] = infer_column_type(values)
    return schema


def _profile_dataframe_nulls(frame: pd.DataFrame) -> dict[str, dict[str, int | float]]:
    total_rows = int(len(frame))
    profile: dict[str, dict[str, int | float]] = {}
    for column in frame.columns:
        column_name = str(column)
        series = frame[column]
        pandas_nulls = series.isna()
        if pandas_types.is_object_dtype(series) or pandas_types.is_string_dtype(series):
            normalized_strings = series.fillna("").astype(str).str.strip().str.lower()
            semantic_nulls = normalized_strings.isin(NULL_MARKERS)
            null_count = int((pandas_nulls | semantic_nulls).sum())
        else:
            null_count = int(pandas_nulls.sum())
        null_ratio = 0.0 if total_rows == 0 else null_count / total_rows
        profile[column_name] = {
            "null_count": null_count,
            "non_null_count": total_rows - null_count,
            "null_ratio": round(null_ratio, 6),
        }
    return profile


def _record_validation_incident(
    *,
    context: AssetExecutionContext,
    settings: Settings,
    source_uri: str,
    validation: Mapping[str, Any],
    asset_key: str = "silver_orders",
):
    incident_type = _incident_type_from_validation(validation)
    incident_id = _incident_id(
        pipeline_name=PIPELINE_NAME,
        source_uri=source_uri,
        run_id=context.run_id,
        incident_type=incident_type,
    )
    evidence = {
        "asset_key": asset_key,
        "dagster_run_id": context.run_id,
        "validation": validation,
        "target_schema": validation.get("target_schema", {}),
        "observed_schema": validation.get("observed_schema", {}),
        "source_uri": source_uri,
        "recorded_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
    }
    repository = IncidentRepository(settings.DATABASE_URL)
    return repository.create_incident(
        incident_id=incident_id,
        pipeline_name=PIPELINE_NAME,
        dagster_run_id=context.run_id,
        source_uri=source_uri,
        incident_type=incident_type,
        severity="high",
        error_summary="; ".join(validation.get("errors", [])) or "data quality failure",
        evidence_json=evidence,
    )


def _incident_type_from_validation(validation: Mapping[str, Any]) -> str:
    errors = set(str(error) for error in validation.get("errors", []))
    if errors <= {"null threshold breaches detected"}:
        return INCIDENT_TYPE_NULL_THRESHOLD
    if errors and errors <= {
        "missing required columns",
        "unexpected extra columns",
        "column type drift detected",
    }:
        return INCIDENT_TYPE_SCHEMA_DRIFT
    return INCIDENT_TYPE_DATA_QUALITY


def _incident_id(
    *,
    pipeline_name: str,
    source_uri: str,
    run_id: str,
    incident_type: str,
) -> str:
    digest = hashlib.sha256(
        f"{pipeline_name}|{source_uri}|{run_id}|{incident_type}".encode("utf-8")
    ).hexdigest()[:32]
    return f"{pipeline_name}_{digest}"


def _silver_destination(
    *,
    settings: Settings,
    source_uri: str,
    run_id: str,
    silver_key_prefix: str | None,
) -> S3Location:
    prefix = (silver_key_prefix or settings.S3_SILVER_PREFIX).strip().replace("\\", "/")
    if prefix == "":
        raise Failure(description="Silver key prefix cannot be empty")
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"
    if prefix.startswith(settings.S3_BRONZE_PREFIX):
        raise Failure(description="Silver output must not target the Bronze prefix")

    source_name = _safe_source_name(source_uri)
    run_part = _safe_key_part(run_id)
    key = posixpath.join(prefix, "orders", f"{source_name}-{run_part}.parquet")
    return S3Location(bucket=settings.S3_BUCKET_NAME, key=key)


def _frame_to_parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    try:
        frame.to_parquet(buffer, index=False)
    except Exception as exc:
        raise Failure(description=f"Failed to serialize Silver Parquet: {exc}") from exc
    return buffer.getvalue()


def _safe_source_name(source_uri: str) -> str:
    parsed = urlparse(source_uri)
    raw_name = posixpath.basename(parsed.path if parsed.scheme else source_uri)
    if raw_name == "":
        raw_name = "orders"
    if "." in raw_name:
        raw_name = raw_name.rsplit(".", 1)[0]
    return _safe_key_part(raw_name)


def _safe_key_part(value: str) -> str:
    sanitized = "".join(
        character if character.isalnum() or character in {"-", "_", "."} else "-"
        for character in value.strip()
    ).strip(".-")
    return sanitized[:120] if sanitized else "unknown"


def _metadata_value(value: str) -> str:
    normalized = str(value).strip()
    return normalized[:512] if normalized else "unknown"
