"""Non-destructive S3 quarantine tool."""

from __future__ import annotations

import hashlib
import posixpath
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlencode

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from config.settings import Settings, get_s3_client, get_settings
from dataops_agent.tools.base import ToolResult, failure_result, require_context
from dataops_agent.tools.csv_source import S3Location, parse_s3_uri, source_uri_from_context_or_incident


def execute(
    incident: Any,
    context: Mapping[str, Any] | None = None,
    *,
    settings: Settings | None = None,
    s3_client: BaseClient | None = None,
    repository: Any | None = None,
) -> ToolResult:
    """Copy a Bronze S3 object to quarantine without deleting or overwriting Bronze."""

    del repository
    tool_context = require_context(context)
    try:
        resolved_settings = settings or get_settings()
        source_uri = source_uri_from_context_or_incident(incident, tool_context)
        if source_uri is None:
            raise ValueError("source_uri is required for quarantine")
        if source_uri.lower().startswith("file://"):
            raise ValueError("quarantine_batch requires an S3 source, not a local file")

        source = parse_s3_uri(source_uri, settings=resolved_settings)
        _validate_bronze_source(source, resolved_settings)
        destination = _quarantine_destination(source, incident, resolved_settings)
        _validate_quarantine_destination(destination, resolved_settings)

        client = s3_client or get_s3_client(resolved_settings)
        if _object_exists(client, destination):
            return ToolResult(
                success=True,
                data={
                    "tool": "quarantine_batch",
                    "source_uri": source.uri,
                    "quarantine_uri": destination.uri,
                    "copied": False,
                    "idempotent": True,
                    "message": "quarantine object already exists",
                },
            )

        metadata = _copy_metadata(incident, source)
        tags = _copy_tags(incident)
        response = client.copy_object(
            Bucket=destination.bucket,
            Key=destination.key,
            CopySource={"Bucket": source.bucket, "Key": source.key},
            MetadataDirective="REPLACE",
            Metadata=metadata,
            TaggingDirective="REPLACE",
            Tagging=urlencode(tags),
        )
        return ToolResult(
            success=True,
            data={
                "tool": "quarantine_batch",
                "source_uri": source.uri,
                "quarantine_uri": destination.uri,
                "copied": True,
                "copy_object_etag": response.get("CopyObjectResult", {}).get("ETag"),
                "version_id": response.get("VersionId"),
                "metadata": metadata,
                "tags": tags,
            },
        )
    except Exception as exc:
        return failure_result(str(exc), {"tool": "quarantine_batch"})


def _validate_bronze_source(source: S3Location, settings: Settings) -> None:
    if not source.key.startswith(settings.S3_BRONZE_PREFIX):
        raise ValueError(
            "quarantine_batch only copies objects from the configured Bronze prefix"
        )


def _validate_quarantine_destination(destination: S3Location, settings: Settings) -> None:
    if not destination.key.startswith(settings.S3_QUARANTINE_PREFIX):
        raise ValueError("quarantine destination must be under S3_QUARANTINE_PREFIX")
    if destination.key.startswith(settings.S3_BRONZE_PREFIX):
        raise ValueError("quarantine destination must not target the Bronze prefix")


def _quarantine_destination(
    source: S3Location,
    incident: Any,
    settings: Settings,
) -> S3Location:
    incident_id = _safe_key_part(str(getattr(incident, "incident_id", "incident")))
    pipeline_name = _safe_key_part(str(getattr(incident, "pipeline_name", "pipeline")))
    relative_source = source.key.removeprefix(settings.S3_BRONZE_PREFIX).lstrip("/")
    if relative_source == "":
        relative_source = posixpath.basename(source.key) or "source.csv"
    destination_key = posixpath.normpath(
        posixpath.join(
            settings.S3_QUARANTINE_PREFIX,
            pipeline_name,
            incident_id,
            relative_source,
        )
    ).replace("\\", "/")
    if destination_key.startswith("../") or destination_key == "..":
        raise ValueError("computed quarantine key escaped the quarantine prefix")
    if not destination_key.startswith(settings.S3_QUARANTINE_PREFIX):
        destination_key = settings.S3_QUARANTINE_PREFIX + destination_key.lstrip("/")
    return S3Location(bucket=source.bucket, key=destination_key)


def _object_exists(s3_client: BaseClient, location: S3Location) -> bool:
    try:
        s3_client.head_object(Bucket=location.bucket, Key=location.key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _copy_metadata(incident: Any, source: S3Location) -> dict[str, str]:
    return {
        "incident-id": _metadata_value(getattr(incident, "incident_id", "")),
        "pipeline-name": _metadata_value(getattr(incident, "pipeline_name", "")),
        "incident-type": _metadata_value(getattr(incident, "incident_type", "")),
        "source-key-sha256": hashlib.sha256(source.key.encode("utf-8")).hexdigest(),
    }


def _copy_tags(incident: Any) -> dict[str, str]:
    return {
        "dataops-status": "quarantined",
        "incident-id": _tag_value(getattr(incident, "incident_id", "")),
        "pipeline": _tag_value(getattr(incident, "pipeline_name", "")),
        "incident-type": _tag_value(getattr(incident, "incident_type", "")),
    }


def _safe_key_part(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._=-]+", "-", value.strip())
    normalized = normalized.strip(".-")
    return normalized[:160] if normalized else "unknown"


def _metadata_value(value: Any) -> str:
    normalized = str(value).strip()
    return normalized[:512] if normalized else "unknown"


def _tag_value(value: Any) -> str:
    normalized = re.sub(r"[^A-Za-z0-9 +\\-=._:/@]+", "-", str(value).strip())
    normalized = normalized[:256]
    return normalized if normalized else "unknown"

