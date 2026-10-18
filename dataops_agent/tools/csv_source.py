"""CSV source loading helpers for deterministic DataOps tools."""

from __future__ import annotations

import csv
from collections.abc import Mapping
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, Field

from config.settings import Settings, get_s3_client, get_settings
from dataops_agent.tools.base import ToolExecutionError, require_non_empty


MAX_CSV_READ_BYTES = 50 * 1024 * 1024


class S3Location(BaseModel):
    """Parsed S3 object location."""

    model_config = ConfigDict(frozen=True)

    bucket: str = Field(min_length=1)
    key: str = Field(min_length=1)

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


class CsvSample(BaseModel):
    """Parsed CSV rows and source metadata."""

    model_config = ConfigDict(frozen=True)

    source_uri: str | None
    headers: list[str]
    rows: list[dict[str, str]]
    total_rows: int
    sampled_rows: int
    truncated: bool


def source_uri_from_context_or_incident(
    incident: Any,
    context: Mapping[str, Any],
) -> str | None:
    """Resolve source_uri from tool context first, then incident state."""

    context_source = context.get("source_uri")
    if context_source is not None:
        return require_non_empty(str(context_source), "context.source_uri")
    incident_source = getattr(incident, "source_uri", None)
    if incident_source is None:
        return None
    return require_non_empty(str(incident_source), "incident.source_uri")


def parse_s3_uri(
    source_uri: str,
    *,
    settings: Settings | None = None,
) -> S3Location:
    """Parse an s3:// URI or configured-bucket object key."""

    normalized = require_non_empty(source_uri, "source_uri").replace("\\", "/")
    if normalized.lower().startswith("s3://"):
        parsed = urlparse(normalized)
        bucket = require_non_empty(parsed.netloc, "s3 bucket")
        key = require_non_empty(parsed.path.lstrip("/"), "s3 key")
        return S3Location(bucket=bucket, key=key)

    resolved = settings or get_settings()
    key = require_non_empty(normalized.lstrip("/"), "s3 key")
    return S3Location(bucket=resolved.S3_BUCKET_NAME, key=key)


def read_csv_sample(
    *,
    source_uri: str | None,
    csv_content: str | None,
    settings: Settings | None = None,
    s3_client: BaseClient | None = None,
    max_rows: int = 10_000,
    max_bytes: int = MAX_CSV_READ_BYTES,
) -> CsvSample:
    """Load CSV content from context, local disk, or S3 and parse a bounded sample."""

    if max_rows < 1:
        raise ValueError("max_rows must be greater than 0")
    if max_bytes < 1:
        raise ValueError("max_bytes must be greater than 0")

    if csv_content is not None:
        text = csv_content
        effective_source = source_uri
    else:
        if source_uri is None:
            raise ToolExecutionError("source_uri or csv_content is required")
        text = _read_text_from_uri(
            source_uri,
            settings=settings,
            s3_client=s3_client,
            max_bytes=max_bytes,
        )
        effective_source = source_uri

    return _parse_csv_text(text, source_uri=effective_source, max_rows=max_rows)


def _read_text_from_uri(
    source_uri: str,
    *,
    settings: Settings | None,
    s3_client: BaseClient | None,
    max_bytes: int,
) -> str:
    normalized_uri = require_non_empty(source_uri, "source_uri")
    local_path = _local_path_if_present(normalized_uri)
    if local_path is not None:
        return _read_local_text(local_path, max_bytes=max_bytes)

    resolved_settings = settings or get_settings()
    location = parse_s3_uri(normalized_uri, settings=resolved_settings)
    client = s3_client or get_s3_client(resolved_settings)
    return _read_s3_text(client, location, max_bytes=max_bytes)


def _local_path_if_present(source_uri: str) -> Path | None:
    if source_uri.lower().startswith("file://"):
        return Path(source_uri[7:])
    path = Path(source_uri)
    if path.exists():
        return path
    return None


def _read_local_text(path: Path, *, max_bytes: int) -> str:
    if not path.is_file():
        raise ToolExecutionError(f"local CSV source is not a file: {path}")
    size = path.stat().st_size
    if size > max_bytes:
        raise ToolExecutionError(
            f"local CSV source exceeds max_bytes ({size} > {max_bytes})"
        )
    try:
        return path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ToolExecutionError("local CSV source must be UTF-8 compatible") from exc
    except OSError as exc:
        raise ToolExecutionError(f"failed to read local CSV source: {exc}") from exc


def _read_s3_text(
    s3_client: BaseClient,
    location: S3Location,
    *,
    max_bytes: int,
) -> str:
    try:
        head = s3_client.head_object(Bucket=location.bucket, Key=location.key)
        content_length = int(head.get("ContentLength", 0))
        if content_length > max_bytes:
            raise ToolExecutionError(
                "S3 CSV source exceeds max_bytes "
                f"({content_length} > {max_bytes}) at {location.uri}"
            )

        response = s3_client.get_object(Bucket=location.bucket, Key=location.key)
        body = response["Body"]
        try:
            data = body.read(max_bytes + 1)
        finally:
            body.close()
    except ClientError as exc:
        raise ToolExecutionError(f"failed to read S3 CSV source {location.uri}") from exc

    if len(data) > max_bytes:
        raise ToolExecutionError(
            f"S3 CSV source exceeds max_bytes while reading {location.uri}"
        )
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ToolExecutionError("S3 CSV source must be UTF-8 compatible") from exc


def _parse_csv_text(
    csv_text: str,
    *,
    source_uri: str | None,
    max_rows: int,
) -> CsvSample:
    if csv_text.strip() == "":
        raise ToolExecutionError("CSV source is empty")

    reader = csv.reader(StringIO(csv_text))
    try:
        raw_headers = next(reader)
    except StopIteration as exc:
        raise ToolExecutionError("CSV source does not contain a header row") from exc

    if raw_headers is None:
        raise ToolExecutionError("CSV source does not contain a header row")

    headers = [_normalize_header(header) for header in raw_headers]
    duplicates = sorted({header for header in headers if headers.count(header) > 1})
    if duplicates:
        raise ToolExecutionError(
            "CSV header contains duplicate columns: " + ", ".join(duplicates)
        )
    if any(header == "" for header in headers):
        raise ToolExecutionError("CSV header contains an empty column name")

    rows: list[dict[str, str]] = []
    total_rows = 0
    for raw_row in reader:
        total_rows += 1
        if len(rows) >= max_rows:
            continue
        rows.append(_normalize_row(raw_row, headers=headers))

    return CsvSample(
        source_uri=source_uri,
        headers=headers,
        rows=rows,
        total_rows=total_rows,
        sampled_rows=len(rows),
        truncated=total_rows > len(rows),
    )


def _normalize_header(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def _normalize_row(raw_row: list[Any], *, headers: list[str]) -> dict[str, str]:
    if len(raw_row) > len(headers):
        raise ToolExecutionError("CSV row contains more fields than the header row")
    row: dict[str, str] = {}
    for index, header in enumerate(headers):
        value = raw_row[index] if index < len(raw_row) else ""
        row[header] = "" if value is None else str(value).strip()
    return row
