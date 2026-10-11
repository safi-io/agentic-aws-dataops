"""Reusable Dagster resources for Amazon S3 and Amazon RDS (PostgreSQL)."""

import io
import json
import os
import re
from typing import Any, Dict, List, Optional
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from dagster import ConfigurableResource
import pandas as pd
from pydantic import Field
import sqlalchemy
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

from dataops_agent.incident_state import IncidentRecord


class S3Resource(ConfigurableResource):
    """Reusable Dagster resource for interacting with Amazon S3.

    Provides direct client operations for listing, reading, uploading,
    and downloading objects from S3 without manual client initialization elsewhere.
    """

    bucket_name: Optional[str] = Field(
        default=None,
        description="Amazon S3 bucket name for data operations"
    )
    prefix: str = Field(
        default="bronze/",
        description="Default S3 key prefix/zone to query (e.g. bronze/ for raw files)"
    )
    region_name: str = Field(
        default="us-east-1",
        description="AWS Region name"
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom endpoint URL for LocalStack, MinIO, or testing"
    )
    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="Explicit AWS Access Key ID (optional)"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="Explicit AWS Secret Access Key (optional)"
    )
    aws_session_token: Optional[str] = Field(
        default=None,
        description="Explicit AWS Session Token (optional)"
    )
    max_retries: int = Field(
        default=3,
        description="Number of retry attempts for S3 requests"
    )

    def get_client(self):
        """Builds and returns a boto3 S3 client with configured parameters."""
        boto_config = BotoConfig(
            retries={"max_attempts": self.max_retries, "mode": "standard"},
            region_name=self.region_name,
        )

        client_kwargs: Dict[str, Any] = {
            "service_name": "s3",
            "region_name": self.region_name,
            "config": boto_config,
        }

        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url

        if self.aws_access_key_id and self.aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = self.aws_access_key_id
            client_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
            if self.aws_session_token:
                client_kwargs["aws_session_token"] = self.aws_session_token

        return boto3.client(**client_kwargs)

    def list_objects(
        self,
        prefix: Optional[str] = None,
        extension: str = ".csv",
        bucket: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Lists objects under the specified prefix matching the given extension.

        Returns a list of dicts with: key, size, last_modified (isoformat), and etag.
        """
        target_bucket = bucket or self.bucket_name
        if not target_bucket:
            raise ValueError("Target S3 bucket is not configured.")

        target_prefix = prefix if prefix is not None else self.prefix
        client = self.get_client()
        matched_objects: List[Dict[str, Any]] = []

        try:
            paginator = client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=target_bucket,
                Prefix=target_prefix
            )

            for page in page_iterator:
                for item in page.get("Contents", []):
                    key = item["Key"]
                    size = item["Size"]
                    # Filter matching extension (case-insensitive) and ignore zero-byte folder markers
                    if key.lower().endswith(extension.lower()) and size > 0:
                        matched_objects.append({
                            "key": key,
                            "size": size,
                            "last_modified": item["LastModified"].isoformat(),
                            "etag": item.get("ETag", "").strip('"'),
                        })

            return matched_objects

        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(
                f"Failed to list S3 objects in s3://{target_bucket}/{target_prefix}: {str(e)}"
            ) from e

    def get_object_bytes(
        self,
        key: str,
        bucket: Optional[str] = None
    ) -> bytes:
        """Retrieves raw object bytes from S3."""
        target_bucket = bucket or self.bucket_name
        if not target_bucket:
            raise ValueError("Target S3 bucket is not configured.")

        client = self.get_client()
        try:
            response = client.get_object(Bucket=target_bucket, Key=key)
            return response["Body"].read()
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(
                f"Failed to read S3 object s3://{target_bucket}/{key}: {str(e)}"
            ) from e

    def download_file(
        self,
        key: str,
        local_path: str,
        bucket: Optional[str] = None
    ) -> str:
        """Downloads an S3 object directly to a local file path."""
        target_bucket = bucket or self.bucket_name
        if not target_bucket:
            raise ValueError("Target S3 bucket is not configured.")

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        client = self.get_client()
        try:
            client.download_file(Bucket=target_bucket, Key=key, Filename=local_path)
            return local_path
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(
                f"Failed to download S3 object s3://{target_bucket}/{key} to {local_path}: {str(e)}"
            ) from e

    def read_csv(
        self,
        key: str,
        bucket: Optional[str] = None,
        **pandas_kwargs: Any
    ) -> pd.DataFrame:
        """Reads a CSV file directly from S3 into a pandas DataFrame."""
        raw_bytes = self.get_object_bytes(key=key, bucket=bucket)
        return pd.read_csv(io.BytesIO(raw_bytes), **pandas_kwargs)

    def put_object(
        self,
        key: str,
        body: bytes | str,
        bucket: Optional[str] = None,
        content_type: str = "text/csv"
    ) -> Dict[str, Any]:
        """Uploads data to the target S3 bucket."""
        target_bucket = bucket or self.bucket_name
        if not target_bucket:
            raise ValueError("Target S3 bucket is not configured.")

        payload = body.encode("utf-8") if isinstance(body, str) else body
        client = self.get_client()

        try:
            return client.put_object(
                Bucket=target_bucket,
                Key=key,
                Body=payload,
                ContentType=content_type
            )
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(
                f"Failed to put S3 object s3://{target_bucket}/{key}: {str(e)}"
            ) from e


class RDSResource(ConfigurableResource):
    """Reusable Dagster resource for interacting with Amazon RDS (PostgreSQL).

    Directly encapsulates database connection pooling, query execution,
    and incident record lifecycle operations without ad-hoc connection handling.
    """

    host: str = Field(
        default="localhost",
        description="RDS Hostname / endpoint"
    )
    port: int = Field(
        default=5432,
        description="RDS Port"
    )
    database: str = Field(
        default="dataops_db",
        description="Database name"
    )
    username: str = Field(
        default="postgres",
        description="Database username"
    )
    password: str = Field(
        default="postgres",
        description="Database password"
    )
    schema_name: str = Field(
        default="public",
        description="Target database schema"
    )
    driver: str = Field(
        default="postgresql+psycopg2",
        description="SQLAlchemy database driver"
    )
    connection_string: Optional[str] = Field(
        default=None,
        description="Direct connection URI (overrides separate credentials)"
    )
    pool_size: int = Field(
        default=5,
        description="Connection pool size"
    )
    max_overflow: int = Field(
        default=10,
        description="Maximum pool overflow"
    )
    pool_pre_ping: bool = Field(
        default=True,
        description="Verify connection liveness before checkout"
    )

    def get_connection_url(self) -> str:
        """Constructs or returns the SQLAlchemy database URL."""
        if self.connection_string:
            return self.connection_string
        return (
            f"{self.driver}://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    def get_engine(self) -> Engine:
        """Builds and returns an SQLAlchemy Engine."""
        url = self.get_connection_url()
        return create_engine(
            url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=self.pool_pre_ping,
        )

    def get_connection(self):
        """Context manager providing an active transactional database connection."""
        engine = self.get_engine()
        return engine.begin()

    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Executes a SQL query against Amazon RDS and returns results."""
        engine = self.get_engine()
        with engine.begin() as connection:
            result = connection.execute(text(query), params or {})
            if result.returns_rows:
                return [dict(row._mapping) for row in result.fetchall()]
            return []

    def record_incident(self, incident: IncidentRecord) -> None:
        """Persists a new incident record into Amazon RDS dataops_incidents table."""
        stmt = text("""
            INSERT INTO dataops_incidents (
                incident_id, pipeline_name, dagster_run_id, source_uri,
                incident_type, severity, error_summary, evidence_json, status,
                created_at, updated_at
            ) VALUES (
                :incident_id, :pipeline_name, :dagster_run_id, :source_uri,
                :incident_type, :severity, :error_summary, :evidence_json, :status,
                :created_at, :updated_at
            )
            ON CONFLICT (incident_id) DO NOTHING;
        """)
        params = {
            "incident_id": incident.incident_id,
            "pipeline_name": incident.pipeline_name,
            "dagster_run_id": incident.dagster_run_id,
            "source_uri": incident.source_uri,
            "incident_type": incident.incident_type,
            "severity": incident.severity.value,
            "error_summary": incident.error_summary,
            "evidence_json": json.dumps(incident.evidence_json),
            "status": incident.status.value,
            "created_at": incident.created_at,
            "updated_at": incident.updated_at,
        }
        with self.get_connection() as conn:
            conn.execute(stmt, params)

    def fetch_and_lock_pending_incidents(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Atomically fetches and locks pending incidents for processing."""
        stmt = text("""
            UPDATE dataops_incidents
            SET status = 'processing', updated_at = now()
            WHERE incident_id IN (
                SELECT incident_id FROM dataops_incidents
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            RETURNING incident_id, pipeline_name, source_uri, incident_type, error_summary, evidence_json;
        """)
        with self.get_connection() as conn:
            result = conn.execute(stmt, {"limit": limit})
            return [dict(row._mapping) for row in result.fetchall()]

    def update_incident_resolution(
        self,
        incident_id: str,
        status: str,
        selected_action: Optional[str] = None,
        decision_confidence: Optional[float] = None,
        action_result: Optional[Dict[str, Any]] = None,
        incident_report: Optional[str] = None
    ) -> None:
        """Updates the incident with action outcomes, confidence, and Bedrock report."""
        stmt = text("""
            UPDATE dataops_incidents
            SET status = :status,
                selected_action = :selected_action,
                decision_confidence = :decision_confidence,
                action_result = :action_result,
                incident_report = :incident_report,
                updated_at = now()
            WHERE incident_id = :incident_id;
        """)
        params = {
            "incident_id": incident_id,
            "status": status,
            "selected_action": selected_action,
            "decision_confidence": decision_confidence,
            "action_result": json.dumps(action_result) if action_result else None,
            "incident_report": incident_report,
        }
        with self.get_connection() as conn:
            conn.execute(stmt, params)

    def get_recent_incidents(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves recent incidents for dashboard and monitoring."""
        return self.execute_query(
            "SELECT * FROM dataops_incidents ORDER BY created_at DESC LIMIT :limit;",
            {"limit": limit}
        )

    def table_exists(
        self,
        table_name: str,
        schema: Optional[str] = None
    ) -> bool:
        """Checks if a table exists in Amazon RDS."""
        engine = self.get_engine()
        inspector = inspect(engine)
        target_schema = schema or self.schema_name
        return inspector.has_table(table_name, schema=target_schema)

    def get_table_row_count(
        self,
        table_name: str,
        schema: Optional[str] = None
    ) -> int:
        """Returns row count for the specified table in Amazon RDS."""
        safe_table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name).lower()
        target_schema = schema or self.schema_name
        full_table = f"{target_schema}.{safe_table_name}" if target_schema else safe_table_name

        results = self.execute_query(f"SELECT COUNT(*) AS cnt FROM {full_table};")
        if results and "cnt" in results[0]:
            return int(results[0]["cnt"])
        return 0
