"""Dagster data assets for Bronze raw ingestion and Silver validation using S3 and RDS resources."""

import os
import uuid
import re
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from dagster import asset, AssetExecutionContext, Config
from pydantic import Field

from dataops_agent.incident_state import IncidentRecord, IncidentStatus, IncidentSeverity
from dagster_project.resources import S3Resource, RDSResource

EXPECTED_COLUMNS = ["order_id", "customer_id", "amount"]
NULL_THRESHOLD = 0.15


class BronzeIngestionConfig(Config):
    """Configuration for Bronze ingestion asset."""
    s3_key: Optional[str] = Field(
        default=None,
        description="S3 object key in Bronze raw zone"
    )
    s3_bucket: Optional[str] = Field(
        default=None,
        description="Target S3 bucket override"
    )
    file_path: Optional[str] = Field(
        default=None,
        description="Local file path override"
    )


@asset(group_name="ingestion")
def orders_bronze_data(
    context: AssetExecutionContext,
    config: BronzeIngestionConfig,
    s3: S3Resource,
) -> str:
    """Reads Bronze source CSV file from Amazon S3 raw zone or local fallback."""

    bucket = config.s3_bucket or s3.bucket_name
    if not bucket:
        raise ValueError("S3 bucket name not provided in config or S3Resource.")

    s3_uri = f"s3://{bucket}/{config.s3_key}"
    context.log.info(f"Downloading raw CSV from {s3_uri} using S3Resource...")

    # Cache locally in storage/bronze for downstream validation & incident diagnostic tools
    storage_dir = os.path.join(os.getenv("LOCAL_STORAGE_DIR", "./storage"), "bronze")
    local_target = os.path.join(storage_dir, os.path.basename(config.s3_key))

    s3.download_file(key=config.s3_key, local_path=local_target, bucket=bucket)
    context.log.info(f"Saved raw file locally to {local_target} for validation.")
    return local_target

@asset(group_name="validation")
def orders_validated_silver(
    context: AssetExecutionContext,
    orders_bronze_data: str,
    s3: S3Resource,
    rds: RDSResource,
) -> Optional[str]:
    """Validates Bronze CSV against contract and writes to Silver S3 if clean."""
    file_path = orders_bronze_data
    df = pd.read_csv(file_path)
    observed_cols = list(df.columns)
    context.log.info(f"Validating dataset columns: {observed_cols}")

    # Check columns
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in observed_cols]
    unexpected_cols = [c for c in observed_cols if c not in EXPECTED_COLUMNS]

    if missing_cols or unexpected_cols:
        incident_id = f"inc-{uuid.uuid4().hex[:8]}"
        error_msg = f"Schema mismatch: missing={missing_cols}, unexpected={unexpected_cols}"
        context.log.error(error_msg)

        incident = IncidentRecord(
            incident_id=incident_id,
            pipeline_name="orders_ingestion_pipeline",
            dagster_run_id=context.run_id,
            source_uri=file_path,
            incident_type="SchemaMismatch",
            severity=IncidentSeverity.HIGH if missing_cols else IncidentSeverity.MEDIUM,
            error_summary=error_msg,
            evidence_json={
                "observed_columns": observed_cols,
                "missing_columns": missing_cols,
                "unexpected_columns": unexpected_cols,
                "source_uri": file_path,
            },
            status=IncidentStatus.PENDING,
        )
        # Directly use RDSResource to record the incident
        rds.record_incident(incident)
        raise ValueError(f"Pipeline stopped: {error_msg}. Incident recorded: {incident_id}")

    # Check null rates
    null_counts = df.isnull().sum()
    null_rates = (null_counts / len(df)).to_dict()
    violations = {c: r for c, r in null_rates.items() if r > NULL_THRESHOLD}

    if violations:
        incident_id = f"inc-{uuid.uuid4().hex[:8]}"
        error_msg = f"Null threshold violations in columns: {violations}"
        context.log.error(error_msg)

        incident = IncidentRecord(
            incident_id=incident_id,
            pipeline_name="orders_ingestion_pipeline",
            dagster_run_id=context.run_id,
            source_uri=file_path,
            incident_type="NullRateBreach",
            severity=IncidentSeverity.MEDIUM,
            error_summary=error_msg,
            evidence_json={
                "null_rates": null_rates,
                "violations": violations,
                "source_uri": file_path,
            },
            status=IncidentStatus.PENDING,
        )
        # Directly use RDSResource to record the incident
        rds.record_incident(incident)
        raise ValueError(f"Pipeline stopped: {error_msg}. Incident recorded: {incident_id}")

    # The data is valid -> Write to Silver S3 zone
    base_name = os.path.basename(file_path)
    clean_base = re.sub(r"\.csv$", "", base_name, flags=re.IGNORECASE)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    silver_filename = f"validated_{clean_base}_{timestamp}.parquet"

    # Write locally
    silver_dir = os.path.join(os.getenv("LOCAL_STORAGE_DIR", "./storage"), "silver")
    os.makedirs(silver_dir, exist_ok=True)
    local_silver_path = os.path.join(silver_dir, silver_filename)
    df.to_parquet(local_silver_path, index=False)
    context.log.info(f"Published validated batch locally to Silver: {local_silver_path}")

    # Directly use S3Resource to upload to S3 Silver zone if configured
    if s3.bucket_name:
        silver_s3_key = f"silver/{silver_filename}"
        try:
            with open(local_silver_path, "rb") as f:
                s3.put_object(
                    key=silver_s3_key,
                    body=f.read(),
                    bucket=s3.bucket_name,
                    content_type="application/octet-stream",
                )
            context.log.info(f"Published validated batch to S3 Silver: s3://{s3.bucket_name}/{silver_s3_key}")
        except Exception as e:
            context.log.warning(f"Could not upload to S3 Silver zone: {e}")

    return local_silver_path
