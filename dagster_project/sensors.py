"""Dagster sensors for incident detection and Amazon S3 Raw (Bronze) event monitoring."""

import json
from typing import Any, Dict, List
from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from dagster_project.jobs import incident_remediation_job, orders_pipeline_job
from dagster_project.resources import S3Resource, RDSResource


@sensor(
    job=incident_remediation_job,
    minimum_interval_seconds=15,
    description="Monitors PostgreSQL database for pending incidents and launches agent remediation.",
)
def pending_incident_sensor(
    context: SensorEvaluationContext,
    rds: RDSResource,
):
    """Monitors Amazon RDS for pending incidents and launches agent remediation."""
    try:
        pending_records = rds.fetch_and_lock_pending_incidents(limit=5)
    except Exception as e:
        context.log.error(f"Error querying Amazon RDS for pending incidents: {e}")
        yield SkipReason(f"Database query error: {str(e)}")
        return

    if not pending_records:
        yield SkipReason("No pending incidents detected in PostgreSQL.")
        return

    for inc in pending_records:
        incident_id = inc["incident_id"]
        context.log.info(f"Sensor detected pending incident: {incident_id}")

        yield RunRequest(
            run_key=f"remediate_{incident_id}",
            run_config={
                "ops": {
                    "run_incident_agent_op": {
                        "config": {
                            "incident_id": incident_id,
                            "pipeline_name": inc.get("pipeline_name", "orders_ingestion_pipeline"),
                            "incident_type": inc.get("incident_type", "UnknownIncident"),
                            "source_uri": inc.get("source_uri", ""),
                            "error_summary": inc.get("error_summary", ""),
                        }
                    }
                }
            },
        )


# ==============================================================================
# S3 Raw (Bronze) Zone Event-Driven Sensor
# ==============================================================================

@sensor(
    job=orders_pipeline_job,
    minimum_interval_seconds=30,
    description="Monitors S3 Raw (Bronze) zone for new CSV files and triggers the ingestion pipeline toward Silver S3.",
)
def s3_raw_bronze_sensor(
    context: SensorEvaluationContext,
    s3: S3Resource,
):
    """Continuously polls Amazon S3 Bronze raw zone for new CSV files.

    Whenever a new CSV file arrives, it starts the ingestion pipeline toward
    the Silver zone. Tracks processed keys and ETags in the sensor cursor to
    prevent duplicate pipeline runs.
    """
    target_bucket = s3.bucket_name
    if not target_bucket:
        yield SkipReason(
            "S3Resource.bucket_name is not configured. Sensor is waiting for bucket configuration."
        )
        return

    # Bronze raw prefix (default bronze/)
    target_prefix = s3.prefix or "bronze/"

    try:
        objects = s3.list_objects(
            prefix=target_prefix,
            extension=".csv",
            bucket=target_bucket
        )
    except Exception as e:
        context.log.error(
            f"Failed to query raw S3 bucket '{target_bucket}' with prefix '{target_prefix}': {str(e)}"
        )
        yield SkipReason(f"S3 access error: {str(e)}")
        return

    if not objects:
        yield SkipReason(
            f"No raw CSV files found in s3://{target_bucket}/{target_prefix}"
        )
        return

    # Parse cursor state: {"last_mtime": "...", "processed_keys": {"key": "etag", ...}}
    cursor_data: Dict[str, Any] = {"last_mtime": "", "processed_keys": {}}
    if context.cursor:
        try:
            cursor_data = json.loads(context.cursor)
            if not isinstance(cursor_data, dict):
                cursor_data = {"last_mtime": str(context.cursor), "processed_keys": {}}
        except Exception:
            cursor_data = {"last_mtime": str(context.cursor), "processed_keys": {}}

    processed_keys: Dict[str, str] = cursor_data.get("processed_keys", {})
    last_mtime: str = cursor_data.get("last_mtime", "")

    # Sort raw objects chronologically by LastModified ascending
    sorted_objects = sorted(objects, key=lambda x: x["last_modified"])
    new_requests: List[RunRequest] = []
    latest_seen_mtime = last_mtime

    for item in sorted_objects:
        obj_key = item["key"]
        obj_etag = item.get("etag", "")
        obj_mtime = item["last_modified"]

        # Check if already processed with matching ETag
        if processed_keys.get(obj_key) == obj_etag:
            continue

        # If file is older than last_mtime and already recorded, skip
        if last_mtime and obj_mtime < last_mtime and obj_key in processed_keys:
            continue

        # Build unique, deterministic run key for Dagster run deduplication
        run_key = f"{target_bucket}:{obj_key}:{obj_etag or obj_mtime}"

        context.log.info(
            f"Sensor discovered new raw CSV: s3://{target_bucket}/{obj_key} (ETag: {obj_etag})"
        )

        run_config = {
            "ops": {
                "orders_bronze_data": {
                    "config": {
                        "s3_key": obj_key,
                        "s3_bucket": target_bucket,
                    }
                }
            }
        }

        new_requests.append(
            RunRequest(
                run_key=run_key,
                run_config=run_config,
                tags={
                    "source": "s3_raw_bronze_sensor",
                    "s3_bucket": target_bucket,
                    "s3_key": obj_key,
                    "s3_etag": obj_etag,
                    "last_modified": obj_mtime,
                    "zone": "bronze_raw",
                },
            )
        )

        # Mark processed in cursor state
        processed_keys[obj_key] = obj_etag
        if not latest_seen_mtime or obj_mtime > latest_seen_mtime:
            latest_seen_mtime = obj_mtime

    if not new_requests:
        yield SkipReason(
            f"All {len(objects)} raw CSV file(s) in s3://{target_bucket}/{target_prefix} "
            f"have already been processed."
        )
        return

    # Update cursor with newly processed keys and latest timestamp
    updated_cursor = json.dumps({
        "last_mtime": latest_seen_mtime,
        "processed_keys": processed_keys,
    })
    context.update_cursor(updated_cursor)

    context.log.info(
        f"Yielding {len(new_requests)} RunRequest(s) to start Bronze -> Silver ingestion."
    )
    for req in new_requests:
        yield req
