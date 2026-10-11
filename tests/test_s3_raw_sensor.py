"""Unit tests for s3_raw_bronze_sensor and pending_incident_sensor event monitoring."""

import json
from unittest.mock import patch
from dagster import build_sensor_context, RunRequest, SkipReason

from dagster_project.sensors import s3_raw_bronze_sensor, pending_incident_sensor
from dagster_project.resources import S3Resource, RDSResource


def test_s3_raw_bronze_sensor_no_files():
    s3 = S3Resource(bucket_name="dataops-bucket", prefix="bronze/")

    with patch.object(S3Resource, "list_objects", return_value=[]):
        context = build_sensor_context(resources={"s3": s3})
        results = list(s3_raw_bronze_sensor(context))

        assert len(results) == 1
        assert isinstance(results[0], SkipReason)
        assert "No raw CSV files found" in results[0].skip_message


def test_s3_raw_bronze_sensor_discovers_file_and_updates_cursor():
    s3 = S3Resource(bucket_name="dataops-bucket", prefix="bronze/")
    mock_files = [
        {
            "key": "bronze/customers_batch_1.csv",
            "size": 1024,
            "last_modified": "2026-10-11T10:00:00+00:00",
            "etag": "etag-111",
        }
    ]

    with patch.object(S3Resource, "list_objects", return_value=mock_files):
        context = build_sensor_context(cursor=None, resources={"s3": s3})
        results = list(s3_raw_bronze_sensor(context))

        assert len(results) == 1
        assert isinstance(results[0], RunRequest)
        req: RunRequest = results[0]

        assert req.run_key == "dataops-bucket:bronze/customers_batch_1.csv:etag-111"
        assert req.run_config["ops"]["orders_bronze_data"]["config"]["s3_key"] == "bronze/customers_batch_1.csv"
        assert req.run_config["ops"]["orders_bronze_data"]["config"]["s3_bucket"] == "dataops-bucket"

        # Verify cursor was updated
        assert context.cursor is not None
        cursor_dict = json.loads(context.cursor)
        assert "bronze/customers_batch_1.csv" in cursor_dict["processed_keys"]
        assert cursor_dict["processed_keys"]["bronze/customers_batch_1.csv"] == "etag-111"


def test_s3_raw_bronze_sensor_prevents_duplicate_runs():
    s3 = S3Resource(bucket_name="dataops-bucket", prefix="bronze/")
    mock_files = [
        {
            "key": "bronze/orders_batch_1.csv",
            "size": 2048,
            "last_modified": "2026-10-11T12:00:00+00:00",
            "etag": "etag-aaa",
        }
    ]

    with patch.object(S3Resource, "list_objects", return_value=mock_files):
        # 1. First evaluation: discovers file
        context = build_sensor_context(cursor=None, resources={"s3": s3})
        first_results = list(s3_raw_bronze_sensor(context))
        assert len(first_results) == 1
        assert isinstance(first_results[0], RunRequest)

        # 2. Second evaluation: file already in cursor -> yields SkipReason
        saved_cursor = context.cursor
        context2 = build_sensor_context(cursor=saved_cursor, resources={"s3": s3})
        second_results = list(s3_raw_bronze_sensor(context2))
        assert len(second_results) == 1
        assert isinstance(second_results[0], SkipReason)
        assert "already been processed" in second_results[0].skip_message


def test_s3_raw_bronze_sensor_incremental_discovery():
    s3 = S3Resource(bucket_name="dataops-bucket", prefix="bronze/")

    # Setup state with batch 1 already processed
    existing_cursor = json.dumps({
        "last_mtime": "2026-10-11T12:00:00+00:00",
        "processed_keys": {"bronze/orders_batch_1.csv": "etag-aaa"},
    })

    # S3 now returns batch 1 and a new batch 2
    mock_files = [
        {
            "key": "bronze/orders_batch_1.csv",
            "size": 2048,
            "last_modified": "2026-10-11T12:00:00+00:00",
            "etag": "etag-aaa",
        },
        {
            "key": "bronze/orders_batch_2.csv",
            "size": 4096,
            "last_modified": "2026-10-11T13:00:00+00:00",
            "etag": "etag-bbb",
        },
    ]

    with patch.object(S3Resource, "list_objects", return_value=mock_files):
        context = build_sensor_context(cursor=existing_cursor, resources={"s3": s3})
        results = list(s3_raw_bronze_sensor(context))

        # Should only process batch 2
        assert len(results) == 1
        assert isinstance(results[0], RunRequest)
        assert results[0].run_key == "dataops-bucket:bronze/orders_batch_2.csv:etag-bbb"


def test_pending_incident_sensor_dispatches_remediation():
    rds = RDSResource()
    mock_incidents = [
        {
            "incident_id": "inc-test-456",
            "pipeline_name": "orders_ingestion_pipeline",
            "source_uri": "s3://bucket/bronze/test.csv",
            "incident_type": "SchemaMismatch",
            "error_summary": "Unexpected column found",
            "evidence_json": {},
        }
    ]

    with patch.object(RDSResource, "fetch_and_lock_pending_incidents", return_value=mock_incidents):
        context = build_sensor_context(resources={"rds": rds})
        results = list(pending_incident_sensor(context))

        assert len(results) == 1
        assert isinstance(results[0], RunRequest)
        assert results[0].run_key == "remediate_inc-test-456"
        assert results[0].run_config["ops"]["run_incident_agent_op"]["config"]["incident_id"] == "inc-test-456"
