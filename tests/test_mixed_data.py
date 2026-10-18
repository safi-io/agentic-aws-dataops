"""Exercise mixed orders and ensure unreadable batches create incidents."""

from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from dagster import materialize_to_memory
import pandas as pd
import pytest

from config.settings import Settings
from dagster_project import assets
from dataops_agent.tools.inspect_schema import DEFAULT_ORDER_SCHEMA


SAMPLES = Path(__file__).resolve().parent.parent / "sample_data" / "mixed_cases"
CASES = json.loads((SAMPLES / "manifest.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_mixed_batch_validation(case):
    try:
        frame = pd.read_csv(SAMPLES / case["file"])
    except (pd.errors.ParserError, pd.errors.EmptyDataError):
        assert case["expected_status"] == "FAILURE"
        return
    result = assets.validate_orders_frame(
        frame, target_schema=DEFAULT_ORDER_SCHEMA, max_null_percentage=0.15
    )
    assert result["valid"] == (case["expected_status"] == "SUCCESS")
    if not result["valid"]:
        assert assets._incident_type_from_validation(result) == case["expected_incident_type"]


@pytest.mark.parametrize("filename", ["empty_file.csv", "malformed_csv.csv"])
def test_unreadable_csv_records_incident_and_never_writes_silver(filename, monkeypatch):
    settings = Settings(
        _env_file=None, DATABASE_URL="postgresql+psycopg://test:test@localhost/test",
        S3_BUCKET_NAME="mixed-data-tests", DECIDER_MODE="cli",
    )
    s3 = Mock()
    s3.get_object.return_value = {"Body": BytesIO((SAMPLES / filename).read_bytes())}
    repository = Mock()
    repository.create_incident.return_value = SimpleNamespace(incident_id="parse-test")
    monkeypatch.setattr(assets, "get_settings", lambda: settings)
    monkeypatch.setattr(assets, "get_s3_client", lambda settings: s3)
    monkeypatch.setattr(assets, "IncidentRepository", lambda url: repository)
    result = materialize_to_memory(
        [assets.bronze_orders, assets.silver_orders], raise_on_error=False,
        run_config={"ops": {"bronze_orders": {"config": {"source_key": f"bronze/{filename}"}}}},
    )
    assert not result.success
    s3.put_object.assert_not_called()
    repository.create_incident.assert_called_once()
    incident = repository.create_incident.call_args.kwargs
    assert incident["incident_type"] == "data_quality_failure"
    assert incident["dagster_run_id"] == result.run_id
    assert incident["source_uri"] == f"s3://mixed-data-tests/bronze/{filename}"
    assert incident["evidence_json"]["asset_key"] == "bronze_orders"
    assert "CSV parsing failed" in incident["error_summary"]
