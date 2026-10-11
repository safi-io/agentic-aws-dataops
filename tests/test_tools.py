import os
from dataops_agent.tools import (
    inspect_schema,
    profile_nulls,
    quarantine_batch,
    retry_pipeline,
    alert_engineer,
)


def test_inspect_schema_clean():
    clean_csv = "sample_data/clean.csv"
    res = inspect_schema(clean_csv)
    assert res["status"] == "success"
    assert res["schema_valid"] is True
    assert len(res["unexpected_columns"]) == 0
    assert len(res["missing_columns"]) == 0


def test_inspect_schema_unexpected_column():
    unexpected_csv = "sample_data/unexpected_column.csv"
    res = inspect_schema(unexpected_csv)
    assert res["status"] == "success"
    assert res["schema_valid"] is False
    assert "customer_segment" in res["unexpected_columns"]


def test_profile_nulls_violations():
    null_csv = "sample_data/high_null_rate.csv"
    res = profile_nulls(null_csv, threshold=0.15)
    assert res["status"] == "success"
    assert res["null_profile_valid"] is False
    assert len(res["threshold_violations"]) > 0


def test_quarantine_batch(tmp_path):
    source_file = tmp_path / "faulty.csv"
    source_file.write_text("a,b\n1,2")
    q_dir = tmp_path / "quarantine"

    res = quarantine_batch(str(source_file), quarantine_dir=str(q_dir), incident_id="inc-123")
    assert res["status"] == "success"
    assert res["quarantined"] is True
    assert os.path.exists(res["quarantined_destination"])
    assert os.path.exists(str(source_file))


def test_retry_pipeline_bounds():
    # Attempt 0 - succeeds with retry count 1
    res1 = retry_pipeline("test_pipe", "inc-1", current_retry_count=0, max_retries=2)
    assert res1["status"] == "success"
    assert res1["new_retry_count"] == 1

    # Attempt 2 - exceeds limit
    res2 = retry_pipeline("test_pipe", "inc-1", current_retry_count=2, max_retries=2)
    assert res2["status"] == "failed"
    assert res2["escalation_required"] is True


def test_alert_engineer():
    res = alert_engineer("inc-test", "pipeline_1", "Manual review required", severity="critical")
    assert res["status"] == "success"
    assert res["escalated"] is True
    assert res["severity"] == "critical"

