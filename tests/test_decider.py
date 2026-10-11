"""Unit tests for Strands Decider 2B client and action selection."""

import pytest
from dataops_agent.decider_client import StrandsDeciderClient


def test_decider_diagnosis_schema_selection():
    client = StrandsDeciderClient(mock_fallback=True)
    choices = ["inspect_schema", "profile_nulls", "alert_engineer"]

    decision = client.select_action(
        incident_id="inc-test-1",
        question="Which diagnostic tool should be executed?",
        choices=choices,
        incident_context={"incident_type": "SchemaMismatch", "error_summary": "Unexpected column"},
        stage="diagnosis",
    )

    assert decision.selected_action == "inspect_schema"
    assert decision.confidence > 0.90


def test_decider_remediation_quarantine_selection():
    client = StrandsDeciderClient(mock_fallback=True)
    choices = ["quarantine_batch", "retry_pipeline", "alert_engineer"]

    decision = client.select_action(
        incident_id="inc-test-2",
        question="Which recovery action should be executed?",
        choices=choices,
        incident_context={
            "incident_type": "SchemaMismatch",
            "diagnostic_output": {"schema_valid": False, "unexpected_columns": ["customer_segment"]},
        },
        stage="remediation",
    )

    assert decision.selected_action == "quarantine_batch"
    assert decision.confidence > 0.90

