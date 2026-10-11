"""Unit tests for deterministic policy enforcement and guardrails."""

import pytest
from dataops_agent.policies import PolicyEngine


def test_allowlist_enforcement():
    engine = PolicyEngine()
    is_allowed, action, reason = engine.validate_action(
        action="unauthorized_delete_all",
        incident_type="SchemaMismatch",
        evidence={},
    )
    assert is_allowed is False
    assert action == "alert_engineer"
    assert "not in allowlist" in reason


def test_retry_limits():
    engine = PolicyEngine(max_retries=2)

    # Exceeding retry count
    is_allowed, action, reason = engine.validate_action(
        action="retry_pipeline",
        incident_type="TransientStorageError",
        evidence={"retry_count": 2},
    )
    assert is_allowed is False
    assert action == "alert_engineer"
    assert "Retry limit exceeded" in reason


def test_retry_on_non_transient_error():
    engine = PolicyEngine(max_retries=2)

    # Non-transient errors cannot be retried -> overridden to quarantine
    is_allowed, action, reason = engine.validate_action(
        action="retry_pipeline",
        incident_type="SchemaMismatch",
        evidence={"retry_count": 0},
    )
    assert is_allowed is False
    assert action == "quarantine_batch"
    assert "Non-transient incident type" in reason

