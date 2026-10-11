"""Deterministic safety policies and guardrails for incident actions."""

from typing import Any, Dict, List, Tuple

# Predefined allowable actions
ALLOWLISTED_ACTIONS: List[str] = [
    "inspect_schema",
    "profile_nulls",
    "quarantine_batch",
    "retry_pipeline",
    "alert_engineer",
]

# Transient error classifications
TRANSIENT_ERROR_TYPES: List[str] = [
    "TransientStorageError",
    "S3SlowDown",
    "ConnectionTimeout",
    "TemporaryNetworkGlitch",
    "ServiceUnavailable",
]

DEFAULT_MAX_RETRIES = 2
DEFAULT_MAX_NULL_RATE = 0.15


class PolicyEngine:
    """Enforces deterministic bounds on Strands Decider actions."""

    def __init__(self, max_retries: int = DEFAULT_MAX_RETRIES):
        self.max_retries = max_retries

    def validate_action(
        self,
        action: str,
        incident_type: str,
        evidence: Dict[str, Any]
    ) -> Tuple[bool, str, str]:
        """Validates if the chosen action complies with system safety policies.

        Returns:
            Tuple of (is_allowed: bool, validated_action: str, reason: str)
        """
        # 1. Allowlist verification
        if action not in ALLOWLISTED_ACTIONS:
            return (
                False,
                "alert_engineer",
                f"Action '{action}' is not in allowlist. Forcing human escalation."
            )

        # 2. Retry policy guardrails
        if action == "retry_pipeline":
            retry_count = evidence.get("retry_count", 0)
            if retry_count >= self.max_retries:
                return (
                    False,
                    "alert_engineer",
                    f"Retry limit exceeded ({retry_count}/{self.max_retries}). Escalating to engineer."
                )
            if incident_type not in TRANSIENT_ERROR_TYPES:
                return (
                    False,
                    "quarantine_batch",
                    f"Non-transient incident type '{incident_type}' cannot be resolved via retry. Overriding to quarantine."
                )

        # 3. Source immutability
        source_uri = evidence.get("source_uri", "")
        if "bronze" in source_uri.lower() and action == "delete_source":
            return (
                False,
                "alert_engineer",
                "Raw bronze layer is immutable. Deletion prohibited."
            )

        return (True, action, "Action adheres to deterministic safety policies.")

