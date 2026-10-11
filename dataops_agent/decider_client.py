"""Client interface for Strands Decider 2B action selection."""

import os
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
import requests

ACTION_CRITERIA: Dict[str, str] = {
    "inspect_schema": "Compare observed columns and data types against the registered dataset contract to identify missing or unexpected fields.",
    "profile_nulls": "Calculate column null counts and percentages to determine if missing value thresholds are breached.",
    "quarantine_batch": "Isolate the failing batch into the quarantine area to preserve data lake integrity and prevent dirty reads.",
    "retry_pipeline": "Re-execute the pipeline for transient storage or network errors within bounded retry limits.",
    "alert_engineer": "Escalate the incident to on-call data engineers for human investigation and manual remediation.",
}


class DeciderChoiceResult(BaseModel):
    selected_action: str
    confidence: float
    rationale: str
    stage: str
    probabilities: Optional[Dict[str, float]] = None


class StrandsDeciderClient:
    """Client for Strands Decider 2B decision engine adhering to the System One API."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        api_key: Optional[str] = None,
        mock_fallback: bool = True
    ):
        self.endpoint_url = endpoint_url or os.getenv("DECIDER_ENDPOINT_URL")
        self.api_key = api_key or os.getenv("DECIDER_API_KEY")
        self.mock_fallback = mock_fallback if mock_fallback is not None else os.getenv("DECIDER_MOCK_FALLBACK", "true").lower() == "true"

    def select_action(
        self,
        incident_id: str,
        question: str,
        choices: List[str],
        incident_context: Dict[str, Any],
        stage: str = "diagnosis"
    ) -> DeciderChoiceResult:
        """Submits typed question and choices to Strands Decider 2B via System One API."""
        # 1. Attempt calling remote System One endpoint if configured
        if self.endpoint_url and not self.mock_fallback:
            try:
                headers = {"Content-Type": "application/json"}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"

                criteria = {c: ACTION_CRITERIA.get(c, f"Execute {c}") for c in choices}
                payload = {
                    "model": os.getenv("DECIDER_MODEL_NAME", "strands-decider-latest"),
                    "state": incident_context,
                    "questions": {
                        "next_action": {
                            "type": "choice",
                            "instructions": question,
                            "criteria": criteria,
                        }
                    },
                }

                url = self.endpoint_url
                if not url.endswith("/v1/systemone"):
                    url = f"{url.rstrip('/')}/v1/systemone"

                response = requests.post(url, json=payload, headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    ans = data.get("answers", {}).get("next_action", {})
                    selected = ans.get("choice", choices[0])
                    conf = float(ans.get("confidence", 0.95))
                    probs = ans.get("probabilities", {})
                    return DeciderChoiceResult(
                        selected_action=selected,
                        confidence=conf,
                        rationale=f"Selected via Strands Decider 2B (p={probs.get(selected, conf):.3f}).",
                        stage=stage,
                        probabilities=probs,
                    )
            except Exception:
                if not self.mock_fallback:
                    raise

        # 2. Deterministic evidence-driven fallback simulation
        return self._simulate_decider(choices, incident_context, stage)

    def _simulate_decider(
        self,
        choices: List[str],
        context: Dict[str, Any],
        stage: str
    ) -> DeciderChoiceResult:
        """Simulates Strands Decider 2B selection using structured evidence."""
        incident_type = context.get("incident_type", "")
        error_summary = context.get("error_summary", "").lower()

        if stage == "diagnosis":
            if "schema" in incident_type.lower() or "column" in error_summary:
                action = "inspect_schema" if "inspect_schema" in choices else choices[0]
                rationale = "Columns mismatch indicated; diagnosing schema discrepancy against contract."
            elif "null" in incident_type.lower() or "missing" in error_summary:
                action = "profile_nulls" if "profile_nulls" in choices else choices[0]
                rationale = "Missing data points detected; profiling null distribution and thresholds."
            else:
                action = "inspect_schema" if "inspect_schema" in choices else choices[0]
                rationale = "Diagnosing dataset structure as initial evidence collection step."

            return DeciderChoiceResult(
                selected_action=action,
                confidence=0.96,
                rationale=rationale,
                stage=stage,
                probabilities={action: 0.96},
            )

        # Stage == remediation
        diagnostic_output = context.get("diagnostic_output", {})
        retry_count = context.get("retry_count", 0)

        # 1. Transient error retry
        if "transient" in incident_type.lower() or "slowdown" in error_summary or "timeout" in error_summary:
            if retry_count < 2 and "retry_pipeline" in choices:
                return DeciderChoiceResult(
                    selected_action="retry_pipeline",
                    confidence=0.91,
                    rationale=f"Transient storage error detected; retry count ({retry_count}) within allowable bounds.",
                    stage=stage,
                    probabilities={"retry_pipeline": 0.91},
                )

        # 2. Schema violations / unexpected columns
        if diagnostic_output.get("unexpected_columns") or not diagnostic_output.get("schema_valid", True):
            if "quarantine_batch" in choices:
                return DeciderChoiceResult(
                    selected_action="quarantine_batch",
                    confidence=0.94,
                    rationale="Unapproved schema drift detected; isolating batch to prevent downstream corruption.",
                    stage=stage,
                    probabilities={"quarantine_batch": 0.94},
                )

        # 3. Null threshold violations
        if diagnostic_output.get("threshold_violations") or not diagnostic_output.get("null_profile_valid", True):
            if "quarantine_batch" in choices:
                return DeciderChoiceResult(
                    selected_action="quarantine_batch",
                    confidence=0.93,
                    rationale="Null threshold breach observed; batch quarantined per data quality policy.",
                    stage=stage,
                    probabilities={"quarantine_batch": 0.93},
                )

        # 4. Default fallback escalation
        fallback_action = "alert_engineer" if "alert_engineer" in choices else choices[0]
        return DeciderChoiceResult(
            selected_action=fallback_action,
            confidence=0.88,
            rationale="Uncertainty remains high or manual inspection required; escalating to engineer.",
            stage=stage,
            probabilities={fallback_action: 0.88},
        )

