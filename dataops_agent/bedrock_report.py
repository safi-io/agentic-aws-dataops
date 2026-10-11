"""Amazon Bedrock integration for incident report generation."""

import os
import json
from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import BotoCoreError, ClientError

DEFAULT_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-haiku-20240307-v1:0")


class BedrockReportGenerator:
    """Generates verifiable incident summaries and next steps via Amazon Bedrock."""

    def __init__(
        self,
        region_name: Optional[str] = None,
        model_id: Optional[str] = None
    ):
        self.region_name = region_name or os.getenv("AWS_REGION", "us-east-1")
        self.model_id = model_id or DEFAULT_MODEL_ID
        self._bedrock_client = None

    def _get_client(self):
        if self._bedrock_client is None:
            self._bedrock_client = boto3.client(
                service_name="bedrock-runtime",
                region_name=self.region_name
            )
        return self._bedrock_client

    def generate_report(
        self,
        incident_id: str,
        pipeline_name: str,
        incident_type: str,
        evidence: Dict[str, Any],
        selected_action: str,
        tool_result: Dict[str, Any]
    ) -> str:
        """Synthesizes structured telemetry and tool results into an operational incident report."""
        prompt = self._build_prompt(
            incident_id=incident_id,
            pipeline_name=pipeline_name,
            incident_type=incident_type,
            evidence=evidence,
            selected_action=selected_action,
            tool_result=tool_result,
        )

        try:
            client = self._get_client()
            # Payload tailored for Claude 3 models via Bedrock Messages API
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1024,
                "temperature": 0.2,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt,
                    }
                ],
            })

            response = client.invoke_model(
                modelId=self.model_id,
                body=body,
                contentType="application/json",
                accept="application/json",
            )
            response_body = json.loads(response["body"].read().decode("utf-8"))
            return response_body["content"][0]["text"]

        except (BotoCoreError, ClientError, Exception) as e:
            # Fallback deterministic report if Bedrock credentials or network unavailable
            return self._generate_fallback_report(
                incident_id=incident_id,
                pipeline_name=pipeline_name,
                incident_type=incident_type,
                evidence=evidence,
                selected_action=selected_action,
                tool_result=tool_result,
                error_note=str(e),
            )

    def _build_prompt(
        self,
        incident_id: str,
        pipeline_name: str,
        incident_type: str,
        evidence: Dict[str, Any],
        selected_action: str,
        tool_result: Dict[str, Any]
    ) -> str:
        return f"""You are an expert DataOps Incident Response Engineer. Generate a concise, factual incident report based solely on the verified evidence below. Never claim an action succeeded unless confirmed by the tool result.

Incident Details:
- Incident ID: {incident_id}
- Pipeline Name: {pipeline_name}
- Classification: {incident_type}
- Diagnostic Evidence: {json.dumps(evidence, indent=2)}
- Strands Decider Choice: {selected_action}
- Confirmed Tool Execution Result: {json.dumps(tool_result, indent=2)}

Format the report with these sections:
1. Incident Summary
2. Probable Cause
3. Evidence Gathered
4. Action Taken & Confirmed Outcome
5. Remaining Uncertainty
6. Recommended Next Steps
"""

    def _generate_fallback_report(
        self,
        incident_id: str,
        pipeline_name: str,
        incident_type: str,
        evidence: Dict[str, Any],
        selected_action: str,
        tool_result: Dict[str, Any],
        error_note: str
    ) -> str:
        outcome_status = tool_result.get("status", "unknown")
        confirmation = tool_result.get("confirmation") or tool_result.get("error") or "Execution recorded."

        return f"""### Incident Report: {incident_id}
*Generated via DataOps Local Reporter (Bedrock offline: {error_note[:80]}...)*

**1. Incident Summary:**
Pipeline `{pipeline_name}` encountered a `{incident_type}` failure during dataset validation.

**2. Probable Cause:**
Investigation identified contract discrepancy or quality threshold breach: {evidence.get('error_summary', 'Dataset validation failure')}.

**3. Evidence Gathered:**
- Observed diagnostic data: {json.dumps(evidence.get('diagnostic_output', evidence), default=str)}

**4. Action Taken & Confirmed Outcome:**
- **Selected Action:** `{selected_action}`
- **Execution Status:** `{outcome_status}`
- **Confirmed Result:** {confirmation}

**5. Remaining Uncertainty:**
- Compatibility with downstream models and consumer contracts remains unverified until reviewed.

**6. Recommended Next Steps:**
- Review schema changes with data contract owners.
- Update pipeline expectations if new columns or null rates are legitimate.
"""

