"""Bedrock-backed incident postmortem report generation."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

from botocore.client import BaseClient

from config.settings import Settings, get_bedrock_runtime_client, get_settings
from dataops_agent.decider_client import DeciderDecision
from dataops_agent.tools.base import ToolResult, ensure_json_object, incident_snapshot


LOGGER = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are an AWS DataOps incident postmortem writer.
Return concise Markdown with exactly these sections:
# Incident Report
## Summary
## Root Cause
## Evidence
## Deterministic Action Taken
## Recommended Engineer Actions

Use only facts from the JSON payload. If a fact is unknown, write "unknown".
Do not invent identifiers, timestamps, S3 keys, or remediation outcomes."""


class BedrockReportGenerator:
    """Generate structured Markdown incident reports using Bedrock Converse."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        bedrock_client: BaseClient | None = None,
        max_tokens: int = 1200,
    ) -> None:
        if max_tokens < 256:
            raise ValueError("max_tokens must be at least 256")
        self._settings = settings
        self._bedrock_client = bedrock_client
        self._max_tokens = max_tokens

    def generate_report(
        self,
        *,
        incident: Any,
        evidence_decision: DeciderDecision,
        evidence_result: ToolResult,
        remediation_decision: DeciderDecision,
        remediation_result: ToolResult,
    ) -> str:
        """Generate a Markdown report, falling back deterministically on AWS failure."""

        payload = self._report_payload(
            incident=incident,
            evidence_decision=evidence_decision,
            evidence_result=evidence_result,
            remediation_decision=remediation_decision,
            remediation_result=remediation_result,
        )
        try:
            settings = self._settings or get_settings()
            client = self._bedrock_client or get_bedrock_runtime_client(settings)
            response = client.converse(
                modelId=settings.BEDROCK_MODEL_ID,
                system=[{"text": SYSTEM_PROMPT}],
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "text": (
                                    "Write the incident report from this JSON payload:\n"
                                    + json.dumps(payload, sort_keys=True, indent=2)
                                )
                            }
                        ],
                    }
                ],
                inferenceConfig={
                    "maxTokens": self._max_tokens,
                    "temperature": 0.0,
                },
                requestMetadata={
                    "component": "agentic-aws-dataops",
                    "incident_id": str(payload["incident"].get("incident_id") or "unknown"),
                },
            )
            report = self._extract_text(response)
            if report.strip() == "":
                raise ValueError("Bedrock returned an empty report")
            return report.strip()
        except Exception as exc:
            LOGGER.warning(
                "Bedrock report generation failed; using deterministic report",
                extra={
                    "incident_id": payload["incident"].get("incident_id"),
                    "error_type": type(exc).__name__,
                },
            )
            return deterministic_report(payload, error=str(exc))

    @staticmethod
    def _report_payload(
        *,
        incident: Any,
        evidence_decision: DeciderDecision,
        evidence_result: ToolResult,
        remediation_decision: DeciderDecision,
        remediation_result: ToolResult,
    ) -> dict[str, Any]:
        return ensure_json_object(
            {
                "incident": incident_snapshot(incident),
                "evidence_decision": evidence_decision.repository_metadata(),
                "evidence_result": evidence_result.model_dump(mode="json"),
                "remediation_decision": remediation_decision.repository_metadata(),
                "remediation_result": remediation_result.model_dump(mode="json"),
                "existing_evidence_json": getattr(incident, "evidence_json", {}) or {},
                "existing_action_result": getattr(incident, "action_result", None),
            },
            "report_payload",
        )

    @staticmethod
    def _extract_text(response: Mapping[str, Any]) -> str:
        message = response["output"]["message"]
        content_blocks = message["content"]
        if not isinstance(content_blocks, list):
            raise TypeError("Bedrock response content must be a list")
        parts = []
        for block in content_blocks:
            if isinstance(block, Mapping) and isinstance(block.get("text"), str):
                parts.append(block["text"])
        return "\n".join(parts)


def deterministic_report(payload: Mapping[str, Any], *, error: str | None = None) -> str:
    """Build a deterministic Markdown report when Bedrock is unavailable."""

    incident = _mapping(payload.get("incident"))
    evidence_decision = _mapping(payload.get("evidence_decision"))
    evidence_result = _mapping(payload.get("evidence_result"))
    remediation_decision = _mapping(payload.get("remediation_decision"))
    remediation_result = _mapping(payload.get("remediation_result"))

    incident_id = _text(incident.get("incident_id"), "unknown")
    pipeline_name = _text(incident.get("pipeline_name"), "unknown")
    incident_type = _text(incident.get("incident_type"), "unknown")
    error_summary = _text(incident.get("error_summary"), "unknown")
    source_uri = _text(incident.get("source_uri"), "unknown")
    evidence_action = _text(evidence_decision.get("selected_action"), "unknown")
    remediation_action = _text(remediation_decision.get("selected_action"), "unknown")
    remediation_success = _text(remediation_result.get("success"), "unknown")

    fallback_note = ""
    if error is not None and error.strip() != "":
        fallback_note = (
            "\n\nBedrock report generation failed; this deterministic report was "
            f"generated locally. Error: `{_escape_markdown_inline(error)}`"
        )

    return (
        "# Incident Report\n"
        "## Summary\n"
        f"Incident `{incident_id}` for pipeline `{pipeline_name}` failed with "
        f"incident type `{incident_type}`. Source URI: `{source_uri}`.\n\n"
        "## Root Cause\n"
        f"{error_summary}\n\n"
        "## Evidence\n"
        f"Evidence action `{evidence_action}` completed with success="
        f"`{_text(evidence_result.get('success'), 'unknown')}`.\n\n"
        "## Deterministic Action Taken\n"
        f"Remediation action `{remediation_action}` completed with success="
        f"`{remediation_success}`.\n\n"
        "## Recommended Engineer Actions\n"
        "- Review persisted evidence and tool results in PostgreSQL.\n"
        "- Verify the source batch lineage before rerunning downstream assets.\n"
        "- Confirm whether manual repair, quarantine review, or producer escalation is required."
        f"{fallback_note}"
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _text(value: Any, default: str) -> str:
    if value is None:
        return default
    normalized = str(value).strip()
    return normalized if normalized else default


def _escape_markdown_inline(value: str) -> str:
    return value.replace("`", "'")[:500]
