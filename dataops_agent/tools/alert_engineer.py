"""Structured engineer alert tool."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from botocore.client import BaseClient

from config.settings import Settings, get_boto3_client, get_settings
from dataops_agent.tools.base import ToolResult, incident_snapshot, require_context


def execute(
    incident: Any,
    context: Mapping[str, Any] | None = None,
    *,
    settings: Settings | None = None,
    s3_client: BaseClient | None = None,
    repository: Any | None = None,
) -> ToolResult:
    """Generate a structured audit and notification payload for manual review."""

    del s3_client, repository
    tool_context = require_context(context)
    resolved_settings = settings or get_settings()
    snapshot = incident_snapshot(incident)
    severity = str(snapshot.get("severity") or "unknown").lower()
    pipeline_name = str(snapshot.get("pipeline_name") or "unknown-pipeline")
    incident_id = str(snapshot.get("incident_id") or "unknown-incident")
    reason = str(
        tool_context.get("reason")
        or tool_context.get("fallback_reason")
        or snapshot.get("error_summary")
        or "manual engineering review required"
    )
    routing_key = str(tool_context.get("routing_key") or "dataops-oncall")

    notification_payload = {
        "summary": snapshot.get("error_summary") or reason,
        "source_uri": snapshot.get("source_uri"),
        "incident_type": snapshot.get("incident_type"),
        "dagster_run_id": snapshot.get("dagster_run_id"),
        "selected_action": snapshot.get("selected_action") or "alert_engineer",
    }
    data = {
        "tool": "alert_engineer",
        "notification_type": "dataops_incident_manual_review",
        "routing_key": routing_key,
        "severity": severity,
        "title": f"[{severity}] {pipeline_name} incident {incident_id}",
        "reason": reason,
        "requires_human_review": True,
        "incident": snapshot,
        "notification_payload": notification_payload,
        "recommended_actions": [
            "Review the incident evidence and source batch lineage.",
            "Confirm whether the batch should be quarantined, retried, or manually repaired.",
            "Record the final operator decision in the incident dashboard.",
        ],
    }
    if resolved_settings.ALERT_SNS_TOPIC_ARN is not None:
        try:
            sns_client = get_boto3_client("sns", settings=resolved_settings)
            response = sns_client.publish(
                TopicArn=resolved_settings.ALERT_SNS_TOPIC_ARN,
                Subject=_sns_subject(severity, pipeline_name, incident_id),
                Message=json.dumps(data, sort_keys=True, indent=2),
            )
            data["alert_delivery"] = {
                "channel": "sns",
                "topic_arn": resolved_settings.ALERT_SNS_TOPIC_ARN,
                "message_id": response.get("MessageId"),
                "status": "published",
            }
        except Exception as exc:
            data["alert_delivery"] = {
                "channel": "sns",
                "topic_arn": resolved_settings.ALERT_SNS_TOPIC_ARN,
                "status": "failed",
                "error": str(exc),
            }
            return ToolResult(
                success=False,
                data=data,
                error=f"SNS alert publish failed: {exc}",
            )
    return ToolResult(success=True, data=data)


def _sns_subject(severity: str, pipeline_name: str, incident_id: str) -> str:
    subject = f"[{severity}] {pipeline_name} incident {incident_id}"
    return subject[:100]
