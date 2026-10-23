"""Allowlisted tool dispatcher with durable result persistence."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any, Literal

from botocore.client import BaseClient

from config.settings import Settings
from dataops_agent.incident_state import (
    ALLOWED_ACTIONS,
    INCIDENT_STATUS_ACTION_FAILED,
    INCIDENT_STATUS_ACTION_SUCCEEDED,
    INCIDENT_STATUS_EVIDENCE_RECORDED,
    INCIDENT_STATUS_FAILED,
    INCIDENT_STATUS_MANUAL_REVIEW,
    DataOpsIncident,
    IncidentRepository,
)
from dataops_agent.tools import alert_engineer, inspect_schema, profile_nulls
from dataops_agent.tools import quarantine_batch, retry_pipeline
from dataops_agent.tools.base import (
    ToolResult,
    failure_result,
    require_context,
    require_non_empty,
)


ToolPhase = Literal["evidence", "remediation"]
ToolCallable = Callable[..., ToolResult]

LOGGER = logging.getLogger(__name__)

TOOL_REGISTRY: dict[str, ToolCallable] = {
    "inspect_schema": inspect_schema.execute,
    "profile_nulls": profile_nulls.execute,
    "quarantine_batch": quarantine_batch.execute,
    "retry_pipeline": retry_pipeline.execute,
    "alert_engineer": alert_engineer.execute,
}


class ToolDispatcher:
    """Validate, execute, log, and persist allowlisted tool actions."""

    def __init__(
        self,
        *,
        repository: IncidentRepository | None = None,
        settings: Settings | None = None,
        s3_client: BaseClient | None = None,
    ) -> None:
        self._repository = repository or IncidentRepository()
        self._settings = settings
        self._s3_client = s3_client

    def dispatch(
        self,
        *,
        selected_action: str,
        incident: DataOpsIncident,
        phase: ToolPhase,
        context: Mapping[str, Any] | None = None,
    ) -> ToolResult:
        """Execute an allowlisted tool and persist its result before status change."""

        action = require_non_empty(selected_action, "selected_action")
        normalized_phase = self._normalize_phase(phase)
        tool_context = require_context(context)
        fallback_from: str | None = None

        if action not in ALLOWED_ACTIONS or action not in TOOL_REGISTRY:
            fallback_from = action
            action = "alert_engineer"
            tool_context = {
                **tool_context,
                "fallback_reason": (
                    f"unapproved action selected by decider: {fallback_from}"
                ),
            }
            LOGGER.warning(
                "unapproved tool action requested; falling back to alert_engineer",
                extra={
                    "incident_id": getattr(incident, "incident_id", None),
                    "selected_action": fallback_from,
                },
            )

        result = self._execute_tool(
            action=action,
            incident=incident,
            context=tool_context,
        )
        if fallback_from is not None:
            result = self._annotate_fallback(result, fallback_from)

        payload = result.repository_payload(phase=normalized_phase, tool_name=action)
        status = self._status_for(
            action=action,
            phase=normalized_phase,
            result=result,
        )

        try:
            self._repository.record_tool_result(
                str(getattr(incident, "incident_id")),
                phase=normalized_phase,
                tool_name=action,
                tool_result=payload,
                status=status,
            )
        except Exception as exc:
            LOGGER.exception(
                "failed to persist tool result",
                extra={
                    "incident_id": getattr(incident, "incident_id", None),
                    "tool_name": action,
                    "phase": normalized_phase,
                },
            )
            return failure_result(
                f"tool result persistence failed: {exc}",
                {
                    "tool": action,
                    "phase": normalized_phase,
                    "execution_error": True,
                    "unpersisted_result": payload,
                },
            )

        LOGGER.info(
            "tool action completed",
            extra={
                "incident_id": getattr(incident, "incident_id", None),
                "tool_name": action,
                "phase": normalized_phase,
                "success": result.success,
                "status": status,
            },
        )
        return result

    def _execute_tool(
        self,
        *,
        action: str,
        incident: DataOpsIncident,
        context: Mapping[str, Any],
    ) -> ToolResult:
        tool = TOOL_REGISTRY[action]
        try:
            result = tool(
                incident,
                context,
                settings=self._settings,
                s3_client=self._s3_client,
                repository=self._repository,
            )
            if not isinstance(result, ToolResult):
                return ToolResult.model_validate(result)
            return result
        except Exception as exc:
            LOGGER.exception(
                "tool execution failed",
                extra={
                    "incident_id": getattr(incident, "incident_id", None),
                    "tool_name": action,
                },
            )
            return failure_result(
                str(exc),
                {
                    "tool": action,
                    "execution_error": True,
                },
            )

    @staticmethod
    def _normalize_phase(phase: str) -> ToolPhase:
        normalized = require_non_empty(phase, "phase")
        if normalized not in {"evidence", "remediation"}:
            raise ValueError("phase must be either evidence or remediation")
        return normalized  # type: ignore[return-value]

    @staticmethod
    def _status_for(
        *,
        action: str,
        phase: ToolPhase,
        result: ToolResult,
    ) -> str:
        if result.data.get("execution_error") is True:
            return INCIDENT_STATUS_FAILED
        if action == "alert_engineer":
            return INCIDENT_STATUS_MANUAL_REVIEW
        if phase == "evidence":
            return INCIDENT_STATUS_EVIDENCE_RECORDED
        if result.success:
            return INCIDENT_STATUS_ACTION_SUCCEEDED
        return INCIDENT_STATUS_ACTION_FAILED

    @staticmethod
    def _annotate_fallback(result: ToolResult, fallback_from: str) -> ToolResult:
        data = {
            **result.data,
            "fallback_from_unapproved_action": fallback_from,
            "fallback_action": "alert_engineer",
        }
        if result.success:
            return ToolResult(success=True, data=data)
        return ToolResult(success=False, data=data, error=result.error)

