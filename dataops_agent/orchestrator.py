"""End-to-end Agentic AWS DataOps incident orchestration."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from dataops_agent.bedrock_report import BedrockReportGenerator, deterministic_report
from dataops_agent.decider_client import DeciderDecision, StrandsDeciderClient
from dataops_agent.dispatcher import ToolDispatcher
from dataops_agent.incident_state import (
    INCIDENT_STATUS_COMPLETED,
    INCIDENT_STATUS_FAILED,
    INCIDENT_STATUS_MANUAL_REVIEW,
    DataOpsIncident,
    IncidentRepository,
)
from dataops_agent.tools.base import ToolResult, ensure_json_object, incident_snapshot


LOGGER = logging.getLogger(__name__)


class OrchestrationResult(BaseModel):
    """Structured result for one orchestration attempt."""

    model_config = ConfigDict(extra="forbid")

    success: bool
    incident_id: str | None = None
    status: str
    evidence_decision: dict[str, Any] | None = None
    remediation_decision: dict[str, Any] | None = None
    evidence_result: dict[str, Any] | None = None
    remediation_result: dict[str, Any] | None = None
    report_generated: bool = False
    error: str | None = None


class DataOpsIncidentOrchestrator:
    """Callable orchestrator for the incident diagnosis lifecycle."""

    def __init__(
        self,
        *,
        repository: IncidentRepository | None = None,
        decider_client: StrandsDeciderClient | None = None,
        dispatcher: ToolDispatcher | None = None,
        report_generator: BedrockReportGenerator | None = None,
    ) -> None:
        self._repository = repository or IncidentRepository()
        self._decider_client = decider_client or StrandsDeciderClient()
        self._dispatcher = dispatcher or ToolDispatcher(repository=self._repository)
        self._report_generator = report_generator or BedrockReportGenerator()

    def __call__(
        self,
        context: Mapping[str, Any] | None = None,
    ) -> OrchestrationResult:
        """Claim and process the next pending incident."""

        return self.run_next_incident(context=context)

    def run_next_incident(
        self,
        context: Mapping[str, Any] | None = None,
    ) -> OrchestrationResult:
        """Claim one pending incident and run the full lifecycle."""

        base_context = ensure_json_object(dict(context or {}), "orchestrator_context")
        incident = self._repository.claim_pending_incident()
        if incident is None:
            return OrchestrationResult(success=True, status="no_pending_incident")
        return self.run_claimed_incident(incident, context=base_context)

    def run_claimed_incident(
        self,
        incident: DataOpsIncident,
        *,
        context: Mapping[str, Any] | None = None,
    ) -> OrchestrationResult:
        """Run the full lifecycle for an already claimed incident."""

        base_context = ensure_json_object(dict(context or {}), "orchestrator_context")
        incident_id = str(incident.incident_id)
        try:
            evidence_decision = self._decider_client.decide_evidence(
                self._incident_context(incident, base_context)
            )
            self._repository.record_decision(
                incident_id,
                selected_action=evidence_decision.selected_action,
                decision_confidence=evidence_decision.confidence,
                phase="evidence",
                reasoning=evidence_decision.reasoning,
            )
            incident = self._fresh_incident(incident_id, fallback=incident)

            evidence_result = self._dispatcher.dispatch(
                selected_action=evidence_decision.selected_action,
                incident=incident,
                phase="evidence",
                context=self._tool_context(
                    base_context,
                    decision=evidence_decision,
                    prior_result=None,
                ),
            )
            incident = self._fresh_incident(incident_id, fallback=incident)

            remediation_context = self._incident_context(
                incident,
                base_context,
                evidence_result=evidence_result,
                evidence_decision=evidence_decision,
            )
            remediation_decision = self._decider_client.decide_remediation(
                remediation_context
            )
            self._repository.record_decision(
                incident_id,
                selected_action=remediation_decision.selected_action,
                decision_confidence=remediation_decision.confidence,
                phase="remediation",
                reasoning=remediation_decision.reasoning,
            )
            incident = self._fresh_incident(incident_id, fallback=incident)

            remediation_result = self._dispatcher.dispatch(
                selected_action=remediation_decision.selected_action,
                incident=incident,
                phase="remediation",
                context=self._tool_context(
                    base_context,
                    decision=remediation_decision,
                    prior_result=evidence_result,
                ),
            )
            incident = self._fresh_incident(incident_id, fallback=incident)

            report = self._report_generator.generate_report(
                incident=incident,
                evidence_decision=evidence_decision,
                evidence_result=evidence_result,
                remediation_decision=remediation_decision,
                remediation_result=remediation_result,
            )
            final_status = self._final_status(remediation_decision, remediation_result)
            self._repository.finalize_incident(
                incident_id,
                action_result=remediation_result.repository_payload(
                    phase="remediation",
                    tool_name=remediation_decision.selected_action,
                ),
                incident_report=report,
                status=final_status,
            )
            return OrchestrationResult(
                success=final_status != INCIDENT_STATUS_FAILED,
                incident_id=incident_id,
                status=final_status,
                evidence_decision=evidence_decision.repository_metadata(),
                remediation_decision=remediation_decision.repository_metadata(),
                evidence_result=evidence_result.model_dump(mode="json"),
                remediation_result=remediation_result.model_dump(mode="json"),
                report_generated=True,
            )
        except Exception as exc:
            LOGGER.exception(
                "incident orchestration failed",
                extra={"incident_id": incident_id},
            )
            return self._fail_incident(incident, error=exc)

    @staticmethod
    def _incident_context(
        incident: DataOpsIncident,
        base_context: Mapping[str, Any],
        *,
        evidence_result: ToolResult | None = None,
        evidence_decision: DeciderDecision | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "incident": incident_snapshot(incident),
            "evidence_json": getattr(incident, "evidence_json", {}) or {},
            "action_result": getattr(incident, "action_result", None),
            "runtime_context": dict(base_context),
        }
        if evidence_result is not None:
            payload["evidence_result"] = evidence_result.model_dump(mode="json")
        if evidence_decision is not None:
            payload["evidence_decision"] = evidence_decision.repository_metadata()
        return ensure_json_object(payload, "incident_context")

    @staticmethod
    def _tool_context(
        base_context: Mapping[str, Any],
        *,
        decision: DeciderDecision,
        prior_result: ToolResult | None,
    ) -> dict[str, Any]:
        context = dict(base_context)
        context["decision"] = decision.repository_metadata()
        if prior_result is not None:
            context["prior_tool_result"] = prior_result.model_dump(mode="json")
        return ensure_json_object(context, "tool_context")

    def _fresh_incident(
        self,
        incident_id: str,
        *,
        fallback: DataOpsIncident,
    ) -> DataOpsIncident:
        fresh = self._repository.get_incident(incident_id)
        return fresh or fallback

    @staticmethod
    def _final_status(
        remediation_decision: DeciderDecision,
        remediation_result: ToolResult,
    ) -> str:
        if remediation_decision.selected_action == "alert_engineer":
            return INCIDENT_STATUS_MANUAL_REVIEW
        if remediation_result.data.get("requires_human_review") is True:
            return INCIDENT_STATUS_MANUAL_REVIEW
        if remediation_result.success:
            return INCIDENT_STATUS_COMPLETED
        return INCIDENT_STATUS_FAILED

    def _fail_incident(
        self,
        incident: DataOpsIncident,
        *,
        error: Exception,
    ) -> OrchestrationResult:
        incident_id = str(getattr(incident, "incident_id", "unknown"))
        error_message = str(error).strip() or type(error).__name__
        failure_result = ToolResult(
            success=False,
            data={
                "tool": "orchestrator",
                "execution_error": True,
                "error_type": type(error).__name__,
            },
            error=error_message,
        )
        fallback_decision = DeciderDecision(
            phase="remediation",
            selected_action="alert_engineer",
            confidence=0.0,
            reasoning=f"Orchestration failed: {error_message}",
            fallback=True,
        )
        report = deterministic_report(
            {
                "incident": incident_snapshot(incident),
                "evidence_decision": fallback_decision.repository_metadata(),
                "evidence_result": failure_result.model_dump(mode="json"),
                "remediation_decision": fallback_decision.repository_metadata(),
                "remediation_result": failure_result.model_dump(mode="json"),
            },
            error=error_message,
        )
        try:
            self._repository.finalize_incident(
                incident_id,
                action_result=failure_result.repository_payload(
                    phase="remediation",
                    tool_name="orchestrator",
                ),
                incident_report=report,
                status=INCIDENT_STATUS_FAILED,
            )
        except Exception as finalize_error:
            return OrchestrationResult(
                success=False,
                incident_id=incident_id,
                status="finalize_failed",
                report_generated=True,
                error=f"{error_message}; finalize failed: {finalize_error}",
            )
        return OrchestrationResult(
            success=False,
            incident_id=incident_id,
            status=INCIDENT_STATUS_FAILED,
            remediation_decision=fallback_decision.repository_metadata(),
            remediation_result=failure_result.model_dump(mode="json"),
            report_generated=True,
            error=error_message,
        )
