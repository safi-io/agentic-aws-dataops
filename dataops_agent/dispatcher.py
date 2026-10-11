"""Python tool dispatcher with safety checks and policy enforcement."""

from typing import Any, Dict, Optional
from .policies import PolicyEngine, ALLOWLISTED_ACTIONS
from .tools import (
    inspect_schema,
    profile_nulls,
    quarantine_batch,
    retry_pipeline,
    alert_engineer,
)


class ToolDispatcher:
    """Dispatches validated actions to allowable Python functions."""

    def __init__(self, policy_engine: Optional[PolicyEngine] = None):
        self.policy_engine = policy_engine or PolicyEngine()
        self.tool_registry = {
            "inspect_schema": self._exec_inspect_schema,
            "profile_nulls": self._exec_profile_nulls,
            "quarantine_batch": self._exec_quarantine,
            "retry_pipeline": self._exec_retry,
            "alert_engineer": self._exec_alert,
        }

    def dispatch(
        self,
        action: str,
        incident_type: str,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validates action against policy engine and executes allowable tool."""
        # 1. Policy check
        is_allowed, approved_action, reason = self.policy_engine.validate_action(
            action=action,
            incident_type=incident_type,
            evidence=params
        )

        if not is_allowed:
            # Policy overrode action
            action_to_run = approved_action
            override_note = f"Policy override: {reason}"
        else:
            action_to_run = approved_action
            override_note = None

        tool_fn = self.tool_registry.get(action_to_run)
        if not tool_fn:
            return {
                "status": "failed",
                "error": f"Tool '{action_to_run}' has no implementation.",
                "policy_override": override_note,
            }

        # 2. Tool execution
        try:
            result = tool_fn(params)
            if override_note:
                result["policy_override"] = override_note
            return result
        except Exception as e:
            return {
                "status": "failed",
                "error": f"Execution error in tool {action_to_run}: {str(e)}",
                "policy_override": override_note,
            }

    def _exec_inspect_schema(self, params: Dict[str, Any]) -> Dict[str, Any]:
        file_path = params.get("source_uri") or params.get("file_path")
        contract = params.get("contract")
        return inspect_schema(file_path=file_path, contract=contract)

    def _exec_profile_nulls(self, params: Dict[str, Any]) -> Dict[str, Any]:
        file_path = params.get("source_uri") or params.get("file_path")
        threshold = params.get("null_threshold")
        return profile_nulls(file_path=file_path, threshold=threshold)

    def _exec_quarantine(self, params: Dict[str, Any]) -> Dict[str, Any]:
        source_path = params.get("source_uri") or params.get("file_path")
        quarantine_dir = params.get("quarantine_dir")
        incident_id = params.get("incident_id")
        return quarantine_batch(source_path=source_path, quarantine_dir=quarantine_dir, incident_id=incident_id)

    def _exec_retry(self, params: Dict[str, Any]) -> Dict[str, Any]:
        pipeline_name = params.get("pipeline_name", "orders_pipeline")
        incident_id = params.get("incident_id", "unknown")
        retry_count = params.get("retry_count", 0)
        return retry_pipeline(pipeline_name=pipeline_name, incident_id=incident_id, current_retry_count=retry_count)

    def _exec_alert(self, params: Dict[str, Any]) -> Dict[str, Any]:
        incident_id = params.get("incident_id", "unknown")
        pipeline_name = params.get("pipeline_name", "orders_pipeline")
        reason = params.get("reason") or params.get("error_summary", "Incident requires manual investigation")
        severity = params.get("severity", "high")
        return alert_engineer(incident_id=incident_id, pipeline_name=pipeline_name, reason=reason, severity=severity)

