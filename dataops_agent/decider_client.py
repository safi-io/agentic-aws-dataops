"""Resilient Strands Decider client."""

from __future__ import annotations

import json
import logging
import re
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import Settings, get_settings
from dataops_agent.incident_state import ALLOWED_ACTIONS
from dataops_agent.tools.base import ensure_json_object, require_non_empty


DecisionPhase = Literal["evidence", "remediation"]
ORDERED_ALLOWED_ACTIONS: tuple[str, ...] = (
    "inspect_schema",
    "profile_nulls",
    "quarantine_batch",
    "retry_pipeline",
    "alert_engineer",
)

LOGGER = logging.getLogger(__name__)


class DeciderContractError(RuntimeError):
    """Raised when the decider HTTP contract is violated."""


class DeciderHttpError(RuntimeError):
    """Raised for retryable decider transport or status failures."""


class DeciderRequest(BaseModel):
    """Strict request payload for Strands Decider."""

    model_config = ConfigDict(extra="forbid")

    phase: DecisionPhase
    incident_context: dict[str, Any]
    allowed_actions: list[str]

    @field_validator("incident_context")
    @classmethod
    def _validate_incident_context(cls, value: dict[str, Any]) -> dict[str, Any]:
        return ensure_json_object(value, "incident_context")

    @field_validator("allowed_actions")
    @classmethod
    def _validate_allowed_actions(cls, value: list[str]) -> list[str]:
        if value != list(ORDERED_ALLOWED_ACTIONS):
            raise ValueError("allowed_actions must match the strict DataOps allowlist")
        if any(action not in ALLOWED_ACTIONS for action in value):
            raise ValueError("allowed_actions contains an unapproved action")
        return value


class DeciderHttpResponse(BaseModel):
    """Strict response payload returned by Strands Decider."""

    model_config = ConfigDict(extra="forbid")

    selected_action: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(min_length=1)

    @field_validator("selected_action")
    @classmethod
    def _validate_selected_action(cls, value: str) -> str:
        selected = value.strip()
        if selected not in ALLOWED_ACTIONS:
            raise ValueError(f"selected_action is not allowlisted: {selected}")
        return selected

    @field_validator("reasoning")
    @classmethod
    def _normalize_reasoning(cls, value: str) -> str:
        normalized = value.strip()
        if normalized == "":
            raise ValueError("reasoning cannot be empty")
        return normalized


class DeciderDecision(BaseModel):
    """Validated decision consumed by the orchestrator."""

    model_config = ConfigDict(extra="forbid")

    phase: DecisionPhase
    selected_action: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    fallback: bool = False
    probabilities: dict[str, float] | None = None

    @model_validator(mode="after")
    def _validate_action_contract(self) -> "DeciderDecision":
        if self.selected_action not in ALLOWED_ACTIONS:
            raise ValueError(f"selected_action is not allowlisted: {self.selected_action}")
        object.__setattr__(
            self,
            "reasoning",
            require_non_empty(self.reasoning, "reasoning"),
        )
        return self

    def repository_metadata(self) -> dict[str, Any]:
        """Return JSON-safe decision metadata for durable state."""

        return ensure_json_object(
            {
                "phase": self.phase,
                "selected_action": self.selected_action,
                "confidence": self.confidence,
                "reasoning": self.reasoning,
                "fallback": self.fallback,
                "probabilities": self.probabilities,
            },
            "decision",
        )


class StrandsDeciderClient:
    """Client for the two-phase Strands Decider contract."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        endpoint: str | None = None,
        api_key: str | None = None,
        timeout_seconds: float | None = None,
        allowed_actions: Sequence[str] = ORDERED_ALLOWED_ACTIONS,
    ) -> None:
        resolved_settings = settings
        resolved_settings = resolved_settings or get_settings()
        resolved_timeout = (
            resolved_settings.DECIDER_TIMEOUT_SECONDS
            if timeout_seconds is None
            else timeout_seconds
        )
        if resolved_timeout <= 0:
            raise ValueError("timeout_seconds must be greater than 0")
        self._mode = resolved_settings.DECIDER_MODE

        raw_endpoint = endpoint or resolved_settings.STRANDS_DECIDER_ENDPOINT
        raw_api_key = api_key
        if raw_api_key is None and resolved_settings.STRANDS_DECIDER_API_KEY is not None:
            raw_api_key = resolved_settings.STRANDS_DECIDER_API_KEY.get_secret_value()

        self._endpoint = raw_endpoint.rstrip("/") if raw_endpoint is not None else None
        self._api_key = raw_api_key
        if self._mode == "http":
            self._endpoint = require_non_empty(
                self._endpoint,
                "STRANDS_DECIDER_ENDPOINT",
            ).rstrip("/")
            self._api_key = require_non_empty(
                self._api_key,
                "STRANDS_DECIDER_API_KEY",
            )
        self._model_id = resolved_settings.STRANDS_DECIDER_MODEL_ID
        self._timeout_seconds = resolved_timeout
        self._allowed_actions = list(allowed_actions)

    def decide_evidence(
        self,
        incident_context: Mapping[str, Any],
    ) -> DeciderDecision:
        """Select the evidence-gathering action."""

        return self.decide(phase="evidence", incident_context=incident_context)

    def decide_remediation(
        self,
        incident_context: Mapping[str, Any],
    ) -> DeciderDecision:
        """Select the remediation action."""

        return self.decide(phase="remediation", incident_context=incident_context)

    def decide(
        self,
        *,
        phase: DecisionPhase,
        incident_context: Mapping[str, Any],
    ) -> DeciderDecision:
        """Call Strands Decider and return a validated decision or hard fallback."""

        try:
            payload = DeciderRequest(
                phase=phase,
                incident_context=dict(incident_context),
                allowed_actions=list(self._allowed_actions),
            )
            if self._mode == "cli":
                parsed_response = self._ask_cli(payload)
            else:
                raw_response = self._post_payload(payload.model_dump(mode="json"))
                parsed_response = self._parse_response(raw_response)
            return DeciderDecision(
                phase=phase,
                selected_action=parsed_response.selected_action,
                confidence=parsed_response.confidence,
                reasoning=parsed_response.reasoning,
                probabilities=getattr(parsed_response, "probabilities", None),
                fallback=False,
            )
        except Exception as exc:
            LOGGER.warning(
                "decider call failed; falling back to alert_engineer: %s",
                exc,
                extra={"phase": phase, "error_type": type(exc).__name__},
            )
            return self._fallback_decision(phase=phase, reason=str(exc))

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, DeciderHttpError)),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _post_payload(self, payload: Mapping[str, Any]) -> httpx.Response:
        if self._endpoint is None or self._api_key is None:
            raise DeciderContractError("HTTP decider endpoint or API key is not configured")
        url = f"{self._endpoint}/v1/decide"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self._timeout_seconds) as client:
            response = client.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            raise DeciderHttpError(
                f"decider returned HTTP {response.status_code}"
            )
        return response

    def _ask_cli(self, payload: DeciderRequest) -> "DeciderCliResponse":
        state = json.dumps(_compact_cli_state(payload), sort_keys=True)
        question = _question_for_phase(payload.phase)
        choices = ",".join(payload.allowed_actions)
        choice_arg = f"{question}={choices}"
        completed = subprocess.run(
            [
                "strands-decider",
                "ask",
                self._model_id,
                "--state",
                state,
                "--choice",
                choice_arg,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=self._timeout_seconds,
        )
        output = "\n".join(
            part.strip()
            for part in (completed.stdout, completed.stderr)
            if part and part.strip()
        )
        if completed.returncode != 0:
            raise DeciderHttpError(
                f"strands-decider CLI exited {completed.returncode}: {output[:500]}"
            )
        return DeciderCliResponse.from_output(output)

    @staticmethod
    def _parse_response(response: httpx.Response) -> DeciderHttpResponse:
        try:
            payload = response.json()
        except ValueError as exc:
            raise DeciderContractError("decider returned unparseable JSON") from exc
        try:
            return DeciderHttpResponse.model_validate(payload)
        except Exception as exc:
            raise DeciderContractError("decider response failed validation") from exc

    @staticmethod
    def _fallback_decision(*, phase: DecisionPhase, reason: str) -> DeciderDecision:
        clean_reason = reason.strip() if reason.strip() else "decider unavailable"
        return DeciderDecision(
            phase=phase,
            selected_action="alert_engineer",
            confidence=0.0,
            reasoning=f"Deterministic fallback selected because decider failed: {clean_reason}",
            fallback=True,
        )


class DeciderCliResponse(BaseModel):
    """Parsed response returned by the local strands-decider CLI."""

    model_config = ConfigDict(extra="forbid")

    selected_action: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str
    probabilities: dict[str, float] | None = None

    @classmethod
    def from_output(cls, output: str) -> "DeciderCliResponse":
        selected_match = re.search(
            r"choice_\d+\s*->\s*(?P<action>[A-Za-z0-9_=-]+)\s*"
            r"\(confidence\s+(?P<confidence>[0-9]*\.?[0-9]+)\)",
            output,
        )
        if selected_match is None:
            raise DeciderContractError("strands-decider CLI output did not include a choice")

        selected_action = selected_match.group("action").strip()
        if selected_action not in ALLOWED_ACTIONS:
            raise DeciderContractError(
                f"strands-decider selected an unapproved action: {selected_action}"
            )
        confidence = float(selected_match.group("confidence"))
        probabilities = _parse_probabilities(output)
        return cls(
            selected_action=selected_action,
            confidence=confidence,
            reasoning="Selected by local strands-decider CLI.",
            probabilities=probabilities or None,
        )


def _question_for_phase(phase: DecisionPhase) -> str:
    if phase == "evidence":
        return "Which evidence tool should inspect this incident first?"
    return "Which remediation action should handle this incident?"


def _compact_cli_state(payload: DeciderRequest) -> dict[str, Any]:
    context = payload.incident_context
    incident = _mapping(context.get("incident"))
    evidence = _mapping(context.get("evidence_json"))
    validation = _mapping(evidence.get("validation"))
    evidence_result = _tool_result_summary(context.get("evidence_result"))
    action_result = _mapping(context.get("action_result"))

    return ensure_json_object(
        {
            "phase": payload.phase,
            "incident_type": incident.get("incident_type"),
            "error_summary": incident.get("error_summary"),
            "severity": incident.get("severity"),
            "source_uri": incident.get("source_uri"),
            "schema_comparison": (
                _mapping(validation.get("schema_comparison"))
                or _mapping(evidence.get("schema_comparison"))
            ),
            "target_schema": evidence.get("target_schema") or validation.get("target_schema"),
            "observed_schema": (
                evidence.get("observed_schema")
                or validation.get("observed_schema")
            ),
            "threshold_breaches": validation.get("threshold_breaches", []),
            "validation_errors": validation.get("errors", []),
            "prior_evidence_tool": evidence_result,
            "prior_action_result": action_result or None,
            "available_actions": list(payload.allowed_actions),
            "tool_guidance": {
                "inspect_schema": (
                    "Choose for missing columns, unexpected extra columns, "
                    "or column type drift."
                ),
                "profile_nulls": "Choose for null threshold breaches or missing-value spikes.",
                "quarantine_batch": (
                    "Choose when a bad batch should be isolated after the "
                    "problem is confirmed."
                ),
                "retry_pipeline": (
                    "Choose only for transient infrastructure, network, or "
                    "runtime failures."
                ),
                "alert_engineer": (
                    "Choose when automated tools cannot safely investigate "
                    "or human approval is required."
                ),
            },
        },
        "decider_cli_state",
    )


def _tool_result_summary(value: Any) -> dict[str, Any] | None:
    result = _mapping(value)
    if not result:
        return None
    data = _mapping(result.get("data"))
    summary: dict[str, Any] = {
        "success": result.get("success"),
        "error": result.get("error"),
    }
    for key in (
        "tool",
        "schema_comparison",
        "expected_schema",
        "observed_schema",
        "null_profile",
        "threshold_breaches",
        "retry_recommended",
        "quarantine_uri",
    ):
        if key in data:
            summary[key] = data[key]
    return ensure_json_object(summary, "tool_result_summary")


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _parse_probabilities(output: str) -> dict[str, float]:
    probabilities: dict[str, float] = {}
    for line in output.splitlines():
        match = re.match(r"^\s*(?P<action>[A-Za-z0-9_=-]+)\s+(?P<prob>[0-9]*\.?[0-9]+)\s*$", line)
        if match is None:
            continue
        action = match.group("action").strip()
        if action in ALLOWED_ACTIONS:
            probabilities[action] = float(match.group("prob"))
    return probabilities
