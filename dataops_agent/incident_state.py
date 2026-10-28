"""PostgreSQL-backed incident state management for Agentic AWS DataOps."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Final, Iterator

from sqlalchemy import Engine, Select, Text, create_engine, func, select, text
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB, TIMESTAMP, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


INCIDENT_STATUS_PENDING: Final[str] = "pending"
INCIDENT_STATUS_INVESTIGATING: Final[str] = "investigating"
INCIDENT_STATUS_EVIDENCE_RECORDED: Final[str] = "evidence_recorded"
INCIDENT_STATUS_DECISION_RECORDED: Final[str] = "decision_recorded"
INCIDENT_STATUS_ACTION_SUCCEEDED: Final[str] = "action_succeeded"
INCIDENT_STATUS_ACTION_FAILED: Final[str] = "action_failed"
INCIDENT_STATUS_COMPLETED: Final[str] = "completed"
INCIDENT_STATUS_FAILED: Final[str] = "failed"
INCIDENT_STATUS_MANUAL_REVIEW: Final[str] = "manual_review"

ALLOWED_ACTIONS: Final[frozenset[str]] = frozenset(
    {
        "inspect_schema",
        "profile_nulls",
        "quarantine_batch",
        "retry_pipeline",
        "alert_engineer",
    }
)


class IncidentRepositoryError(RuntimeError):
    """Base exception for durable incident state failures."""


class IncidentNotFoundError(IncidentRepositoryError):
    """Raised when an incident lookup cannot find the requested row."""


class IncidentConflictError(IncidentRepositoryError):
    """Raised when a requested mutation conflicts with existing incident state."""


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for application tables."""


class DataOpsIncident(Base):
    """ORM mapping for the dataops_incidents table."""

    __tablename__ = "dataops_incidents"

    incident_id: Mapped[str] = mapped_column(Text, primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(Text, nullable=False)
    dagster_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    incident_type: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        server_default=text("'{}'::jsonb"),
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        default=INCIDENT_STATUS_PENDING,
        server_default=text("'pending'"),
    )
    selected_action: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision_confidence: Mapped[float | None] = mapped_column(
        DOUBLE_PRECISION,
        nullable=True,
    )
    action_result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    incident_report: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    def __repr__(self) -> str:
        return (
            "DataOpsIncident("
            f"incident_id={self.incident_id!r}, "
            f"pipeline_name={self.pipeline_name!r}, "
            f"incident_type={self.incident_type!r}, "
            f"status={self.status!r})"
        )


class IncidentRepository:
    """Durable repository for incident lifecycle state."""

    def __init__(
        self,
        database_url: str | None = None,
        *,
        engine: Engine | None = None,
        session_factory: sessionmaker[Session] | None = None,
    ) -> None:
        if session_factory is not None and (engine is not None or database_url is not None):
            raise ValueError(
                "Provide either session_factory, engine, or database_url, not multiple"
            )

        if session_factory is not None:
            self._session_factory = session_factory
            return

        if engine is None:
            if database_url is None:
                from config.settings import get_settings

                database_url = get_settings().DATABASE_URL
            engine = create_engine(
                database_url,
                pool_pre_ping=True,
                pool_recycle=1800,
                connect_args={"connect_timeout": 5},
            )

        self._session_factory = sessionmaker(
            bind=engine,
            autoflush=False,
            expire_on_commit=False,
        )

    @contextmanager
    def _session_scope(self) -> Iterator[Session]:
        session = self._session_factory()
        try:
            with session.begin():
                yield session
        except SQLAlchemyError as exc:
            raise IncidentRepositoryError("database operation failed") from exc
        finally:
            session.close()

    def create_incident(
        self,
        *,
        incident_id: str,
        pipeline_name: str,
        incident_type: str,
        dagster_run_id: str | None = None,
        source_uri: str | None = None,
        severity: str | None = None,
        error_summary: str | None = None,
        evidence_json: Mapping[str, Any] | None = None,
    ) -> DataOpsIncident:
        """Create an incident once and return the durable row.

        Duplicate calls with the same immutable incident fields are treated as
        idempotent and return the existing row without changing its state.
        """

        incident_id = _require_non_empty(incident_id, "incident_id")
        pipeline_name = _require_non_empty(pipeline_name, "pipeline_name")
        incident_type = _require_non_empty(incident_type, "incident_type")
        normalized_evidence = _validate_json_object(evidence_json or {}, "evidence_json")

        values = {
            "incident_id": incident_id,
            "pipeline_name": pipeline_name,
            "dagster_run_id": _normalize_optional_text(dagster_run_id),
            "source_uri": _normalize_optional_text(source_uri),
            "incident_type": incident_type,
            "severity": _normalize_optional_text(severity),
            "error_summary": _normalize_optional_text(error_summary),
            "evidence_json": normalized_evidence,
            "status": INCIDENT_STATUS_PENDING,
        }

        with self._session_scope() as session:
            stmt = (
                insert(DataOpsIncident)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[DataOpsIncident.incident_id])
                .returning(DataOpsIncident)
            )
            inserted = session.scalars(stmt).one_or_none()
            if inserted is not None:
                session.flush()
                session.refresh(inserted)
                return inserted

            existing = self._get_required(session, incident_id)
            self._validate_idempotent_create(existing, values)
            return existing

    def claim_pending_incident(self) -> DataOpsIncident | None:
        """Atomically claim one pending incident using FOR UPDATE SKIP LOCKED."""

        with self._session_scope() as session:
            stmt: Select[tuple[DataOpsIncident]] = (
                select(DataOpsIncident)
                .where(DataOpsIncident.status == INCIDENT_STATUS_PENDING)
                .order_by(DataOpsIncident.created_at.asc(), DataOpsIncident.incident_id.asc())
                .with_for_update(of=DataOpsIncident, skip_locked=True)
                .limit(1)
            )
            incident = session.execute(stmt).scalars().first()
            if incident is None:
                return None

            incident.status = INCIDENT_STATUS_INVESTIGATING
            session.flush()
            session.refresh(incident)
            return incident

    def record_evidence(
        self,
        incident_id: str,
        evidence_json: Mapping[str, Any],
        *,
        status: str = INCIDENT_STATUS_EVIDENCE_RECORDED,
    ) -> DataOpsIncident:
        """Persist evidence collected for an incident."""

        incident_id = _require_non_empty(incident_id, "incident_id")
        normalized_evidence = _validate_json_object(evidence_json, "evidence_json")
        status = _require_non_empty(status, "status")

        with self._session_scope() as session:
            incident = self._get_required(session, incident_id)
            incident.evidence_json = normalized_evidence
            incident.status = status
            session.flush()
            session.refresh(incident)
            return incident

    def record_decision(
        self,
        incident_id: str,
        *,
        selected_action: str,
        decision_confidence: float,
        phase: str | None = None,
        reasoning: str | None = None,
        status: str = INCIDENT_STATUS_DECISION_RECORDED,
    ) -> DataOpsIncident:
        """Persist the selected action and confidence from the decider."""

        incident_id = _require_non_empty(incident_id, "incident_id")
        selected_action = _require_non_empty(selected_action, "selected_action")
        if selected_action not in ALLOWED_ACTIONS:
            raise ValueError(f"selected_action is not allowlisted: {selected_action}")
        confidence = _validate_confidence(decision_confidence)
        normalized_phase = _normalize_optional_text(phase)
        normalized_reasoning = _normalize_optional_text(reasoning)
        status = _require_non_empty(status, "status")

        with self._session_scope() as session:
            incident = self._get_required(session, incident_id)
            evidence = _validate_json_object(
                incident.evidence_json or {},
                "existing evidence_json",
            )
            if normalized_phase is not None:
                raw_decisions = evidence.get("decisions", {})
                if not isinstance(raw_decisions, Mapping):
                    raw_decisions = {}
                decisions = _validate_json_object(
                    raw_decisions,
                    "existing evidence_json.decisions",
                )
                decisions[normalized_phase] = _validate_json_object(
                    {
                        "selected_action": selected_action,
                        "confidence": confidence,
                        "reasoning": normalized_reasoning,
                    },
                    f"decision.{normalized_phase}",
                )
                evidence["decisions"] = decisions
                incident.evidence_json = evidence

            incident.selected_action = selected_action
            incident.decision_confidence = confidence
            incident.status = status
            session.flush()
            session.refresh(incident)
            return incident

    def finalize_incident(
        self,
        incident_id: str,
        *,
        action_result: Mapping[str, Any],
        incident_report: str | None,
        status: str = INCIDENT_STATUS_COMPLETED,
    ) -> DataOpsIncident:
        """Finalize an incident with the action result and report text."""

        incident_id = _require_non_empty(incident_id, "incident_id")
        normalized_action_result = _validate_json_object(action_result, "action_result")
        normalized_report = _normalize_optional_text(incident_report)
        status = _require_non_empty(status, "status")

        with self._session_scope() as session:
            incident = self._get_required(session, incident_id)
            incident.action_result = normalized_action_result
            incident.incident_report = normalized_report
            incident.status = status
            session.flush()
            session.refresh(incident)
            return incident

    def record_tool_result(
        self,
        incident_id: str,
        *,
        phase: str,
        tool_name: str,
        tool_result: Mapping[str, Any],
        status: str,
    ) -> DataOpsIncident:
        """Persist a tool result and update incident status atomically."""

        incident_id = _require_non_empty(incident_id, "incident_id")
        phase = _require_non_empty(phase, "phase")
        tool_name = _require_non_empty(tool_name, "tool_name")
        status = _require_non_empty(status, "status")
        normalized_result = _validate_json_object(tool_result, "tool_result")

        with self._session_scope() as session:
            incident = self._get_required(session, incident_id)
            evidence = _validate_json_object(
                incident.evidence_json or {},
                "existing evidence_json",
            )
            raw_tool_results = evidence.get("tool_results", {})
            if not isinstance(raw_tool_results, Mapping):
                raw_tool_results = {}
            tool_results = _validate_json_object(
                raw_tool_results,
                "existing evidence_json.tool_results",
            )
            tool_results[f"{phase}.{tool_name}"] = normalized_result
            evidence["tool_results"] = tool_results
            evidence["last_tool_result"] = normalized_result

            incident.evidence_json = evidence
            if phase == "remediation":
                incident.action_result = normalized_result
            incident.status = status
            session.flush()
            session.refresh(incident)
            return incident

    def get_incident(self, incident_id: str) -> DataOpsIncident | None:
        """Return one incident by ID, or None when it does not exist."""

        incident_id = _require_non_empty(incident_id, "incident_id")
        with self._session_scope() as session:
            return session.get(DataOpsIncident, incident_id)

    def list_incidents(
        self,
        *,
        status: str | Sequence[str] | None = None,
        pipeline_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[DataOpsIncident]:
        """List incidents with stable ordering and bounded pagination."""

        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if offset < 0:
            raise ValueError("offset must be greater than or equal to 0")

        stmt: Select[tuple[DataOpsIncident]] = select(DataOpsIncident)
        if status is not None:
            if isinstance(status, str):
                stmt = stmt.where(DataOpsIncident.status == _require_non_empty(status, "status"))
            else:
                statuses = [_require_non_empty(item, "status") for item in status]
                if len(statuses) == 0:
                    raise ValueError("status sequence cannot be empty")
                stmt = stmt.where(DataOpsIncident.status.in_(statuses))
        if pipeline_name is not None:
            stmt = stmt.where(
                DataOpsIncident.pipeline_name
                == _require_non_empty(pipeline_name, "pipeline_name")
            )

        stmt = (
            stmt.order_by(DataOpsIncident.created_at.desc(), DataOpsIncident.incident_id.asc())
            .limit(limit)
            .offset(offset)
        )

        with self._session_scope() as session:
            return list(session.execute(stmt).scalars().all())

    def _get_required(self, session: Session, incident_id: str) -> DataOpsIncident:
        incident = session.get(DataOpsIncident, incident_id)
        if incident is None:
            raise IncidentNotFoundError(f"incident not found: {incident_id}")
        return incident

    @staticmethod
    def _validate_idempotent_create(
        existing: DataOpsIncident,
        requested_values: Mapping[str, Any],
    ) -> None:
        immutable_fields = (
            "pipeline_name",
            "dagster_run_id",
            "source_uri",
            "incident_type",
            "severity",
            "error_summary",
        )
        mismatches = [
            field
            for field in immutable_fields
            if getattr(existing, field) != requested_values[field]
        ]
        if existing.evidence_json != requested_values["evidence_json"]:
            mismatches.append("evidence_json")
        if mismatches:
            raise IncidentConflictError(
                "incident_id already exists with different immutable fields: "
                + ", ".join(mismatches)
            )


def _require_non_empty(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if normalized == "":
        raise ValueError(f"{field_name} cannot be empty")
    return normalized


def _normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _require_non_empty(value, "optional text field")
    return normalized


def _validate_confidence(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("decision_confidence must be a numeric value")
    confidence = float(value)
    if not math.isfinite(confidence):
        raise ValueError("decision_confidence must be finite")
    if confidence < 0.0 or confidence > 1.0:
        raise ValueError("decision_confidence must be between 0.0 and 1.0")
    return confidence


def _validate_json_object(
    value: Mapping[str, Any],
    field_name: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a JSON object")
    return _validate_json_value(dict(value), field_name)


def _validate_json_value(value: Any, path: str) -> Any:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, nested_value in value.items():
            if not isinstance(key, str) or key.strip() == "":
                raise ValueError(f"{path} contains a non-string or empty object key")
            normalized[key] = _validate_json_value(nested_value, f"{path}.{key}")
        return normalized
    if isinstance(value, list | tuple):
        return [
            _validate_json_value(item, f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise TypeError(f"{path} contains a value that is not JSON serializable")
