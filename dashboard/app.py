"""Streamlit monitoring dashboard for Agentic AWS DataOps incidents."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import streamlit as st
from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Settings, get_settings
from dataops_agent.incident_state import (
    INCIDENT_STATUS_COMPLETED,
    INCIDENT_STATUS_FAILED,
    INCIDENT_STATUS_MANUAL_REVIEW,
    INCIDENT_STATUS_PENDING,
    DataOpsIncident,
    IncidentRepository,
)


STATUS_OPTIONS = [
    "all",
    INCIDENT_STATUS_PENDING,
    "investigating",
    "evidence_recorded",
    "decision_recorded",
    "action_succeeded",
    "action_failed",
    INCIDENT_STATUS_MANUAL_REVIEW,
    INCIDENT_STATUS_COMPLETED,
    INCIDENT_STATUS_FAILED,
]


st.set_page_config(
    page_title="Agentic AWS DataOps",
    layout="wide",
    initial_sidebar_state="expanded",
)


def main() -> None:
    st.title("Agentic AWS DataOps")
    st.caption("Incident remediation pipeline monitor")

    settings = _load_settings()
    if settings is None:
        return

    repository = IncidentRepository(settings.DATABASE_URL)
    incidents = _load_incidents(settings.DATABASE_URL)
    filtered = _sidebar_filters(incidents)

    _render_metrics(filtered)
    left, right = st.columns([1.25, 1.0], gap="large")
    with left:
        selected = _render_incident_table(filtered)
    with right:
        if selected is None:
            st.info("Select an incident from the table to inspect details.")
        else:
            _render_incident_details(repository, selected)


def _load_settings() -> Settings | None:
    try:
        return get_settings()
    except ValidationError as exc:
        st.error("Configuration is incomplete.")
        st.code(str(exc), language="text")
        return None
    except Exception as exc:
        st.error("Failed to load application settings.")
        st.code(str(exc), language="text")
        return None


@st.cache_data(ttl=10)
def _incident_rows(database_url: str) -> list[dict[str, Any]]:
    repository = IncidentRepository(database_url)
    incidents = repository.list_incidents(limit=1000)
    return [_incident_to_row(incident) for incident in incidents]


def _load_incidents(database_url: str) -> list[dict[str, Any]]:
    try:
        return _incident_rows(database_url)
    except Exception as exc:
        st.error("Failed to load incidents from PostgreSQL.")
        st.code(str(exc), language="text")
        st.info(
            "Check that the database is running and reachable. For AWS RDS, "
            "its security group must allow your current public IP on port 5432. "
            "For local PostgreSQL, start docker compose up -d."
        )
        return []


def _incident_to_row(incident: DataOpsIncident) -> dict[str, Any]:
    evidence = _mapping(incident.evidence_json)
    action_result = _mapping(incident.action_result)
    decisions = _mapping(evidence.get("decisions"))
    remediation_decision = _mapping(decisions.get("remediation"))
    evidence_decision = _mapping(decisions.get("evidence"))
    return {
        "incident_id": incident.incident_id,
        "pipeline_name": incident.pipeline_name,
        "dagster_run_id": incident.dagster_run_id,
        "source_uri": incident.source_uri,
        "incident_type": incident.incident_type,
        "severity": incident.severity,
        "error_summary": incident.error_summary,
        "status": incident.status,
        "selected_action": incident.selected_action,
        "decision_confidence": incident.decision_confidence,
        "created_at": incident.created_at,
        "updated_at": incident.updated_at,
        "evidence_json": evidence,
        "action_result": action_result,
        "incident_report": incident.incident_report,
        "evidence_action": evidence_decision.get("selected_action"),
        "remediation_action": remediation_decision.get("selected_action"),
        "remediation_confidence": remediation_decision.get("confidence"),
    }


def _sidebar_filters(incidents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    st.sidebar.header("Filters")
    selected_status = st.sidebar.selectbox("Status", STATUS_OPTIONS, index=0)
    pipeline_names = sorted(
        {
            str(row["pipeline_name"])
            for row in incidents
            if row.get("pipeline_name") is not None
        }
    )
    selected_pipeline = st.sidebar.selectbox(
        "Pipeline",
        ["all", *pipeline_names],
        index=0,
    )
    search = st.sidebar.text_input("Search incident/source", value="").strip().lower()
    refresh = st.sidebar.button("Refresh")
    if refresh:
        _incident_rows.clear()
        st.rerun()

    filtered = []
    for row in incidents:
        if selected_status != "all" and row["status"] != selected_status:
            continue
        if selected_pipeline != "all" and row["pipeline_name"] != selected_pipeline:
            continue
        searchable = " ".join(
            str(row.get(field) or "")
            for field in ("incident_id", "source_uri", "error_summary", "incident_type")
        ).lower()
        if search and search not in searchable:
            continue
        filtered.append(row)
    return filtered


def _render_metrics(rows: list[dict[str, Any]]) -> None:
    status_counts = Counter(str(row.get("status") or "unknown") for row in rows)
    action_counts = Counter(
        str(row.get("selected_action") or "none") for row in rows
    )
    confidence_values = [
        float(row["decision_confidence"])
        for row in rows
        if row.get("decision_confidence") is not None
    ]
    avg_confidence = (
        sum(confidence_values) / len(confidence_values)
        if confidence_values
        else 0.0
    )

    metric_cols = st.columns(4)
    metric_cols[0].metric("Incidents", len(rows))
    metric_cols[1].metric("Pending", status_counts.get(INCIDENT_STATUS_PENDING, 0))
    metric_cols[2].metric("Manual Review", status_counts.get(INCIDENT_STATUS_MANUAL_REVIEW, 0))
    metric_cols[3].metric("Avg Confidence", f"{avg_confidence:.2f}")

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("Status Mix")
        if status_counts:
            st.bar_chart(pd.DataFrame(status_counts.items(), columns=["status", "count"]), x="status", y="count")
        else:
            st.info("No status data.")
    with chart_cols[1]:
        st.subheader("Action Mix")
        if action_counts:
            st.bar_chart(pd.DataFrame(action_counts.items(), columns=["action", "count"]), x="action", y="count")
        else:
            st.info("No action data.")


def _render_incident_table(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    st.subheader("Incidents")
    if not rows:
        st.info("No incidents match the selected filters.")
        return None

    table = pd.DataFrame(
        [
            {
                "incident_id": row["incident_id"],
                "status": row["status"],
                "type": row["incident_type"],
                "severity": row["severity"],
                "action": row["selected_action"],
                "confidence": row["decision_confidence"],
                "updated_at": row["updated_at"],
                "source_uri": row["source_uri"],
            }
            for row in rows
        ]
    )
    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
    )
    selected_id = st.selectbox(
        "Selected incident",
        [str(row["incident_id"]) for row in rows],
        index=0,
    )
    return next(row for row in rows if row["incident_id"] == selected_id)


def _render_incident_details(
    repository: IncidentRepository,
    row: dict[str, Any],
) -> None:
    st.subheader("Incident Detail")
    st.write(f"`{row['incident_id']}`")
    st.write(row.get("error_summary") or "No error summary recorded.")

    cols = st.columns(3)
    cols[0].metric("Status", str(row.get("status") or "unknown"))
    cols[1].metric("Selected Action", str(row.get("selected_action") or "none"))
    confidence = row.get("decision_confidence")
    cols[2].metric(
        "Decision Confidence",
        "n/a" if confidence is None else f"{float(confidence):.2f}",
    )

    _render_manual_actions(repository, row)

    tabs = st.tabs(["Evidence", "Nulls", "Decisions", "Postmortem", "Raw"])
    with tabs[0]:
        _render_schema_evidence(row)
    with tabs[1]:
        _render_null_profile(row)
    with tabs[2]:
        _render_decisions(row)
    with tabs[3]:
        report = row.get("incident_report")
        if isinstance(report, str) and report.strip():
            st.markdown(report)
        else:
            st.info("No Bedrock postmortem has been recorded.")
    with tabs[4]:
        st.json(
            {
                "evidence_json": row.get("evidence_json", {}),
                "action_result": row.get("action_result", {}),
            }
        )


def _render_manual_actions(
    repository: IncidentRepository,
    row: dict[str, Any],
) -> None:
    with st.form(f"manual-action-{row['incident_id']}"):
        action = st.radio(
            "Review action",
            ["acknowledge", "mark_resolved", "escalate"],
            horizontal=True,
        )
        note = st.text_area("Operator note", value="", height=80)
        submitted = st.form_submit_button("Apply")
        if not submitted:
            return
        status = {
            "acknowledge": INCIDENT_STATUS_MANUAL_REVIEW,
            "mark_resolved": INCIDENT_STATUS_COMPLETED,
            "escalate": INCIDENT_STATUS_FAILED,
        }[action]
        report = row.get("incident_report")
        report_text = report if isinstance(report, str) and report.strip() else _manual_report(row, action, note)
        payload = {
            "dashboard_action": action,
            "operator_note": note.strip() or "No operator note provided.",
            "previous_status": row.get("status"),
            "applied_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        }
        try:
            repository.finalize_incident(
                str(row["incident_id"]),
                action_result=payload,
                incident_report=report_text,
                status=status,
            )
            _incident_rows.clear()
            st.success(f"Incident updated to {status}.")
            st.rerun()
        except Exception as exc:
            st.error("Failed to apply manual action.")
            st.code(str(exc), language="text")


def _render_schema_evidence(row: dict[str, Any]) -> None:
    validation = _validation(row)
    schema_comparison = _mapping(validation.get("schema_comparison"))
    if not schema_comparison:
        schema_comparison = _latest_tool_data(row, "inspect_schema")

    missing = schema_comparison.get("missing_columns", [])
    extra = schema_comparison.get("extra_columns", [])
    drift = schema_comparison.get("type_drift_columns", [])

    st.write("Schema drift")
    drift_rows = []
    for column in _sequence(missing):
        drift_rows.append({"column": column, "issue": "missing", "expected": "", "observed": ""})
    for column in _sequence(extra):
        drift_rows.append({"column": column, "issue": "extra", "expected": "", "observed": ""})
    for item in _sequence(drift):
        item_map = _mapping(item)
        drift_rows.append(
            {
                "column": item_map.get("column", ""),
                "issue": "type_drift",
                "expected": item_map.get("expected_type", ""),
                "observed": item_map.get("observed_type", ""),
            }
        )
    if drift_rows:
        st.dataframe(pd.DataFrame(drift_rows), use_container_width=True, hide_index=True)
    else:
        st.success("No schema drift recorded.")


def _render_null_profile(row: dict[str, Any]) -> None:
    validation = _validation(row)
    null_profile = _mapping(validation.get("null_profile"))
    if not null_profile:
        tool_data = _latest_tool_data(row, "profile_nulls")
        null_profile = _mapping(tool_data.get("columns"))
    if not null_profile:
        st.info("No null profile recorded.")
        return

    rows = [
        {
            "column": column,
            "null_ratio": float(_mapping(metrics).get("null_ratio", 0.0)),
            "null_count": int(_mapping(metrics).get("null_count", 0)),
        }
        for column, metrics in null_profile.items()
    ]
    profile_frame = pd.DataFrame(rows)
    st.bar_chart(profile_frame, x="column", y="null_ratio")
    st.dataframe(profile_frame, use_container_width=True, hide_index=True)


def _render_decisions(row: dict[str, Any]) -> None:
    evidence = _mapping(row.get("evidence_json"))
    decisions = _mapping(evidence.get("decisions"))
    if not decisions:
        st.info("No decider decisions recorded.")
        return
    decision_rows = []
    for phase, decision in decisions.items():
        decision_map = _mapping(decision)
        decision_rows.append(
            {
                "phase": phase,
                "selected_action": decision_map.get("selected_action"),
                "confidence": decision_map.get("confidence"),
                "reasoning": decision_map.get("reasoning"),
            }
        )
    st.dataframe(pd.DataFrame(decision_rows), use_container_width=True, hide_index=True)


def _manual_report(row: Mapping[str, Any], action: str, note: str) -> str:
    return (
        "# Incident Report\n"
        "## Summary\n"
        f"Incident `{row.get('incident_id')}` was updated from the dashboard.\n\n"
        "## Deterministic Action Taken\n"
        f"Manual dashboard action `{action}` was applied.\n\n"
        "## Recommended Engineer Actions\n"
        f"{note.strip() or 'Review source data and downstream consumers.'}"
    )


def _validation(row: Mapping[str, Any]) -> Mapping[str, Any]:
    evidence = _mapping(row.get("evidence_json"))
    validation = _mapping(evidence.get("validation"))
    return validation


def _latest_tool_data(row: Mapping[str, Any], tool_name: str) -> Mapping[str, Any]:
    evidence = _mapping(row.get("evidence_json"))
    tool_results = _mapping(evidence.get("tool_results"))
    for result in reversed(list(tool_results.values())):
        result_map = _mapping(result)
        if result_map.get("tool_name") == tool_name:
            return _mapping(result_map.get("data"))
    return {}


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, list | tuple):
        return value
    return []


if __name__ == "__main__":
    main()
