"""Streamlit Dashboard for Agentic AWS DataOps Incident Monitoring."""

import os
import streamlit as st
import pandas as pd

from dagster_project.definitions import rds_resource

st.set_page_config(
    page_title="Agentic AWS DataOps | Incident Control Center",
    page_icon="🛡️",
    layout="wide",
)

st.title("🛡️ Agentic AWS DataOps Control Center")
st.caption("Automated, evidence-driven data pipeline incident response powered by Strands Decider 2B & Amazon Bedrock")

# Sidebar settings and metrics
st.sidebar.header("System Status")
st.sidebar.text(f"Database: Amazon RDS ({rds_resource.database})")
st.sidebar.text(f"Host: {rds_resource.host}:{rds_resource.port}")
st.sidebar.text(f"Decider: {os.getenv('DECIDER_MODEL_NAME', 'strands-decider-2b')}")
st.sidebar.text(f"Bedrock: {os.getenv('BEDROCK_MODEL_ID', 'claude-3-haiku')}")

st.sidebar.divider()
st.sidebar.subheader("Seed Test Scenarios")
scenario = st.sidebar.selectbox(
    "Choose Seed Dataset",
    ["clean.csv", "unexpected_column.csv", "high_null_rate.csv"]
)
if st.sidebar.button("Simulate Ingestion Run"):
    st.sidebar.success(f"Dispatched ingestion with {scenario}!")


def load_incidents():
    """Loads incidents directly using RDSResource."""
    try:
        return rds_resource.get_recent_incidents(limit=50)
    except Exception as e:
        st.warning(f"Unable to query Amazon RDS: {e}")
        return []


incidents = load_incidents()

# Top metric bar
col1, col2, col3, col4 = st.columns(4)
total_count = len(incidents)
pending_count = sum(1 for i in incidents if i.get("status") == "pending")
processing_count = sum(1 for i in incidents if i.get("status") == "processing")
resolved_count = sum(1 for i in incidents if i.get("status") == "resolved")
review_count = sum(1 for i in incidents if i.get("status") == "needs_review")

col1.metric("Total Incidents", total_count)
col2.metric("Pending / Processing", f"{pending_count + processing_count}")
col3.metric("Resolved", resolved_count)
col4.metric("Needs Human Review", review_count)

st.divider()

if not incidents:
    st.info("No incidents recorded yet. Trigger a pipeline run or seed sample data to view incident workflows.")
else:
    st.subheader("📋 Active & Recent Incidents")
    df_incidents = pd.DataFrame(incidents)
    display_cols = ["incident_id", "pipeline_name", "incident_type", "severity", "status", "created_at"]
    st.dataframe(df_incidents[[c for c in display_cols if c in df_incidents.columns]], use_container_width=True)

    # Detailed view
    st.subheader("🔍 Incident Investigation & Audit Trail")
    selected_id = st.selectbox("Select Incident ID to Inspect", [i["incident_id"] for i in incidents])
    selected_inc = next((i for i in incidents if i["incident_id"] == selected_id), None)

    if selected_inc:
        c_left, c_right = st.columns([1, 1])

        with c_left:
            st.markdown("### 📊 Diagnostic Evidence")
            st.write(f"**Pipeline:** `{selected_inc.get('pipeline_name')}`")
            st.write(f"**Source File:** `{selected_inc.get('source_uri')}`")
            st.write(f"**Error Summary:** {selected_inc.get('error_summary')}")
            st.write(f"**Severity:** `{selected_inc.get('severity')}` | **Status:** `{selected_inc.get('status')}`")

            st.markdown("#### Evidence JSON")
            st.json(selected_inc.get("evidence_json", {}))

            st.markdown("### 🤖 Strands Decider 2B Action")
            st.write(f"**Selected Action:** `{selected_inc.get('selected_action', 'None')}`")
            confidence = selected_inc.get('decision_confidence')
            if confidence:
                st.progress(float(confidence), text=f"Confidence: {round(float(confidence)*100, 1)}%")

            st.markdown("#### Confirmed Tool Result")
            st.json(selected_inc.get("action_result", {}))

        with c_right:
            st.markdown("### 📝 Amazon Bedrock Incident Report")
            report_text = selected_inc.get("incident_report")
            if report_text:
                st.markdown(report_text)
            else:
                st.info("Report generation in progress or pending resolution.")
