"""Dagster job definitions for pipeline execution and incident workflows."""

from typing import Dict, Any
from dagster import define_asset_job, job, op, OpExecutionContext

from dataops_agent.decider_client import StrandsDeciderClient
from dataops_agent.dispatcher import ToolDispatcher
from dataops_agent.bedrock_report import BedrockReportGenerator
from dagster_project.resources import RDSResource

# 1. Main Bronze to Silver Ingestion & Validation Pipeline Job
orders_pipeline_job = define_asset_job(
    name="orders_ingestion_pipeline",
    selection=["orders_bronze_data", "orders_validated_silver"],
    description="Ingests CSV from Bronze raw zone, validates contracts, and writes clean batches to Silver S3.",
)


# 2. Agentic Incident Investigation & Remediation Job
@op
def run_incident_agent_op(
    context: OpExecutionContext,
    config: dict,
    rds: RDSResource,
):
    """Executes the agentic workflow for a pending incident:

    Decider (Diagnosis) -> Dispatch Diagnostic -> Decider (Remediation) -> Dispatch Action -> Bedrock Report
    Persists findings and reports directly into Amazon RDS.
    """
    incident_id = config.get("incident_id")
    pipeline_name = config.get("pipeline_name", "orders_ingestion_pipeline")
    incident_type = config.get("incident_type", "UnknownIncident")
    source_uri = config.get("source_uri", "")
    error_summary = config.get("error_summary", "")

    context.log.info(f"Starting agent investigation for incident: {incident_id}")

    decider = StrandsDeciderClient()
    dispatcher = ToolDispatcher()
    reporter = BedrockReportGenerator()

    # Step 1: Decider - Diagnosis choice
    diag_choices = ["inspect_schema", "profile_nulls", "alert_engineer"]
    diag_decision = decider.select_action(
        incident_id=incident_id,
        question="Which diagnostic tool should be executed to assess this failure?",
        choices=diag_choices,
        incident_context={"incident_type": incident_type, "error_summary": error_summary, "source_uri": source_uri},
        stage="diagnosis",
    )
    context.log.info(f"Decider selected diagnostic: {diag_decision.selected_action} (conf: {diag_decision.confidence})")

    # Step 2: Execute diagnostic tool
    diag_result = dispatcher.dispatch(
        action=diag_decision.selected_action,
        incident_type=incident_type,
        params={"source_uri": source_uri, "incident_id": incident_id},
    )
    context.log.info(f"Diagnostic executed: {diag_result.get('status')}")

    # Step 3: Decider - Remediation choice based on diagnostic result
    remed_choices = ["quarantine_batch", "retry_pipeline", "alert_engineer"]
    remed_decision = decider.select_action(
        incident_id=incident_id,
        question="Based on diagnostic evidence, which recovery action should be taken?",
        choices=remed_choices,
        incident_context={
            "incident_type": incident_type,
            "error_summary": error_summary,
            "source_uri": source_uri,
            "diagnostic_output": diag_result,
            "retry_count": 0,
        },
        stage="remediation",
    )
    context.log.info(f"Decider selected remediation: {remed_decision.selected_action} (conf: {remed_decision.confidence})")

    # Step 4: Execute remediation tool
    remed_result = dispatcher.dispatch(
        action=remed_decision.selected_action,
        incident_type=incident_type,
        params={
            "source_uri": source_uri,
            "incident_id": incident_id,
            "pipeline_name": pipeline_name,
            "error_summary": error_summary,
            "retry_count": 0,
        },
    )
    context.log.info(f"Remediation tool result: {remed_result}")

    # Step 5: Bedrock generates explanation and report
    full_evidence = {
        "error_summary": error_summary,
        "diagnostic_action": diag_decision.selected_action,
        "diagnostic_output": diag_result,
    }
    report = reporter.generate_report(
        incident_id=incident_id,
        pipeline_name=pipeline_name,
        incident_type=incident_type,
        evidence=full_evidence,
        selected_action=remed_decision.selected_action,
        tool_result=remed_result,
    )
    context.log.info("Bedrock incident report generated successfully.")

    # Determine final incident status
    final_status = (
        "resolved"
        if remed_result.get("status") == "success" and remed_decision.selected_action != "alert_engineer"
        else "needs_review"
    )

    # Persist directly via RDSResource
    try:
        rds.update_incident_resolution(
            incident_id=incident_id,
            status=final_status,
            selected_action=remed_decision.selected_action,
            decision_confidence=remed_decision.confidence,
            action_result=remed_result,
            incident_report=report,
        )
        context.log.info(f"Updated incident {incident_id} in Amazon RDS to status: {final_status}")
    except Exception as e:
        context.log.error(f"Failed to update incident in Amazon RDS: {e}")

    return {
        "incident_id": incident_id,
        "diagnostic_action": diag_decision.selected_action,
        "remediation_action": remed_decision.selected_action,
        "remediation_result": remed_result,
        "report": report,
    }


@job(description="Remediates pipeline incidents using Strands Decider 2B and Bedrock")
def incident_remediation_job():
    run_incident_agent_op()
