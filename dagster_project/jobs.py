"""Dagster job definitions for data quality and incident diagnosis."""

from dagster import AssetSelection, Field, Out, define_asset_job, job, op

from dataops_agent.orchestrator import DataOpsIncidentOrchestrator
from dagster_project.assets import bronze_orders, silver_orders


orders_data_quality_job = define_asset_job(
    name="orders_data_quality_job",
    selection=AssetSelection.assets(bronze_orders, silver_orders),
)


@op(
    name="diagnose_next_incident",
    config_schema={
        "incident_id": Field(str, is_required=False, default_value=""),
        "source_uri": Field(str, is_required=False, default_value=""),
    },
    out=Out(dict),
)
def diagnose_next_incident(context) -> dict[str, object]:
    """Run the DataOps incident orchestrator for one pending incident."""

    op_config = dict(context.op_config or {})
    orchestration_context = {
        "dagster_run_id": context.run_id,
        "triggered_incident_id": op_config.get("incident_id") or None,
        "source_uri": op_config.get("source_uri") or None,
    }
    result = DataOpsIncidentOrchestrator().run_next_incident(
        context=orchestration_context
    )
    metadata = {
        "status": result.status,
        "success": result.success,
    }
    if result.incident_id is not None:
        metadata["incident_id"] = result.incident_id
    if result.error is not None:
        metadata["error"] = result.error
    context.add_output_metadata(metadata)
    if not result.success:
        raise RuntimeError(
            f"incident diagnosis failed for {result.incident_id}: {result.error}"
        )
    return result.model_dump(mode="json")


@job(name="incident_diagnosis_job")
def incident_diagnosis_job() -> None:
    """Dagster job that diagnoses one pending incident per run."""

    diagnose_next_incident()


__all__ = [
    "bronze_orders",
    "incident_diagnosis_job",
    "orders_data_quality_job",
    "silver_orders",
]
