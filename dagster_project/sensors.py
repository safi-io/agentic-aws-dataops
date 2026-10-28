"""Dagster sensors for pending DataOps incidents."""

from dagster import RunRequest, SensorEvaluationContext, SkipReason, sensor

from dataops_agent.incident_state import INCIDENT_STATUS_PENDING, IncidentRepository
from dagster_project.jobs import incident_diagnosis_job


@sensor(
    name="incident_remediation_sensor",
    job=incident_diagnosis_job,
    minimum_interval_seconds=30,
)
def incident_remediation_sensor(context: SensorEvaluationContext):
    """Poll PostgreSQL for pending incidents and launch idempotent diagnosis runs."""

    repository = IncidentRepository()
    incidents = repository.list_incidents(status=INCIDENT_STATUS_PENDING, limit=25)
    if not incidents:
        yield SkipReason("No pending DataOps incidents found.")
        return

    for incident in incidents:
        run_key = f"incident_{incident.incident_id}_{incident.updated_at.isoformat()}"
        yield RunRequest(
            run_key=run_key,
            run_config={
                "ops": {
                    "diagnose_next_incident": {
                        "config": {
                            "incident_id": incident.incident_id,
                            "source_uri": incident.source_uri or "",
                        }
                    }
                }
            },
            tags={
                "dataops/incident_id": incident.incident_id,
                "dataops/pipeline_name": incident.pipeline_name,
                "dataops/status": incident.status,
            },
        )


__all__ = ["incident_remediation_sensor"]
