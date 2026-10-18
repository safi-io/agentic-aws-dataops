"""Dagster definitions for the Agentic AWS DataOps pipeline."""

from dagster import Definitions

from dagster_project.assets import bronze_orders, silver_orders
from dagster_project.jobs import incident_diagnosis_job, orders_data_quality_job
from dagster_project.sensors import incident_remediation_sensor


defs = Definitions(
    assets=[bronze_orders, silver_orders],
    jobs=[orders_data_quality_job, incident_diagnosis_job],
    sensors=[incident_remediation_sensor],
)


__all__ = ["defs"]
