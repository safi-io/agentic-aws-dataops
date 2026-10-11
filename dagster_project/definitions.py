"""Dagster definitions entrypoint registering assets, jobs, sensors, and resources."""

import os
import sys
from pathlib import Path
from dagster import Definitions, load_assets_from_modules

# Ensure project root is in sys.path when loaded directly via -f
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dagster_project import assets
from dagster_project.jobs import (
    orders_pipeline_job,
    incident_remediation_job,
)
from dagster_project.sensors import (
    pending_incident_sensor,
    s3_raw_bronze_sensor,
)
from dagster_project.resources import S3Resource, RDSResource

all_assets = load_assets_from_modules([assets])

# Reusable Amazon S3 Resource configuration
s3_resource = S3Resource(
    bucket_name=os.getenv("S3_BUCKET_NAME"),
    prefix=os.getenv("S3_BRONZE_PREFIX", "bronze/"),
    region_name=os.getenv("AWS_REGION", "us-east-1"),
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
)

# Reusable Amazon RDS (PostgreSQL) Resource configuration
rds_resource = RDSResource(
    host=os.getenv("RDS_HOST", os.getenv("POSTGRES_HOST", "localhost")),
    port=int(os.getenv("RDS_PORT", os.getenv("POSTGRES_PORT", "5432"))),
    database=os.getenv("RDS_DATABASE", os.getenv("POSTGRES_DB", "dataops_db")),
    username=os.getenv("RDS_USERNAME", os.getenv("POSTGRES_USER", "postgres")),
    password=os.getenv("RDS_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
    schema_name=os.getenv("RDS_SCHEMA", "public"),
    connection_string=os.getenv("DATABASE_URL"),
)

defs = Definitions(
    assets=all_assets,
    jobs=[
        orders_pipeline_job,
        incident_remediation_job,
    ],
    sensors=[
        pending_incident_sensor,
        s3_raw_bronze_sensor,
    ],
    resources={
        "s3": s3_resource,
        "rds": rds_resource,
    },
)