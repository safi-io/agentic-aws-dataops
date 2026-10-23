"""Retest existing S3 batches without saving CSVs or asset data on local disk."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
from urllib.parse import urlparse
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_s3_client, get_settings


EXISTING_PREFIX = "bronze/mixed-tests/20261018T205950Z-2afc5252/"
CASES = {
    "clean": ("clean_large.csv", True),
    "broken": ("late_invalid_amount.csv", False),
}


def run_cases(names: list[str]) -> bool:
    from dagster import DagsterInstance, mem_io_manager
    from dagster_project import defs
    from dataops_agent.incident_state import IncidentRepository

    settings = get_settings()
    s3 = get_s3_client(settings)
    objects = s3.get_paginator("list_objects_v2")
    repository = IncidentRepository(settings.DATABASE_URL)
    job = defs.get_repository_def().get_job("orders_data_quality_job")
    home = Path(os.environ.get("DAGSTER_HOME", PROJECT_ROOT / ".dagster_home")).resolve()
    all_matched = True

    with DagsterInstance.from_config(str(home)) as instance:
        for name in names:
            filename, expected_success = CASES[name]
            source_key = EXISTING_PREFIX + filename
            source_metadata = s3.head_object(Bucket=settings.S3_BUCKET_NAME, Key=source_key)
            silver_prefix = f"{settings.S3_SILVER_PREFIX}manual-retests/{uuid4().hex}/"
            config = {"ops": {
                "bronze_orders": {"config": {"source_key": source_key}},
                "silver_orders": {"config": {
                    "source_key": source_key, "silver_key_prefix": silver_prefix,
                    "max_null_percentage": 0.15,
                }},
            }}
            print(f"Testing {name}: s3://{settings.S3_BUCKET_NAME}/{source_key}", flush=True)
            # One process per invocation and an in-memory IO manager avoid Dagster's
            # default on-disk DataFrame intermediates. Run records/logs remain local.
            result = job.execute_in_process(
                instance=instance, run_config=config, raise_on_error=False,
                resources={"io_manager": mem_io_manager},
                tags={"dataops/manual_retest": name},
            )
            incidents = [row for row in repository.list_incidents(limit=1000)
                         if row.dagster_run_id == result.run_id]
            outputs = [item["Key"] for page in objects.paginate(
                Bucket=settings.S3_BUCKET_NAME, Prefix=silver_prefix
            ) for item in page.get("Contents", [])]
            bronze_ready = any(event.is_step_success and event.step_key == "bronze_orders"
                               for event in result.all_events)
            rows = None
            frame_mib = None
            if bronze_ready:
                frame = result.output_for_node("bronze_orders")
                rows = len(frame)
                frame_mib = round(int(frame.memory_usage(deep=True).sum()) / (1024 * 1024), 3)
                del frame
            matched = (
                result.success == expected_success
                and rows == 1100
                and len(incidents) == (0 if expected_success else 1)
                and len(outputs) == (1 if expected_success else 0)
                and all(row.incident_type == "schema_drift" for row in incidents)
            )
            if result.success:
                output_uri = result.output_for_node("silver_orders")
                parsed = urlparse(output_uri)
                metadata = s3.head_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
                matched &= metadata["ContentLength"] > 0
                print(f"Silver output: {output_uri}", flush=True)
            all_matched &= matched
            print(
                f"{'PASS' if matched else 'MISMATCH'} {name}: "
                f"{'SUCCESS' if result.success else 'FAILURE'}; rows={rows}; "
                f"CSV={source_metadata['ContentLength']} bytes; "
                f"DataFrame={frame_mib} MiB; incidents={len(incidents)}",
                flush=True,
            )
            print(f"Run: http://127.0.0.1:3000/runs/{result.run_id}", flush=True)
            del result
    return all_matched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("clean", "broken", "both"), default="both")
    parser.add_argument("--env-file", default=".env.aws.local")
    args = parser.parse_args()
    env_file = (PROJECT_ROOT / args.env_file).resolve()
    if not env_file.is_file():
        parser.error(f"Environment file not found: {env_file}")
    os.environ["DATAOPS_ENV_FILE"] = str(env_file)
    names = list(CASES) if args.case == "both" else [args.case]
    return 0 if run_cases(names) else 1


if __name__ == "__main__":
    raise SystemExit(main())
