"""Generate and test mixed orders batches locally or against the configured AWS setup."""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
from io import BytesIO
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import get_s3_client, get_settings
from dagster_project.assets import validate_orders_frame
from dataops_agent.tools.inspect_schema import DEFAULT_ORDER_SCHEMA


SAMPLES = PROJECT_ROOT / "sample_data" / "mixed_cases"
THRESHOLD = 0.15


def generate_samples() -> None:
    """Write synthetic fixtures and independently specified expected outcomes."""
    SAMPLES.mkdir(parents=True, exist_ok=True)
    columns = list(DEFAULT_ORDER_SCHEMA)
    manifest = []

    def rows(count=20):
        return [
            {
                "order_id": 10000 + index,
                "customer_id": 20000 + index,
                "order_date": f"2026-10-{index % 28 + 1:02d}",
                "status": ("paid", "pending", "refunded")[index % 3],
                "amount": f"{10 + index * 1.25:.2f}",
                "currency": ("USD", "EUR", "GBP")[index % 3],
            }
            for index in range(count)
        ]

    def save(name, description, batch, *, fields=None, incident_type=None, raw=None):
        path = SAMPLES / f"{name}.csv"
        if raw is not None:
            path.write_text(raw, encoding="utf-8")
        else:
            with path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=fields or columns, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(batch)
        manifest.append({
            "name": name, "file": path.name, "description": description,
            "expected_status": "SUCCESS" if incident_type is None else "FAILURE",
            "expected_incident_type": incident_type, "rows": len(batch),
        })

    save("clean", "20 complete, valid orders", rows())
    save("reordered_columns", "Valid orders with columns in a different order", rows(), fields=columns[::-1])
    for name, missing, incident_type in (
        ("nulls_10_percent", 2, None),
        ("nulls_15_percent", 3, None),
        ("nulls_20_percent", 4, "null_threshold_breach"),
    ):
        batch = rows()
        for row in batch[:missing]:
            row["status"] = ""
        save(name, f"{missing}/20 missing status values; limit is 15%", batch, incident_type=incident_type)
    save("clean_large", "1,100 valid orders", rows(1100))
    save("missing_currency", "Required currency column is absent", rows(), fields=columns[:-1], incident_type="schema_drift")
    batch = rows()
    for row in batch:
        row["promo_code"] = "AUTUMN"
    save("extra_column", "Unexpected promo_code column", batch, fields=[*columns, "promo_code"], incident_type="schema_drift")
    batch = rows()
    batch[7]["amount"] = "broken-amount"
    save("invalid_amount", "One text value among numeric amounts", batch, incident_type="schema_drift")
    batch = rows()
    batch[12]["order_date"] = "2026-02-30"
    save("invalid_date", "One impossible date among valid dates", batch, incident_type="schema_drift")
    batch = rows()
    batch[7]["amount"] = "broken-amount"
    batch[12]["order_date"] = "not-a-date"
    for row in batch[:4]:
        row["status"] = ""
    save("mixed_bad_rows", "Good rows mixed with bad amounts, dates, and excessive nulls", batch, incident_type="data_quality_failure")
    batch = rows(1100)
    batch[1050]["amount"] = "broken-after-row-1000"
    save("late_invalid_amount", "Bad amount in data row 1,051 of 1,100", batch, incident_type="schema_drift")
    save("header_only", "Column headers with no order rows", [], incident_type="data_quality_failure")
    save("empty_file", "Completely empty CSV", [], raw="", incident_type="data_quality_failure")
    save("malformed_csv", "Unclosed quote prevents CSV parsing", [], raw=",".join(columns) + '\n10001,20001,2026-10-01,"unclosed,25.00,USD\n', incident_type="data_quality_failure")
    (SAMPLES / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def preview(cases: list[dict]) -> bool:
    matched = True
    for case in cases:
        try:
            frame = pd.read_csv(SAMPLES / case["file"])
        except (pd.errors.ParserError, pd.errors.EmptyDataError):
            accepted = False
        else:
            accepted = validate_orders_frame(
                frame, target_schema=DEFAULT_ORDER_SCHEMA, max_null_percentage=THRESHOLD
            )["valid"]
        correct = accepted == (case["expected_status"] == "SUCCESS")
        matched &= correct
        print(f"{'OK' if correct else 'MISMATCH'} {case['name']}: {'accepted' if accepted else 'rejected'}", flush=True)
    return matched


def live_test(cases: list[dict]) -> bool:
    """Run real assets sequentially and verify S3 output and PostgreSQL incidents."""
    from dagster import DagsterInstance
    from dagster_project import defs
    from dataops_agent.incident_state import IncidentRepository

    settings = get_settings()
    s3 = get_s3_client(settings)
    objects = s3.get_paginator("list_objects_v2")
    repository = IncidentRepository(settings.DATABASE_URL)
    campaign = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    bronze_prefix = f"{settings.S3_BRONZE_PREFIX}mixed-tests/{campaign}/"
    silver_prefix = f"{settings.S3_SILVER_PREFIX}mixed-tests/{campaign}/"
    reports = PROJECT_ROOT / "reports"
    reports.mkdir(exist_ok=True)
    report_path = reports / f"mixed-data-{campaign}.json"
    home = Path(os.environ.get("DAGSTER_HOME", PROJECT_ROOT / ".dagster_home")).resolve()
    job = defs.get_repository_def().get_job("orders_data_quality_job")
    report = {"campaign": campaign, "threshold": THRESHOLD, "bucket": settings.S3_BUCKET_NAME, "results": []}

    with DagsterInstance.from_config(str(home)) as instance:
        for case in cases:
            key = bronze_prefix + case["file"]
            payload = (SAMPLES / case["file"]).read_bytes()
            # Unique campaign keys preserve all existing Bronze objects.
            s3.put_object(Bucket=settings.S3_BUCKET_NAME, Key=key, Body=payload, ContentType="text/csv", IfNoneMatch="*")
            run_config = {"ops": {
                "bronze_orders": {"config": {"source_key": key}},
                "silver_orders": {"config": {
                    "source_key": key, "silver_key_prefix": silver_prefix,
                    "max_null_percentage": THRESHOLD,
                }},
            }}
            result = job.execute_in_process(
                instance=instance, run_config=run_config, raise_on_error=False,
                tags={"dataops/test_campaign": campaign, "dataops/test_case": case["name"]},
            )
            status = "SUCCESS" if result.success else "FAILURE"
            incidents = [row for row in repository.list_incidents(limit=1000) if row.dagster_run_id == result.run_id]
            silver_key = f"{silver_prefix}orders/{case['name']}-{result.run_id}.parquet"
            silver_exists = any(
                item["Key"] == silver_key
                for page in objects.paginate(Bucket=settings.S3_BUCKET_NAME, Prefix=silver_key)
                for item in page.get("Contents", [])
            )
            silver_rows = None
            content_matches = None
            if silver_exists:
                with s3.get_object(Bucket=settings.S3_BUCKET_NAME, Key=silver_key)["Body"] as body:
                    silver = pd.read_parquet(BytesIO(body.read()))
                silver_rows = len(silver)
                content_matches = silver.equals(pd.read_csv(BytesIO(payload)))
            expected_incident = case["expected_incident_type"]
            matched = (
                status == case["expected_status"]
                and silver_exists == (case["expected_status"] == "SUCCESS")
                and (not silver_exists or (silver_rows == case["rows"] and content_matches))
                and (len(incidents) == 1 if expected_incident else len(incidents) == 0)
                and all(row.incident_type == expected_incident for row in incidents)
            )
            entry = {
                **case, "actual_status": status, "run_id": result.run_id,
                "source_uri": f"s3://{settings.S3_BUCKET_NAME}/{key}",
                "silver_exists": silver_exists, "silver_rows": silver_rows,
                "silver_content_matches_input": content_matches, "matched": matched,
                "incidents": [{"id": row.incident_id, "type": row.incident_type, "status": row.status,
                               "errors": row.evidence_json.get("validation", {}).get("errors", [])}
                              for row in incidents],
            }
            report["results"].append(entry)
            report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            print(f"{'PASS' if matched else 'MISMATCH'} {case['name']}: {status}; Silver={silver_exists}; incidents={len(incidents)}", flush=True)

    lines = ["# Mixed-data live test results", "", f"Campaign: `{campaign}`", "",
             "Executed against real S3 and PostgreSQL using the registered Dagster job,",
             "sequentially in one process. Remediation was not invoked by this runner.", "",
             "| Case | Expected | Actual | Silver rows | Incidents | Check |",
             "|---|---|---|---:|---:|---|"]
    for row in report["results"]:
        lines.append(f"| {row['name']} | {row['expected_status']} | {row['actual_status']} | {row['silver_rows'] if row['silver_exists'] else 'none'} | {len(row['incidents'])} | {'PASS' if row['matched'] else 'MISMATCH'} |")
    lines.extend(["", "Rejected batches should appear as failed Dagster runs with pending incidents.",
                  "A failure is expected for these intentionally broken samples.", "",
                  f"The JSON report `{report_path.name}` contains source URIs, run IDs, and incident IDs."])
    report_path.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Report: {report_path.with_suffix('.md')}", flush=True)
    return all(row["matched"] for row in report["results"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true", help="Generate the synthetic sample CSVs")
    parser.add_argument("--live", action="store_true", help="Upload unique batches and execute against real services")
    parser.add_argument("--env-file", default=".env.aws.local")
    args = parser.parse_args()
    if args.generate:
        generate_samples()
    cases = json.loads((SAMPLES / "manifest.json").read_text(encoding="utf-8"))
    if not args.live:
        return 0 if preview(cases) else 1
    os.environ["DATAOPS_ENV_FILE"] = str((PROJECT_ROOT / args.env_file).resolve())
    return 0 if live_test(cases) else 1


if __name__ == "__main__":
    raise SystemExit(main())
