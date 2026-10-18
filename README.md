# Agentic AWS DataOps

An S3 data quality pipeline with Dagster orchestration, PostgreSQL incident tracking, a Streamlit dashboard, and policy-controlled remediation using Strands Decider and Amazon Bedrock.

CSV batches remain unchanged in Bronze. Valid batches are written to Silver as Parquet. Failed batches create incidents for inspection, quarantine, retry assessment, or escalation.

## Setup

Requires Python 3.11 or later, an S3 bucket, and PostgreSQL. AWS deployment also requires an AWS profile with access to the relevant resources.

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
python -m pip install -e ".[dev]"
```

Copy `.env.aws.example` to `.env.aws.local` and set `DATABASE_URL`, `S3_BUCKET_NAME`, and `AWS_PROFILE`. Use a URL-encoded database password and retain `sslmode=require` for RDS. Leave explicit AWS keys and `AWS_ENDPOINT_URL` blank when using a profile; leave `AWS_PROFILE` blank when using an instance role.

Initialize a new database with `db/init/001_create_dataops_incidents.sql`. This branch uses that schema; it does not migrate an existing installation automatically.

Start each service in a separate terminal:

```bash
python scripts/run.py dagster --env-file .env.aws.local
python scripts/run.py dashboard --env-file .env.aws.local
```

Dagster: http://127.0.0.1:3000 · Dashboard: http://127.0.0.1:8501

For optional local development, copy `.env.example` to `.env`, run `docker compose up -d`, and use `--env-file .env`. Compose initializes PostgreSQL and seeds the S3 emulator with the sample batches. LocalStack provides S3 only; Bedrock reports require AWS.

## Run a batch

Expected columns:

| Column | Type |
| --- | --- |
| `order_id`, `customer_id` | Integer |
| `order_date` | Date |
| `status`, `currency` | String |
| `amount` | Float |

Unexpected or missing columns, invalid types, empty batches, unreadable CSV, or a column exceeding `MAX_NULL_PERCENTAGE` fail validation. The default null limit is 15%; exactly 15% passes. Column order may vary.

In Dagster, open **Jobs → orders_data_quality_job → Launchpad** and set both source keys:

```yaml
ops:
  bronze_orders:
    config:
      source_key: bronze/orders.csv
  silver_orders:
    config:
      source_key: bronze/orders.csv
```

Use an existing object in your bucket. For local samples, `bronze/orders.csv` passes; `bronze/unexpected_column.csv` and `bronze/high_nulls.csv` fail and create incidents. Run the included failure configuration from the command line:

```bash
python scripts/run.py job --env-file .env.aws.local --config run_configs/schema_drift_demo.yml
```

Input objects stay in S3, and validation reads the complete batch into memory. Size batches for the worker's available RAM. Ingestion is launched manually; this branch does not include an S3 arrival sensor.

## Remediation

Enable `incident_remediation_sensor` in Dagster after configuring the decision service and Bedrock access. It claims pending incidents, collects evidence, selects an allowed action, records the result, and requests an incident report.

`DECIDER_MODE=cli` runs the configured model on the worker and can download model weights. Use `DECIDER_MODE=http` with `STRANDS_DECIDER_ENDPOINT` and `STRANDS_DECIDER_API_KEY` for a remote service. Keep the sensor stopped when running validation alone.

Quarantine copies the source without deleting Bronze. The retry tool validates whether a rerun is permitted; it does not launch a new pipeline run. SNS notifications are enabled only when `ALERT_SNS_TOPIC_ARN` is set.

## Shared AWS deployment

`deploy/aws/` provisions an EC2 worker, HTTPS gateway, instance role, and shared login. It uses an existing S3 bucket and initialized RDS PostgreSQL database with a Secrets Manager managed master password. The database VPC must have a public subnet with internet access.

```bash
python scripts/deploy_team.py deploy --profile default --region us-east-1 --bucket YOUR_BUCKET --database YOUR_RDS_INSTANCE --stack agentic-dataops-team
python scripts/deploy_team.py login --profile default --stack agentic-dataops-team
```

Stack outputs provide the public Dagster and dashboard URLs after bootstrap completes. Login credentials are written to the ignored `.deployment/team-access.txt` file. The worker defaults to `t3a.large`, processes one run at a time, and installs the decision model on AWS. Normal AWS charges apply.

Deploy updated source from this checkout:

```bash
python scripts/deploy_team.py update --profile default --bucket YOUR_BUCKET --stack agentic-dataops-team
```

The instance role reads Bronze and writes under `silver/shared-demo/` and `quarantine/shared-demo/`. Additional S3 permissions may be needed for quarantine existence checks and object tags. The deployment does not initialize the database or upload sample datasets.

## Tests

```bash
python -m pytest -q
```

The suite covers startup configuration, dashboard loading, mixed CSV batches including 1,100 rows, malformed-input incident creation, and deployment archive exclusions. Tests use small fixtures and mocked services; they do not require AWS access or a running database.
