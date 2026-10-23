# AWS Setup Status

Last checked: 2026-10-18

## Startup Verification: 2026-10-19

- Both apps now start using `scripts/run.py` with `--env-file .env.aws.local`.
  See [README.md](README.md) for restart and job commands.
- Dagster loads the `agentic_dataops` code location from `workspace.yaml` and
  keeps run history in `.dagster_home/`.
- RDS was available, but its security group allowed the previous client IP
  `182.190.177.53/32`. Added TCP port 5432 access for the current client IP
  `182.190.180.179/32` to `sg-0066d81cd25de01af`.
  The added rule ID is `sgr-03afd3af888ee1c86`. Existing rules were retained.
  If the client's public IP changes again, its allowed address must be updated.
- PostgreSQL reads succeeded, and Streamlit rendered three existing incidents
  with no application errors.
- A live `orders_data_quality_job` run completed successfully, including Bronze
  and Silver materializations: `cb5e1d4e-1252-4639-8853-fe68f3654907`.
- All six startup regression tests passed.

The remaining sections record the earlier AWS setup; they were not all rechecked
during the startup repair.

## AWS Account

- Account ID: `731451715213`
- Region used: `us-east-1`
- Default CLI identity verified: `arn:aws:iam::731451715213:user/amplify-developer`
- `agentic-dataops` profile also exists, but it resolves to root credentials. Prefer the default IAM user and avoid using root for project work.

## Completed

### S3

Created project bucket:

```text
agentic-dataops-731451715213-us-east-1
```

Created/uploaded objects:

```text
bronze/orders.csv
bronze/unexpected_column.csv
bronze/high_nulls.csv
silver/.keep
quarantine/.keep
```

Use this bucket in project config:

```env
S3_BUCKET_NAME=agentic-dataops-731451715213-us-east-1
S3_BRONZE_PREFIX=bronze/
S3_SILVER_PREFIX=silver/
S3_QUARANTINE_PREFIX=quarantine/
```

### Bedrock

Bedrock model discovery works in `us-east-1`.

Test invocation succeeded with:

```text
amazon.nova-micro-v1:0
```

Recommended project config:

```env
BEDROCK_MODEL_ID=amazon.nova-micro-v1:0
```

### Strands Decider Local CLI

Installed local package:

```text
strands-decider 0.1.0
```

Live local model test succeeded with:

```text
Model: StrandsAgents/strands-decider-2B-hobson-v19
State: unexpected column promo_code in orders CSV
Selected action: inspect_schema
Confidence: 0.809
```

Observed probabilities:

```text
inspect_schema    0.847
retry_pipeline    0.089
quarantine_batch  0.028
alert_engineer    0.024
profile_nulls     0.013
```

Notes:

- First run downloaded/loaded the model from Hugging Face.
- Hugging Face requests were unauthenticated; set `HF_TOKEN` only if you need higher rate limits or faster downloads.

### RDS PostgreSQL

Created RDS PostgreSQL demo database:

```text
DB identifier: agentic-dataops-postgres
Engine: PostgreSQL
Class: db.t4g.micro
Storage: 20 GB gp3
Region: us-east-1
Public access: true
Security group: sg-0066d81cd25de01af
Allowed inbound PostgreSQL source: 182.188.241.206/32
Endpoint: agentic-dataops-postgres.c8zes4ouet7m.us-east-1.rds.amazonaws.com
Port: 5432
Database name: dataops
Master username: dataopsadmin
Backup retention: 0 days
```

The master password is managed by AWS Secrets Manager:

```text
arn:aws:secretsmanager:us-east-1:731451715213:secret:rds!db-ff8168f6-a84d-4bad-b448-3f3a6008501c-BHnCtK
```

Initialized table:

```text
dataops_incidents
```

Verification result:

```text
incident_count=0
```

### AWS Budget Alerts

Created monthly budget:

```text
Budget name: AgenticDataOps-Monthly-150
Limit: 150 USD/month
Email: khadijasajid280@gmail.com
```

Configured actual spend alerts:

```text
25 USD
50 USD
100 USD
140 USD
```

### SNS Engineer Alerts

Created SNS topic:

```text
arn:aws:sns:us-east-1:731451715213:agentic-dataops-engineer-alerts
```

Subscribed email:

```text
khadijasajid280@gmail.com
```

Status:

```text
pending confirmation
```

Action needed:

- Open the AWS SNS confirmation email and confirm the subscription.
- Alerts will not deliver to email until confirmed.

Verification:

- Direct SNS publish succeeded.
- Python `alert_engineer` tool publish succeeded.

## Not Completed Yet

### Hosted Strands Decider

Hosted Decider credentials are still missing:

```env
STRANDS_DECIDER_ENDPOINT=
STRANDS_DECIDER_API_KEY=
```

Normal AWS CLI credentials do not provide these automatically unless your hackathon/Strands provider gave you a hosted Decider endpoint.

Updated demo decision:

- Use local `strands-decider` CLI for the submitted demo.
- Model ID:

```text
StrandsAgents/strands-decider-2B-hobson-v19
```

The code has been updated to call the local CLI when `DECIDER_MODE=cli`.

Recommended config:

```env
DECIDER_MODE=cli
STRANDS_DECIDER_MODEL_ID=StrandsAgents/strands-decider-2B-hobson-v19
STRANDS_DECIDER_ENDPOINT=
STRANDS_DECIDER_API_KEY=
```

### Real Alert Channel

SNS email alerting is configured and wired into the `alert_engineer` tool.

Remaining action:

- Confirm the SNS email subscription.
## Important Config Change Needed

For real AWS, do not use LocalStack values:

```env
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://127.0.0.1:4566
```

For real AWS, remove those and rely on the configured AWS CLI credentials or deployment IAM role.

Use service-specific endpoints only if needed:

```env
S3_ENDPOINT_URL=
BEDROCK_ENDPOINT_URL=
```

## Generate Real AWS `.env`

Run this helper to create a local `.env.aws.local` file from AWS Secrets Manager:

```powershell
.\scripts\write_real_aws_env.ps1
```

That generated file contains secrets and is ignored by Git.
