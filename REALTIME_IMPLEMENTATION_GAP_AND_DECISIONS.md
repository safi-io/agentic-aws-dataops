# Real-Time Implementation Gap And Decisions

This file is the project source of truth for what is already implemented, what is missing for the real-time design, what credentials are needed, and which implementation decisions must stay fixed to avoid vague or hallucinated behavior.

## Target System

The intended system is a self-healing DataOps incident workflow:

```text
Dagster pipeline fails
  -> failure metadata, logs, and context are stored in PostgreSQL
  -> Dagster remediation sensor detects a new failure
  -> Strands Decider receives current context and allowed tools
  -> Decider selects one tool with confidence
  -> selected tool runs
  -> tool findings are persisted to PostgreSQL
  -> Amazon Bedrock generates an incident report from verified facts
  -> Streamlit displays the report and incident state
```

Allowed tools:

1. Inspect Schema
2. Null Checks
3. Quarantine
4. Alert Engineer
5. Retry Pipeline

Important behavior rule:

- Transient errors can be retried automatically.
- Schema mismatch, null threshold failures, missing data, and other data quality failures should not be silently fixed automatically.
- For non-transient data issues, the system should investigate, confirm the likely cause, optionally contain the batch, and generate a clear incident report for the engineer.

## Current Project Status

The current project already has several strong pieces:

- Local PostgreSQL and LocalStack S3 through `docker-compose.yml`.
- S3 Bronze/Silver/Quarantine prefixes.
- Validated environment settings in `config/settings.py`.
- Bronze-to-Silver Dagster assets in `dagster_project/assets.py`.
- Validation failure incident creation.
- SQLAlchemy incident repository in `dataops_agent/incident_state.py`.
- Incident remediation sensor in `dagster_project/sensors.py`.
- End-to-end agent orchestrator in `dataops_agent/orchestrator.py`.
- Strict Decider client in `dataops_agent/decider_client.py`.
- Tool dispatcher in `dataops_agent/dispatcher.py`.
- Typed tool result contract in `dataops_agent/tools/base.py`.
- S3-first tools for schema inspection, null profiling, quarantine, retry gating, and engineer alert payloads.
- Bedrock report generator with deterministic fallback.
- Streamlit dashboard with filters, evidence, decisions, reports, and manual actions.

## Missing Or Needs Update

### 1. Generic Dagster Failure Capture Is Missing

Current state:

- Incidents are created when `silver_orders` validation fails and when
  `bronze_orders` cannot parse an empty or malformed CSV (added during the
  mixed-data tests on 2026-10-19).
- The system does not yet capture every arbitrary Dagster run failure.
- It does not automatically persist Dagster event logs, stack traces, op names, asset keys, or failure metadata for all failed jobs.

Needed for real-time implementation:

- Add a Dagster run failure sensor or failure hook.
- When a Dagster run fails, extract a bounded failure context:
  - run id
  - job name
  - op or asset key
  - failure message
  - relevant log excerpt
  - stack trace excerpt
  - source URI, if available
  - failure classification, if known
- Store that context in `dataops_incidents.evidence_json`.

Recommended approach:

- Keep one incident table: `dataops_incidents`.
- Store detailed failure context inside `evidence_json` instead of creating a second table at first.
- Add extra columns only if querying by those fields becomes necessary.

### 2. Raw S3 Bronze Sensor Is Missing

Current state:

- There is a remediation sensor that polls pending incidents.
- There is no sensor that polls S3 Bronze for new CSV files and triggers ingestion.

Needed:

- Add an S3 raw Bronze sensor.
- It should list objects under the Bronze prefix.
- It should trigger `orders_data_quality_job` for new CSV files.
- It should deduplicate using object key and ETag.

Reason:

- Without this, the system is not fully real-time from new file arrival to validation.
- The friend repo already has a good reference implementation for this idea.

### 3. Incident Sensor Should Claim The Specific Incident It Triggered

Current state:

- `incident_remediation_sensor` emits a run for a specific `incident_id`.
- `DataOpsIncidentOrchestrator.run_next_incident()` then claims the next pending incident from the database.
- It does not guarantee it claims the same incident that the sensor placed in run config.

Needed:

- Update `IncidentRepository.claim_pending_incident()` to optionally accept an `incident_id`.
- Update the orchestrator to claim `triggered_incident_id` when provided.

Reason:

- This avoids confusing mismatches where a Dagster run says it is processing incident A but the orchestrator claims incident B.

### 4. Global `AWS_ENDPOINT_URL` Must Be Split

Current state:

- `.env` contains:

```text
AWS_ENDPOINT_URL=http://127.0.0.1:4566
```

- `config/settings.py` applies `AWS_ENDPOINT_URL` to all boto3 clients.
- That is good for LocalStack S3.
- That is bad for real Amazon Bedrock, because the Bedrock runtime client can accidentally point to LocalStack.

Needed:

- Replace global `AWS_ENDPOINT_URL` with service-specific endpoint settings:
  - `S3_ENDPOINT_URL`
  - optionally `BEDROCK_ENDPOINT_URL` only for advanced custom cases

Recommended rule:

- LocalStack endpoint should apply to S3 only.
- Bedrock should use real AWS Bedrock Runtime endpoints in real runs.

### 5. Current AWS Credentials Are Local/Test Only

Current `.env` values:

```text
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_ENDPOINT_URL=http://127.0.0.1:4566
```

These are valid for LocalStack-style development, not real AWS.

Needed for real AWS:

- Use AWS SSO/profile, IAM role, or real temporary credentials.
- Do not commit real AWS credentials.
- For production, prefer IAM roles and Secrets Manager over static keys.

### 6. Strands Decider Integration Should Use Local CLI For The Demo

Current state:

- `.env` currently points to an HTTP endpoint:

```text
STRANDS_DECIDER_ENDPOINT=http://127.0.0.1:9000
STRANDS_DECIDER_API_KEY=<your-decider-api-key>
```

- If no service is running at this endpoint, the Decider call fails.
- The current project falls back to `alert_engineer`.

New decision:

- For the hackathon demo, use the local `strands-decider` CLI instead of a hosted Decider API key.
- Model ID:

```text
StrandsAgents/strands-decider-2B-hobson-v19
```

Example intended command shape:

```powershell
strands-decider ask StrandsAgents/strands-decider-2B-hobson-v19 `
  --state "<incident context>" `
  --choice "Which tool should handle this?=inspect_schema,profile_nulls,quarantine_batch,alert_engineer,retry_pipeline"
```

Needed:

- Update `dataops_agent/decider_client.py` to support:
  - `DECIDER_MODE=cli`
  - local CLI invocation with `strands-decider`
  - parsing selected action, confidence, and action probabilities
  - deterministic fallback when the CLI/model is unavailable
- Keep HTTP endpoint mode optional for future hosted deployments.

Important:

- The CLI/model path is good for the submitted hackathon demo and local development.
- Do not claim a hosted production Decider endpoint unless it is actually deployed and tested.
- The first local CLI run may need to download model files from Hugging Face.

### 7. Add Explicit Policy Engine

Current state:

- The dispatcher enforces the strict allowlist.
- `retry_pipeline` checks retry counts.
- There is no central policy engine for action rules.

Needed:

- Add `dataops_agent/policies.py`.
- Enforce these deterministic rules before tool execution:
  - only allowlisted actions can run
  - retry only transient errors
  - do not retry schema drift or null-threshold failures
  - cap retry count
  - never delete or mutate Bronze source data
  - fallback to `alert_engineer` when confidence is too low or policy rejects an action

Reason:

- Strands Decider should choose.
- Python policy should enforce.
- Tool execution should never rely only on model choice.

### 8. Quarantine Should Be Containment, Not Automatic Resolution

Current state:

- In `dataops_agent/orchestrator.py`, a successful remediation tool generally leads to `completed`.
- That means successful quarantine can be treated as completed.

Problem:

- Your stated design says non-transient data issues should not be automatically fixed.
- Quarantine contains a bad batch; it does not prove the upstream data contract is fixed.

Needed:

- Treat successful quarantine as `manual_review` or a new status such as `contained`.
- The report should say the system contained the batch and confirmed the problem.
- The engineer should still review the producer contract or source data issue.

Recommended final status policy:

```text
retry_pipeline success on transient error -> completed
quarantine_batch success on data quality issue -> manual_review or contained
inspect_schema/profile_nulls evidence only -> manual_review unless followed by containment
alert_engineer -> manual_review
tool failure -> failed
```

### 9. Retry Pipeline Does Not Yet Trigger A Real Dagster Rerun

Current state:

- `retry_pipeline.py` determines whether retry is permitted.
- It returns rerun metadata.
- It does not actually launch a Dagster rerun.

Needed:

- Implement actual retry execution.
- Options:
  - emit a Dagster `RunRequest` from a retry sensor
  - call Dagster GraphQL/API
  - use Dagster instance APIs if running inside the same Dagster environment

Rule:

- Only transient failures should trigger retry.
- Retries must be idempotent and bounded.

### 10. Alert Engineer Tool Does Not Yet Send A Real Notification

Current state:

- `alert_engineer.py` builds a structured notification payload.
- It does not send Slack, email, PagerDuty, SNS, SES, or webhook notifications.

Needed for real-time implementation:

- Choose one alert channel:
  - Amazon SNS
  - Slack webhook
  - PagerDuty webhook
  - email through SES
- Store alert delivery result in `action_result`.

### 11. Failure Logs Need Sanitization And Size Limits

Current state:

- Bedrock report generation uses structured evidence.
- Generic pipeline log capture is not implemented yet.
- There is no central log sanitizer.

Needed:

- Add a log sanitization function before sending logs to Decider or Bedrock.
- Redact likely secrets:
  - AWS keys
  - tokens
  - passwords
  - database URLs
  - bearer tokens
- Truncate logs to a fixed maximum size.

Reason:

- Logs may contain secrets or sensitive customer data.
- Bedrock and Decider should only receive necessary context.

### 12. Dataset Schema Contract Needs A Clear Source Of Truth

Current state:

- Current sample data uses this 6-column schema:

```text
order_id, customer_id, order_date, status, amount, currency
```

- The written problem statement uses a simpler 3-column example:

```text
order_id, customer_id, amount
```

Needed:

- Decide which schema is the official demo contract.
- Store the contract in a config file or schema registry instead of hard-coding only in Python.

Recommended:

- Keep the 6-column schema for the current project because the sample data already matches it.
- Mention the 3-column version only as a simplified explanation in documentation.

### 13. Bedrock Should Only Generate Reports From Verified Facts

Current state:

- `bedrock_report.py` already instructs Bedrock not to invent facts.
- It also has deterministic fallback.

Needed:

- Keep Bedrock limited to report generation.
- Do not let Bedrock choose tools.
- Do not let Bedrock generate executable remediation code.
- Preserve explicit `maxTokens` in every Bedrock call.

### 14. Store Decider Probability Distribution If Available

Current state:

- The Decider response stores:
  - selected action
  - confidence
  - reasoning

Possible improvement:

- If the Decider returns probabilities for all tools, store them in `evidence_json.decisions`.

Reason:

- Your model screenshot shows action probabilities.
- Storing them helps explain why `inspect_schema` was selected when the state mentions `unexpected column`.

### 15. Tests Are Missing

Current state:

- The current project has no visible test suite.

Needed:

- Add tests for:
  - settings validation
  - incident repository
  - schema inspection
  - null checks
  - quarantine behavior
  - retry limits
  - Decider fallback
  - Bedrock fallback
  - Dagster sensors
  - Dagster assets
  - dashboard data parsing helpers

Reason:

- For a real-time remediation project, tests are not optional.
- Tool safety and incident lifecycle logic must be verified.

### 16. Documentation And Environment Templates Are Missing

Current state:

- Current project has no README.
- Current project has no `.env.example`.
- Current project has no `pyproject.toml`.

Needed:

- Add `README.md`.
- Add `.env.example`.
- Add `pyproject.toml`.
- Keep real `.env` out of version control.

## Manual Credentials And Setup Needed

### Local Development

For the current local setup:

- Docker must be installed and running.
- PostgreSQL comes from `docker-compose.yml`.
- LocalStack S3 comes from `docker-compose.yml`.
- LocalStack can use:

```text
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```

Manual local setup still needed:

- Start Docker Compose.
- Start Dagster.
- Start Streamlit.
- Install and run local `strands-decider` CLI if you want real local Decider choices beyond fallback.

Previous local Decider endpoint mode:

```text
http://127.0.0.1:9000
```

If neither the CLI mode nor endpoint mode is available, the current project will fall back to `alert_engineer`.

### Real AWS Development Or Demo

Manual AWS setup needed:

1. AWS account access.
2. AWS CLI or SDK credentials.
3. Region selected.
4. Bedrock model access enabled in that region.
5. IAM permission to invoke the selected Bedrock model.
6. S3 bucket with prefixes:
   - `bronze/`
   - `silver/`
   - `quarantine/`
7. PostgreSQL database:
   - local Postgres, or
   - Amazon RDS PostgreSQL
8. Database credentials:
   - host
   - port
   - database name
   - username
   - password
9. Network access from the app/Dagster host to PostgreSQL.
10. Local `strands-decider` installed if using demo CLI mode.
11. Hugging Face/model download access for first local model run.
12. Real Strands Decider endpoint and API key only if using Decider as an external hosted service.
13. Alert channel credentials if real alerting is implemented:
   - SNS topic ARN, or
   - Slack webhook, or
   - PagerDuty integration key, or
   - SES/email configuration

### Current Tooling Check

AWS CLI is installed in this environment:

```text
aws-cli/2.34.5
```

Not verified yet:

- real AWS identity
- Bedrock model access
- live Decider endpoint
- live RDS connectivity

Reason:

- The current `.env` uses local/test credentials and a LocalStack endpoint.

## Required Environment Variable Update

Replace this pattern:

```text
AWS_ENDPOINT_URL=http://127.0.0.1:4566
```

With this pattern:

```text
S3_ENDPOINT_URL=http://127.0.0.1:4566
BEDROCK_ENDPOINT_URL=
```

Recommended variables:

```text
DATABASE_URL=
AWS_REGION=
AWS_PROFILE=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=

S3_BUCKET_NAME=
S3_ENDPOINT_URL=
S3_BRONZE_PREFIX=bronze/
S3_SILVER_PREFIX=silver/
S3_QUARANTINE_PREFIX=quarantine/

STRANDS_DECIDER_ENDPOINT=
STRANDS_DECIDER_API_KEY=
DECIDER_MODE=cli
STRANDS_DECIDER_MODEL_ID=StrandsAgents/strands-decider-2B-hobson-v19

BEDROCK_MODEL_ID=
BEDROCK_MAX_TOKENS=1200

MAX_NULL_PERCENTAGE=0.15
MAX_RETRY_COUNT=2
```

## Implementation Decisions

These decisions should stay fixed unless intentionally changed.

### Decision 1: PostgreSQL Is The Incident Source Of Truth

All incident lifecycle state should be stored in PostgreSQL.

Do not use Streamlit session state, local files, or Dagster logs as the only source of truth.

### Decision 2: Use One Incident Table First

Use `dataops_incidents` for both validation-created incidents and generic Dagster failures.

Store extra failure context in `evidence_json`.

Only add more tables when query needs become clear.

### Decision 3: Decider Chooses, Python Enforces

Strands Decider may choose an action.

Python policy must validate that action before execution.

The Decider must never execute tools directly.

For this hackathon implementation, Decider can run locally through the `strands-decider` CLI.

The local Decider still only chooses from the explicit tool list.

### Decision 4: Bedrock Reports, It Does Not Remediate

Amazon Bedrock should generate the incident report from verified data.

It should not:

- choose tools
- call tools
- generate executable remediation code
- claim results that tools did not prove

### Decision 5: No Arbitrary Tool Execution

Only these actions are allowed:

- `inspect_schema`
- `profile_nulls`
- `quarantine_batch`
- `alert_engineer`
- `retry_pipeline`

Any other selected action must be rejected and escalated.

### Decision 6: Retry Only Transient Failures

`retry_pipeline` is only valid for transient errors such as:

- temporary network failure
- S3 throttling
- service unavailable
- timeout

Do not retry schema mismatch, missing data, or null threshold failures as the main remediation.

### Decision 7: Quarantine Is Containment

Quarantine protects downstream systems from bad data.

It does not prove the upstream issue is fixed.

Quarantine should usually lead to manual review or contained status, not full completed status.

### Decision 8: Logs Must Be Bounded And Sanitized

Before logs are sent to Decider or Bedrock:

- redact secrets
- truncate large logs
- keep only useful failure context

### Decision 9: LocalStack Is For S3 Local Development Only

Do not route Bedrock calls to LocalStack.

Do not claim real AWS behavior when using LocalStack-only services.

### Decision 11: Local Strands Decider Is The Demo Path

Use `strands-decider` locally for the submitted demo unless a hosted Decider endpoint is provided.

This removes the need for these hosted-service credentials in the demo path:

```text
STRANDS_DECIDER_ENDPOINT
STRANDS_DECIDER_API_KEY
```

Production deployment would still need either:

- a hosted Decider service endpoint, or
- a deployed runtime that can serve the local decision model reliably.

### Decision 10: The Current Official Demo Schema Is Six Columns

Use this schema for the current project demo:

```text
order_id, customer_id, order_date, status, amount, currency
```

The 3-column schema can be documented as a simplified explanation only.

## Recommended Implementation Order

Do these updates in this order:

1. Split `AWS_ENDPOINT_URL` into `S3_ENDPOINT_URL` and optional `BEDROCK_ENDPOINT_URL`.
2. Add `.env.example`.
3. Add generic Dagster failure capture into PostgreSQL.
4. Add S3 raw Bronze sensor.
5. Update orchestrator to claim the specific incident from sensor run config.
6. Update `decider_client.py` to support `DECIDER_MODE=cli`.
7. Add explicit `PolicyEngine`.
8. Change final status behavior so quarantine means contained/manual review, not fully fixed.
9. Implement actual Dagster retry execution for transient failures.
10. Add real alert delivery integration.
11. Add log sanitization and truncation.
12. Add tests for repository, tools, policies, sensors, Decider CLI parsing, and Bedrock fallback.
13. Add README and real-time architecture documentation.

## Do Not Claim Until Implemented

Do not claim the project has these features until they are actually implemented and tested:

- automatic capture of all Dagster failure logs
- hosted Strands Decider production endpoint
- local Strands Decider integration until `strands-decider` is installed and `decider_client.py` supports CLI mode
- real Bedrock invocation with valid AWS credentials
- actual Dagster rerun execution
- real engineer notification delivery
- production RDS deployment
- full end-to-end real-time run from S3 upload to Streamlit report
