# Agentic AWS DataOps Reference Specification

## Objective

Build a production-grade, fault-tolerant implementation of the Agentic AWS DataOps incident remediation pipeline. The implementation must be deterministic, defensively typed, and suitable for Python 3.11+ using Pydantic v2 and SQLAlchemy 2.0.

## Core Architecture

The pipeline detects failed data quality runs, records incidents in PostgreSQL, claims incidents atomically, gathers evidence with allowlisted tools, asks a Strands Decider service for evidence and remediation actions, executes only approved actions, writes operational state back to PostgreSQL, optionally generates a Bedrock incident report, and exposes operational state through Dagster and Streamlit surfaces.

## Core Technology Responsibilities

- `config/settings.py` owns validated runtime configuration and centralized AWS client construction.
- `dataops_agent/incident_state.py` owns the PostgreSQL incident schema mapping and all durable incident lifecycle transitions.
- `dataops_agent/tools/` owns allowlisted deterministic tools.
- `dataops_agent/dispatcher.py` validates selected actions against the strict allowlist and persists tool outcomes.
- `dataops_agent/decider_client.py` owns the resilient Strands Decider HTTP contract.
- `dataops_agent/bedrock_report.py` owns AWS Bedrock postmortem generation.
- `dataops_agent/orchestrator.py` owns the end-to-end incident lifecycle.
- `dagster_project/assets.py` owns Bronze-to-Silver data validation and incident creation.
- `dagster_project/jobs.py` owns Dagster job definitions.
- `dagster_project/sensors.py` owns idempotent polling and run triggering.
- `dashboard/app.py` owns incident monitoring and human-in-the-loop controls.

## Operational Flow

1. Dagster reads immutable Bronze CSV input from S3.
2. Dagster validates schema and null ratios.
3. On success, Dagster writes clean Parquet output to the Silver prefix.
4. On failure, Dagster inserts a pending incident in PostgreSQL and raises a controlled failure.
5. A sensor polls pending incidents and emits idempotent Dagster run requests.
6. The orchestrator claims exactly one pending incident using a row-level lock.
7. The orchestrator calls the Strands Decider for the evidence phase.
8. The dispatcher validates and executes the selected evidence action.
9. The orchestrator calls the Strands Decider for the remediation phase.
10. The dispatcher validates and executes the selected remediation action.
11. Bedrock generates a structured Markdown incident report.
12. The repository finalizes the incident with durable action result and report state.
13. Streamlit presents metrics, evidence, decisions, reports, and manual review actions.

## Allowed Tool Definitions

The strict action allowlist is:

- `inspect_schema`
- `profile_nulls`
- `quarantine_batch`
- `retry_pipeline`
- `alert_engineer`

No unlisted action may be executed. Invalid, missing, malformed, or failed decisions must fall back deterministically to `alert_engineer`.

## Reliability Policies

### Atomic Concurrency And Claiming

When polling PostgreSQL for pending incidents, workers must use `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction and atomically transition the claimed row to `investigating`.

### Idempotent Sensor Runs

Every Dagster `RunRequest` emitted by `dagster_project/sensors.py` must include:

```python
run_key = f"incident_{incident.incident_id}_{incident.updated_at.isoformat()}"
```

No sensor may emit a `RunRequest` without a deterministic `run_key`.

### S3 Bronze Immutability

Original Bronze input files are immutable. Tools must never delete or destructively overwrite Bronze objects. Quarantine operations must use S3 server-side copy through `s3_client.copy_object`.

### Strands Decider HTTP Contract

The decider client must use `httpx.Client`, a 10-second timeout, and `tenacity` exponential retry with a maximum of three attempts.

Request:

```http
POST {STRANDS_DECIDER_ENDPOINT}/v1/decide
Authorization: Bearer {STRANDS_DECIDER_API_KEY}
Content-Type: application/json
```

Body:

```json
{
  "phase": "evidence",
  "incident_context": {},
  "allowed_actions": [
    "inspect_schema",
    "profile_nulls",
    "quarantine_batch",
    "retry_pipeline",
    "alert_engineer"
  ]
}
```

Expected response:

```json
{
  "selected_action": "quarantine_batch",
  "confidence": 0.95,
  "reasoning": "Schema drift identified on untrusted batch."
}
```

If the HTTP call fails, returns non-200, provides unparseable JSON, or returns an unapproved action, the client must catch the exception cleanly and return:

```json
{
  "selected_action": "alert_engineer",
  "confidence": 0.0
}
```

### AWS Client Construction

All boto3 clients must be created through the centralized factory in `config/settings.py`. If `AWS_ENDPOINT_URL` is set, the factory must pass it as `endpoint_url=settings.AWS_ENDPOINT_URL` to support LocalStack and offline testing.

## PostgreSQL Incident Table

```sql
CREATE TABLE dataops_incidents (
    incident_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    dagster_run_id TEXT,
    source_uri TEXT,
    incident_type TEXT NOT NULL,
    severity TEXT,
    error_summary TEXT,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    selected_action TEXT,
    decision_confidence DOUBLE PRECISION,
    action_result JSONB,
    incident_report TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Phase 1 Scope

Implement:

- `config/settings.py`
- `dataops_agent/incident_state.py`

The Phase 1 implementation must include:

- Validated settings for database, AWS, S3 prefixes, Strands Decider, Bedrock, and retry/null thresholds.
- Centralized boto3 S3 and Bedrock runtime client factories.
- SQLAlchemy 2.0 ORM mapping for `dataops_incidents`.
- Production `IncidentRepository` methods:
  - `create_incident(...)`
  - `claim_pending_incident(...)`
  - `record_evidence(...)`
  - `record_decision(...)`
  - `finalize_incident(...)`
  - `get_incident(...)`
  - `list_incidents(...)`

