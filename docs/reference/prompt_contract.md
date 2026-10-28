# Prompt Contract

The following raw prompt is the implementation contract for the Agentic AWS DataOps build.

```text
### 0. Reference & Documentation Initialization (Execute First)
Before writing any application code:
1. Create a dedicated documentation folder: `docs/reference/`.
2. Inside that directory, create two persistent reference files containing the exact architecture and system guidelines for your reference throughout this build:
   - `docs/reference/agentic_dataops_spec.md`: Store the full architectural overview, core technology responsibilities, operational flow, allowed tool definitions, and reliability policies.
   - `docs/reference/prompt_contract.md`: Store this entire prompt, including the execution constraints, safety policies, database schema, and phased rollout instructions.
3. Keep these files unmodified as the single source of truth to check implementation details, schemas, and contracts during all subsequent phases.  You are a Principal Platform & AI Systems Engineer. Build a production-grade, fault-tolerant implementation of the "Agentic AWS DataOps" incident remediation pipeline.

The solution must be strictly deterministic, defensively typed (Python 3.11+, Pydantic v2, SQLAlchemy 2.0), and adhere to production-grade resilience standards. Do NOT generate placeholders, stubbed methods, or `# TODO` comments. Implement every file completely with exhaustive error handling.

---

### Critical Production Rules (Anti-Breakage Constraints)

1. **Atomic Concurrency & Claiming:**
   - When polling PostgreSQL for `pending` incidents, you must execute `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction.
   - Atomically transition the status to `investigating` immediately upon claiming to prevent multiple workers or race conditions.
2. **Idempotent Sensor Runs:**
   - In `dagster_project/sensors.py`, every `RunRequest` MUST yield an explicit `run_key=f"incident_{incident.incident_id}_{incident.updated_at.isoformat()}"`.
   - Never emit a `RunRequest` without a deterministic `run_key`.
3. **Data Immutability on S3:**
   - Original Bronze input files are strictly immutable.
   - All tools (such as `quarantine_batch`) must use `boto3` server-side copy (`s3_client.copy_object`). Never invoke `delete_object` or perform destructive overwrites on the Bronze prefix.
4. **Resilient Decider HTTP Contract (Strands Decider 2B):**
   - Execute Strands Decider via `httpx.Client` with a 10-second timeout, `tenacity` retry logic (exponential backoff, max 3 attempts), and strict JSON payload validation.
   - Target HTTP Request Spec:
     POST {STRANDS_DECIDER_ENDPOINT}/v1/decide
     Headers: {"Authorization": "Bearer {STRANDS_DECIDER_API_KEY}", "Content-Type": "application/json"}
     Body:
     {
       "phase": "evidence" | "remediation",
       "incident_context": { ... },
       "allowed_actions": ["inspect_schema", "profile_nulls", "quarantine_batch", "retry_pipeline", "alert_engineer"]
     }
   - Expected Response Spec:
     {
       "selected_action": "quarantine_batch",
       "confidence": 0.95,
       "reasoning": "Schema drift identified on untrusted batch."
     }
   - If the HTTP call fails, returns non-200, provides unparseable JSON, or returns an unapproved action, catch the exception cleanly and fallback deterministically to `selected_action="alert_engineer"` with `confidence=0.0`.
5. **LocalStack & AWS Client Flexibility:**
   - Configure all `boto3` client initializations through a centralized factory in `config/settings.py`.
   - If `AWS_ENDPOINT_URL` is set, pass it to `boto3.client(..., endpoint_url=settings.AWS_ENDPOINT_URL)` to support local offline testing via LocalStack.

---

### Step-by-Step Code Generation Schedule

To prevent output truncation and token exhaustion, we will build the project sequentially.
Wait for my explicit "continue" command before advancing between major phases.

#### PHASE 1: Configuration, Schema & Database State Management
1. `config/settings.py`:
   - Pydantic Settings class with environment variables: `DATABASE_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, `S3_BUCKET_NAME`, `S3_BRONZE_PREFIX` ("bronze/"), `S3_SILVER_PREFIX` ("silver/"), `S3_QUARANTINE_PREFIX` ("quarantine/"), `STRANDS_DECIDER_API_KEY`, `STRANDS_DECIDER_ENDPOINT`, `BEDROCK_MODEL_ID` ("anthropic.claude-3-haiku-20240307-v1:0"), `MAX_NULL_PERCENTAGE` (0.15), and `MAX_RETRY_COUNT` (3).
2. `dataops_agent/incident_state.py`:
   - Exact SQLAlchemy 2.0 ORM model mapping to:
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
   - Production `IncidentRepository` class handling:
     - `create_incident(...)`
     - `claim_pending_incident(...)` (with row-level lock)
     - `record_evidence(...)`
     - `record_decision(...)`
     - `finalize_incident(...)`
     - `get_incident(...)`
     - `list_incidents(...)`

#### PHASE 2: Allowlisted Tools & Dispatcher
1. `dataops_agent/tools/`:
   - Base `ToolResult` Pydantic model (`success: bool`, `data: dict`, `error: Optional[str]`).
   - `inspect_schema.py`: Compares observed CSV schema against target schema contract. Highlights extra, missing, and type-drifted columns.
   - `profile_nulls.py`: Computes null counts and ratios per column; detects null threshold breaches.
   - `quarantine_batch.py`: Performs non-destructive S3 server-side copy to quarantine prefix with metadata tags.
   - `retry_pipeline.py`: Queries Postgres retry attempts; permits rerun if attempts < `MAX_RETRY_COUNT`, else fails with retry exhaustion.
   - `alert_engineer.py`: Generates structured audit and operational notification payload.
2. `dataops_agent/dispatcher.py`:
   - `ToolDispatcher` validating choices against the strict allowlist: `{"inspect_schema", "profile_nulls", "quarantine_batch", "retry_pipeline", "alert_engineer"}`.
   - Safely executes the tool, logs outcomes, and persists the result to Postgres before setting incident state.

#### PHASE 3: Decider, Bedrock Postmortem & Agent Workflow
1. `dataops_agent/decider_client.py`:
   - Production client with `httpx` and `tenacity` matching the exact HTTP specification above.
   - Two-phase execution: Phase 1 (evidence selection) and Phase 2 (remediation selection).
   - Hard fallback mechanism to `alert_engineer`.
2. `dataops_agent/bedrock_report.py`:
   - Generates natural-language incident reports using AWS Bedrock (`boto3.client('bedrock-runtime')`).
   - System prompt formats root cause, evidence diff, deterministic action taken, and recommended engineer actions into structured Markdown.
3. `dataops_agent/orchestrator.py`:
   - Single callable orchestrating the end-to-end diagnosis lifecycle: Claim -> Phase 1 Decider -> Tool Dispatch -> Phase 2 Decider -> Tool Dispatch -> Bedrock Report -> Finalize.

#### PHASE 4: Dagster Pipeline & Sensors
1. `dagster_project/assets.py`:
   - `bronze_orders`: Reads CSV from S3.
   - `silver_orders`: Validates schema and null ratios using Pandas.
     - On success: saves clean Parquet to Silver prefix.
     - On failure: captures error details, inserts `pending` incident into Postgres, and raises controlled failure.
2. `dagster_project/jobs.py`: Defines asset evaluation and incident diagnosis jobs.
3. `dagster_project/sensors.py`:
   - `incident_remediation_sensor` polling Postgres for unclaimed/pending records.
   - Yields idempotent `RunRequest` with deterministic `run_key`.

#### PHASE 5: Streamlit Dashboard, Docker & Sample Data
1. `dashboard/app.py`:
   - Streamlit monitoring interface displaying incident status metrics, schema diffs, null percentage bar charts, Decider confidence, and rendered Bedrock Markdown postmortems.
   - Human-in-the-loop manual review action buttons.
2. `docker-compose.yml`: Local PostgreSQL container, LocalStack (S3 mock), and database migration script.
3. `sample_data/`:
   - `clean_orders.csv`
   - `unexpected_column.csv`
   - `high_nulls.csv`

---

Start with PHASE 1. Generate `config/settings.py` and `dataops_agent/incident_state.py` now. Write out all code completely. & 'c:\Users\arfar\Downloads\Agentic AWS DataOps Documentation (1).pdf'
```

