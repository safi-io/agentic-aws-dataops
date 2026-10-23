# Project Review Guide

This guide explains which files to review first, why each file matters, and how the two projects connect internally.

There are two projects in this workspace:

- Current project: `C:\Users\arfar\Downloads\FYP Project\AWS Hachathon`
- Friend project: `C:\Users\arfar\Downloads\FYP Project\AWS Hachathon\friend-agentic-aws-dataops`

The best way to review both projects is by execution flow, not by random folders:

```text
CSV input
  -> Dagster asset
  -> validation
  -> PostgreSQL incident
  -> Dagster sensor
  -> remediation job
  -> Decider client
  -> dispatcher
  -> tool execution
  -> Bedrock report
  -> dashboard
```

## Files To Ignore

Do not spend time deeply reviewing generated or cache files:

- `__pycache__/`
- `.git/`
- `logs/`
- `.tmp_dagster_home.../`
- `dagster_project/schedules/schedules.db`
- `.pyc` files
- other generated cache/runtime files

## How To Review Each Function

For every important function, write down these five points:

1. What input does it receive?
2. What output does it return?
3. What external system does it touch?
   - S3
   - PostgreSQL
   - Dagster
   - Strands Decider
   - Amazon Bedrock
   - Streamlit
4. What happens if it fails?
5. Which file calls this function?

This method makes the project understandable line by line.

## Current Project Review Order

Start with the current project because it is the stronger production base.

### 1. `docker-compose.yml`

Review first because it explains the local infrastructure.

It starts:

- PostgreSQL for incident storage.
- LocalStack for local S3 simulation.

Why it matters:

- The Python code depends on these services.
- If PostgreSQL or LocalStack is not running, many parts of the project will fail.
- It explains the local development environment.

### 2. `.env`

Review second because it contains runtime configuration.

Important values:

- `DATABASE_URL`
- `AWS_REGION`
- `AWS_ENDPOINT_URL`
- `S3_BUCKET_NAME`
- `S3_BRONZE_PREFIX`
- `S3_SILVER_PREFIX`
- `S3_QUARANTINE_PREFIX`
- `STRANDS_DECIDER_ENDPOINT`
- `BEDROCK_MODEL_ID`

Why it matters:

- `config/settings.py` loads these values.
- S3, PostgreSQL, Decider, and Bedrock clients depend on them.
- It tells you whether the app is using LocalStack or real AWS.

Note:

- In a polished repo, `.env` should usually not be committed.
- A `.env.example` should be committed instead.

### 3. `requirements.txt`

Review this to understand the technology stack.

Important dependencies:

- `dagster`: pipeline orchestration.
- `boto3` and `botocore`: AWS clients.
- `pandas`: CSV validation and data profiling.
- `pyarrow`: Parquet writing.
- `SQLAlchemy`: database ORM.
- `psycopg`: PostgreSQL driver.
- `pydantic` and `pydantic-settings`: validation and config.
- `httpx`: Strands Decider HTTP calls.
- `tenacity`: retry logic.
- `streamlit`: dashboard.

Why it matters:

- It tells you what frameworks the project is built on.
- It helps explain imports across the codebase.

### 4. `db/init/001_create_dataops_incidents.sql`

Review this before Python database code.

It creates the `dataops_incidents` table.

Important columns:

- `incident_id`
- `pipeline_name`
- `dagster_run_id`
- `source_uri`
- `incident_type`
- `severity`
- `error_summary`
- `evidence_json`
- `status`
- `selected_action`
- `decision_confidence`
- `action_result`
- `incident_report`
- `created_at`
- `updated_at`

Why it matters:

- Almost every backend file reads or writes this table.
- The table is the central state store of the whole system.
- Understanding this table makes the orchestrator and dashboard easier to understand.

### 5. `localstack/init/01_bootstrap_s3.sh`

Review this to understand the local S3 setup.

It does these things:

- Creates the local S3 bucket.
- Uploads sample CSV files into the `bronze/` prefix.
- Creates placeholder objects for `silver/` and `quarantine/`.

Why it matters:

- Dagster reads CSVs from Bronze.
- Clean data is written to Silver.
- Bad data is copied to Quarantine.

### 6. `sample_data/`

Review the sample files after LocalStack setup.

Important files:

- `clean_orders.csv`
- `unexpected_column.csv`
- `high_nulls.csv`

Why it matters:

- These files represent demo scenarios.
- They trigger success, schema drift, and null-threshold failure paths.

## Current Project Core Runtime

### 7. `config/settings.py`

This is one of the most important files.

Review:

- `Settings`
- environment variable fields
- validators
- S3 prefix validation
- database URL validation
- AWS credential validation
- `get_settings`
- `create_boto3_session`
- `get_boto3_client`
- `get_s3_client`
- `get_bedrock_runtime_client`

Why it matters:

- All runtime configuration is validated here.
- All boto3 clients should be created here.
- LocalStack support depends on `AWS_ENDPOINT_URL`.
- Bad config fails early instead of creating confusing runtime errors.

How it relates to other files:

- `dagster_project/assets.py` uses settings and S3 client factories.
- `dataops_agent/tools/quarantine_batch.py` uses settings and S3 clients.
- `dataops_agent/bedrock_report.py` uses Bedrock client factory.
- `dashboard/app.py` loads settings before showing incidents.

### 8. `dagster_project/__init__.py`

Review this to understand Dagster registration.

It registers:

- assets
- jobs
- sensors

Why it matters:

- Dagster uses this file to know what exists in the project.
- If an asset, job, or sensor is not registered here, Dagster may not show or run it.

### 9. `dagster_project/assets.py`

This is the most important pipeline file.

Review in this order:

1. Constants such as pipeline name and incident types.
2. `OrdersAssetConfig`
3. `bronze_orders`
4. `silver_orders`
5. `validate_orders_frame`
6. `_record_validation_incident`
7. `_incident_type_from_validation`
8. `_silver_destination`
9. helper functions for schema/null checks.

What it does:

- Reads a Bronze CSV from S3.
- Parses it with pandas.
- Validates schema.
- Validates null percentages.
- Writes clean data as Parquet to Silver.
- Creates a PostgreSQL incident when validation fails.

Why it matters:

- This is where data quality problems are detected.
- This is where incidents begin.

How it relates to other files:

- Uses `config/settings.py` for S3 and config.
- Uses `IncidentRepository` from `dataops_agent/incident_state.py`.
- Uses schema helpers from `dataops_agent/tools/inspect_schema.py`.
- Produces incidents that sensors later process.

### 10. `dagster_project/jobs.py`

Review this after assets.

Important objects:

- `orders_data_quality_job`
- `diagnose_next_incident`
- `incident_diagnosis_job`

What it does:

- Defines the Bronze-to-Silver data quality job.
- Defines the incident diagnosis job.
- Calls `DataOpsIncidentOrchestrator`.

Why it matters:

- It connects Dagster orchestration to the agent runtime.
- It is the bridge between pipeline failure and automated remediation.

### 11. `dagster_project/sensors.py`

Review this after jobs.

Important function:

- `incident_remediation_sensor`

What it does:

- Polls PostgreSQL for pending incidents.
- Emits Dagster `RunRequest`s.
- Starts the incident diagnosis job.

Why it matters:

- Without the sensor, incidents stay pending.
- This file starts the automated response loop.

Current limitation:

- Your current project has an incident sensor.
- It does not yet have the friend repo's S3 raw Bronze sensor for discovering new S3 CSV files.

## Current Project Agent Core

### 12. `dataops_agent/incident_state.py`

This is the most important backend state file.

Review in this order:

1. Incident status constants.
2. `ALLOWED_ACTIONS`
3. `DataOpsIncident`
4. `IncidentRepository`
5. `create_incident`
6. `claim_pending_incident`
7. `record_evidence`
8. `record_decision`
9. `record_tool_result`
10. `finalize_incident`
11. `get_incident`
12. `list_incidents`

What it does:

- Maps the PostgreSQL table to a Python ORM class.
- Creates incidents.
- Claims pending incidents safely.
- Records decisions.
- Records tool results.
- Finalizes incidents.
- Lists incidents for the dashboard.

Why it matters:

- This file controls the incident lifecycle.
- It prevents duplicate unsafe processing using row locking.
- It is stronger than the friend repo's simpler state model.

How it relates to other files:

- Called by `dagster_project/assets.py` when validation fails.
- Called by `dataops_agent/orchestrator.py` during remediation.
- Called by `dataops_agent/dispatcher.py` after tools run.
- Called by `dashboard/app.py` to display incidents.

### 13. `dataops_agent/orchestrator.py`

This is the main agent workflow.

Review in this order:

1. `OrchestrationResult`
2. `DataOpsIncidentOrchestrator.__init__`
3. `run_next_incident`
4. `run_claimed_incident`
5. `_incident_context`
6. `_tool_context`
7. `_final_status`
8. `_fail_incident`

What it does:

- Claims one pending incident.
- Sends incident context to the Decider for evidence selection.
- Runs the evidence tool.
- Sends updated context to the Decider for remediation selection.
- Runs the remediation tool.
- Generates a Bedrock report.
- Finalizes the incident.

Why it matters:

- This is the end-to-end automated incident response loop.
- It explains how all agent components work together.

How it relates to other files:

- Uses `IncidentRepository`.
- Uses `StrandsDeciderClient`.
- Uses `ToolDispatcher`.
- Uses `BedrockReportGenerator`.
- Called by `dagster_project/jobs.py`.

### 14. `dataops_agent/decider_client.py`

Review this to understand action selection.

Important objects:

- `ORDERED_ALLOWED_ACTIONS`
- `DeciderRequest`
- `DeciderHttpResponse`
- `DeciderDecision`
- `StrandsDeciderClient`

What it does:

- Builds a strict request for Strands Decider.
- Sends HTTP request to the Decider endpoint.
- Validates the response.
- Falls back to `alert_engineer` if Decider fails.

Why it matters:

- The model is not allowed to choose arbitrary actions.
- All actions must be in the strict allowlist.
- Failures are handled safely.

How it relates to other files:

- Called by `dataops_agent/orchestrator.py`.
- Uses allowed actions from `dataops_agent/incident_state.py`.
- Produces decisions consumed by `dataops_agent/dispatcher.py`.

### 15. `dataops_agent/dispatcher.py`

Review this to understand how selected actions become actual code execution.

Important parts:

- `TOOL_REGISTRY`
- `ToolDispatcher.dispatch`
- `_execute_tool`
- `_status_for`
- `_annotate_fallback`

What it does:

- Checks whether the selected action is allowed.
- Falls back to `alert_engineer` if the action is not allowed.
- Executes the matching tool.
- Persists tool results to PostgreSQL.

Why it matters:

- This is a major safety layer.
- The Decider does not execute tools directly.
- Python controls what actually runs.

Current improvement opportunity:

- Add an explicit policy engine like the friend repo's `dataops_agent/policies.py`.
- This would make retry and transient-error rules clearer.

### 16. `dataops_agent/tools/base.py`

Review this before individual tools.

Important objects:

- `ToolResult`
- `success_result`
- `failure_result`
- `repository_payload`
- `ensure_json_object`
- `ensure_json_value`
- `incident_snapshot`

What it does:

- Defines the standard result format for all tools.
- Ensures tool results are JSON-safe.
- Builds payloads for persistence.

Why it matters:

- All tools should return the same shape.
- The dispatcher and dashboard depend on this consistency.

### 17. `dataops_agent/tools/csv_source.py`

Review this before schema/null tools.

What it does:

- Resolves CSV source locations.
- Reads CSV content from S3 or context.
- Parses sample rows.

Why it matters:

- `inspect_schema` and `profile_nulls` use this shared file.
- It keeps CSV reading logic out of individual tools.

### 18. `dataops_agent/tools/inspect_schema.py`

What it does:

- Infers CSV schema.
- Normalizes expected schema.
- Compares expected and observed columns/types.
- Returns missing columns, extra columns, and type drift.

Why it matters:

- This is the diagnostic tool for schema drift.
- It supports stronger type checking than the friend repo's local-file version.

### 19. `dataops_agent/tools/profile_nulls.py`

What it does:

- Reads CSV sample data.
- Counts nulls per column.
- Calculates null ratios.
- Flags threshold breaches.

Why it matters:

- This is the diagnostic tool for data quality/null-rate incidents.

### 20. `dataops_agent/tools/quarantine_batch.py`

What it does:

- Copies bad Bronze S3 objects into the Quarantine prefix.
- Does not delete or overwrite Bronze data.
- Uses S3 `copy_object`.
- Adds metadata and tags.
- Handles idempotency if the quarantine object already exists.

Why it matters:

- This is the safest remediation action for bad data.
- It preserves raw data immutability.

### 21. `dataops_agent/tools/retry_pipeline.py`

What it does:

- Checks previous retry attempts.
- Enforces retry limits.
- Builds a rerun config payload.

Why it matters:

- This protects against endless retry loops.
- It is useful for transient errors, not schema/data corruption.

### 22. `dataops_agent/tools/alert_engineer.py`

What it does:

- Builds a structured manual-review notification payload.
- Marks that human review is required.

Why it matters:

- This is the safe fallback when automation cannot confidently fix the issue.

### 23. `dataops_agent/bedrock_report.py`

Review this after tools.

Important parts:

- `SYSTEM_PROMPT`
- `BedrockReportGenerator`
- `generate_report`
- `_report_payload`
- `deterministic_report`

What it does:

- Sends verified incident and tool evidence to Amazon Bedrock.
- Asks for a structured Markdown incident report.
- Falls back to a local deterministic report if Bedrock fails.

Why it matters:

- Bedrock explains what happened.
- Bedrock does not choose actions here.
- The fallback makes the system demoable even when AWS is unavailable.

### 24. `dashboard/app.py`

Review this last in the current project.

What it does:

- Loads incidents from PostgreSQL.
- Shows incident metrics.
- Filters incidents.
- Displays schema evidence, null profiles, decisions, reports, and raw JSON.
- Allows manual actions from the dashboard.

Why it matters:

- This is the human-in-the-loop control center.
- It shows the final state of the whole system.

## Friend Project Review Order

The friend repo is better for presentation, tests, and packaging. Use it to learn what your current project is missing.

### 1. `friend-agentic-aws-dataops/README.md`

Review this first.

Why it matters:

- It explains the intended architecture clearly.
- It is strong hackathon documentation.
- Your current project should have a README like this.

Look for:

- Problem statement.
- Architecture diagram.
- End-to-end flow.
- Tool descriptions.
- Safety controls.
- Budget plan.
- Demo scenarios.

### 2. `friend-agentic-aws-dataops/pyproject.toml`

Review this after README.

What it does:

- Defines package metadata.
- Defines dependencies.
- Defines optional dev dependencies.
- Configures pytest.
- Configures Ruff.

Why it matters:

- This is better packaging than your current `requirements.txt` only approach.
- Your current project should add something similar.

### 3. `friend-agentic-aws-dataops/.env.example`

Review this to see what a shareable environment template looks like.

Why it matters:

- It documents required environment variables.
- It avoids committing real/local `.env` values.
- Your current project should add a clean `.env.example`.

### 4. `friend-agentic-aws-dataops/dagster_project/definitions.py`

Review this as the Dagster entrypoint.

What it does:

- Registers assets.
- Registers jobs.
- Registers sensors.
- Registers resources.

Why it matters:

- It is more explicit than your current `dagster_project/__init__.py`.
- It shows how Dagster resources are attached.

### 5. `friend-agentic-aws-dataops/dagster_project/resources.py`

This is one of the most useful friend repo files.

Important classes:

- `S3Resource`
- `RDSResource`

What it does:

- Wraps S3 operations in a Dagster resource.
- Wraps PostgreSQL operations in a Dagster resource.
- Provides helper methods for listing objects, downloading files, recording incidents, and fetching pending incidents.

Why it matters:

- It makes Dagster assets and sensors easier to test.
- It is useful for resource-based Dagster design.

Comparison:

- Friend repo uses Dagster resources.
- Your current repo uses centralized settings/client factories and a repository layer.
- Your current approach is stronger for backend structure.
- Friend approach is useful for Dagster integration patterns.

### 6. `friend-agentic-aws-dataops/dagster_project/sensors.py`

Review carefully.

Important sensors:

- `pending_incident_sensor`
- `s3_raw_bronze_sensor`

Why it matters:

- The S3 raw Bronze sensor is a feature your current project should add.
- It polls S3 for new CSV files.
- It tracks processed keys and ETags in the Dagster sensor cursor.
- It prevents duplicate ingestion runs.

Best feature to borrow:

- The S3 raw sensor cursor logic.

### 7. `friend-agentic-aws-dataops/dagster_project/assets.py`

Review this to compare validation logic.

What it does:

- Downloads Bronze CSV from S3 to local storage.
- Validates expected columns.
- Validates null rates.
- Writes valid data to local Silver and optionally S3.
- Records incidents through `RDSResource`.

Why it matters:

- It is easier to read than your current `assets.py`.
- It is less production-safe because it relies more on local file paths.

Comparison:

- Friend version is simpler and easier to understand.
- Your version is more robust and S3-first.

### 8. `friend-agentic-aws-dataops/dagster_project/jobs.py`

Review this to understand the friend agent workflow.

What it does:

- Defines the ingestion job.
- Defines the incident remediation job.
- Calls Decider for diagnosis.
- Dispatches diagnostic tool.
- Calls Decider for remediation.
- Dispatches remediation tool.
- Generates Bedrock report.
- Updates incident status.

Why it matters:

- It is the simple version of your orchestrator.
- It is easier for learning the basic idea.

Comparison:

- Friend repo puts much of the workflow directly in the Dagster job.
- Your repo separates the workflow into `dataops_agent/orchestrator.py`, which is cleaner.

### 9. `friend-agentic-aws-dataops/dataops_agent/incident_state.py`

Review this to compare state modeling.

What it does:

- Defines `IncidentStatus`.
- Defines `IncidentSeverity`.
- Defines `IncidentRecord`.

Why it matters:

- It is simpler than your ORM repository.
- It is good for understanding the data model.

Comparison:

- Friend repo has a simple Pydantic record.
- Your project has a full SQLAlchemy repository and lifecycle methods.

### 10. `friend-agentic-aws-dataops/dataops_agent/policies.py`

This is one of the best friend repo files to borrow from.

What it does:

- Defines allowlisted actions.
- Defines transient error types.
- Defines retry limits.
- Implements `PolicyEngine.validate_action`.

Why it matters:

- It clearly separates policy decisions from tool execution.
- Your dispatcher currently enforces allowlisting, but an explicit policy engine would make retry/transient rules clearer.

Best feature to borrow:

- A dedicated `PolicyEngine`.

### 11. `friend-agentic-aws-dataops/dataops_agent/dispatcher.py`

Review this after policies.

What it does:

- Runs policy validation.
- Applies policy override if needed.
- Executes matching tool.
- Returns dict result.

Why it matters:

- It shows a clean policy-before-tool pattern.

Comparison:

- Friend dispatcher has clearer policy logic.
- Your dispatcher has better persistence and typed result handling.

### 12. `friend-agentic-aws-dataops/dataops_agent/decider_client.py`

Review this to understand demo-friendly Decider behavior.

What it does:

- Calls a Decider endpoint if configured.
- Otherwise uses deterministic simulated logic.
- Chooses schema/null/retry/quarantine/alert actions based on incident context.

Why it matters:

- It is useful for demos without a live Decider service.

Comparison:

- Friend version is demo-friendly.
- Your version is stricter and more production-oriented.

### 13. `friend-agentic-aws-dataops/dataops_agent/bedrock_report.py`

Review this to compare Bedrock integration.

What it does:

- Builds a prompt.
- Calls Bedrock `invoke_model`.
- Falls back to local report text if Bedrock fails.

Comparison:

- Friend version is easier to read.
- Your version uses Bedrock Converse and stricter payload handling.

### 14. Friend Tools

Review these after the dispatcher:

- `friend-agentic-aws-dataops/dataops_agent/tools/inspect_schema.py`
- `friend-agentic-aws-dataops/dataops_agent/tools/profile_nulls.py`
- `friend-agentic-aws-dataops/dataops_agent/tools/quarantine.py`
- `friend-agentic-aws-dataops/dataops_agent/tools/retry_pipeline.py`
- `friend-agentic-aws-dataops/dataops_agent/tools/alert_engineer.py`

Why they matter:

- They are simple and easy to understand.
- They are good for learning the core behavior.

Limitations:

- They mostly work on local files.
- They do not enforce S3 Bronze immutability as strongly as your current project.
- They return plain dictionaries instead of typed `ToolResult` objects.

### 15. `friend-agentic-aws-dataops/dashboard/app.py`

Review this to compare dashboards.

What it does:

- Shows incidents.
- Shows metrics.
- Shows evidence JSON.
- Shows selected action and report.
- Includes seed scenario UI controls.

Why it matters:

- It is simpler and more presentation-focused.

Comparison:

- Friend dashboard looks more hackathon/demo oriented.
- Your dashboard is stronger for operational review and manual actions.

### 16. Friend Tests

Review every file in:

```text
friend-agentic-aws-dataops/tests/
```

Important files:

- `test_tools.py`
- `test_policies.py`
- `test_decider.py`
- `test_resources.py`
- `test_s3_raw_sensor.py`

Why they matter:

- This is one of the friend repo's biggest strengths.
- Your current project has no visible test suite.
- These tests are a good starting pattern for your project.

What to learn:

- How to test tools.
- How to test policy decisions.
- How to test Decider fallback behavior.
- How to test S3 resource behavior with mocks.
- How to test Dagster sensors and run requests.

## Similarities Between Both Projects

Both projects implement:

- Dagster orchestration.
- S3 Bronze/Silver/Quarantine concept.
- PostgreSQL incident tracking.
- Strands Decider action selection.
- Allowlisted tools.
- Bedrock reporting.
- Streamlit dashboard.
- Sample data scenarios.

Both use the same main tool actions:

- `inspect_schema`
- `profile_nulls`
- `quarantine_batch`
- `retry_pipeline`
- `alert_engineer`

## Main Differences

### Current Project Strengths

- More production-ready runtime structure.
- Strong Pydantic settings validation.
- Centralized AWS client creation.
- SQLAlchemy incident repository.
- Atomic incident claiming.
- Typed tool result contract.
- S3-first Bronze/Silver/Quarantine operations.
- Safer quarantine behavior.
- End-to-end orchestrator.
- Better dashboard filtering and manual actions.
- LocalStack and Docker setup already present.

### Current Project Weaknesses

- No README.
- No `pyproject.toml`.
- No `.env.example`.
- No tests.
- No raw S3 Bronze discovery sensor.
- `.env` exists directly in the repo.

### Friend Project Strengths

- Strong README.
- Has `pyproject.toml`.
- Has `.env.example`.
- Has tests.
- Has S3 raw Bronze sensor.
- Has explicit `PolicyEngine`.
- Easier to understand as a learning/demo project.

### Friend Project Weaknesses

- Less production-safe.
- More local-file based.
- Simpler incident lifecycle.
- Less defensive typing.
- Less robust persistence.
- Quarantine is local file copy, not S3-first.
- Core agent flow is inside Dagster job instead of a separate orchestrator.

## What To Continue With

Use the current project as the main base.

Reason:

- It has the stronger core implementation.
- It is closer to a real production architecture.
- It has better safety around S3, state, and tool execution.

Use the friend repo as a source for missing polish.

Borrow these ideas:

1. README structure.
2. `pyproject.toml`.
3. `.env.example`.
4. Test suite structure.
5. S3 raw Bronze sensor.
6. Explicit policy engine.
7. Hackathon/demo explanation style.

## Best Merge Plan

If you want to improve the current project, do this in order:

1. Add `README.md`.
2. Add `.env.example`.
3. Add `pyproject.toml`.
4. Add tests for tools.
5. Add tests for `IncidentRepository`.
6. Add tests for Decider fallback.
7. Add tests for Dagster assets.
8. Add tests for sensors.
9. Port S3 raw Bronze sensor from friend repo.
10. Add `PolicyEngine` to current project.
11. Update dashboard with optional demo controls.
12. Add a simple demo script or quickstart.

## Mental Model

Use this mental model while reviewing:

```text
Dagster detects data problem.
PostgreSQL stores the incident.
Sensor notices pending incident.
Orchestrator claims the incident.
Decider chooses what evidence to collect.
Dispatcher runs an allowlisted tool.
Decider chooses remediation.
Dispatcher runs remediation tool.
Bedrock explains the result.
Dashboard shows everything to the user.
```

This is the core story of the whole project.

