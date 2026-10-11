# Agentic AWS DataOps

> **Automated, evidence-driven data pipeline incident response powered by Dagster, Strands Decider 2B, and Amazon Bedrock.**

**Hackathon Tags:** `#strands-decider-2b` `#tool-selection`

---

## 1. Project Overview

Traditional data pipelines usually halt abruptly when they encounter unexpected data or a processing anomaly. A data engineer is typically paged to inspect cryptic logs, identify the root cause, determine a recovery strategy, and manually re-run or quarantine the batch.

**Agentic AWS DataOps** automates this incident-handling lifecycle while maintaining **deterministic safety checks, allowlisted tools, and human oversight** for risky operations.

### Core Architecture Concept
- **Orchestration**: [Dagster](https://dagster.io/) orchestrates the data pipeline and coordinates incident response workflows.
- **Data Lake Storage**: **Amazon S3** stores raw immutable inputs (`bronze/`), validated clean data (`silver/`), and isolated batches (`quarantine/`).
- **Validation & Transformation**: **pandas** performs fast, reproducible CSV schema checks, null rate profiling, and transformations.
- **Incident Persistence**: **PostgreSQL** maintains stateful, durable incident records across lifecycle states (`pending` → `processing` → `resolved` / `needs_review`).
- **Action Selection**: **Strands Decider 2B** selects the appropriate diagnostic or remediation action from predefined typed options.
- **Controlled Execution**: A **Python Tool Dispatcher** enforces safety policies, limits retries, and invokes allowlisted functions.
- **Natural Language Reporting**: **Amazon Bedrock** synthesizes verified tool results and diagnostic evidence into executive-ready incident reports.
- **Observability**: **Streamlit** provides an interactive control center dashboard for tracking pipeline health and agent decisions.
- **Cost Conscious**: Designed within an approximate **$100 AWS Credit Budget**, prioritizing local development and serverless AWS primitives.

---

## 2. The Problem We Are Solving

Consider an ingestion pipeline processing customer order batches from CSV files with the following contract:

```text
order_id, customer_id, amount
```

In real-world data lakes, data corruption and schema drift frequently occur:
1. **Schema Drift**: An upstream service introduces a new column (`customer_segment`) without contract pre-approval.
2. **Data Quality Breaches**: Missing required columns or excessive null rates (e.g. >15% missing customer identifiers).
3. **Data Type Corruption**: Unparseable amounts or malformed numeric fields.
4. **Transient Storage Failures**: Temporary network drops or S3 `503 SlowDown` rate limits.

Rather than blindly running unconstrained code or failing silently, **Agentic AWS DataOps**:
1. **Detects** the failure immediately during validation.
2. **Records** structured evidence in PostgreSQL.
3. **Diagnoses** the issue using allowlisted Python tools.
4. **Decides** the next recovery step using **Strands Decider 2B**.
5. **Enforces** deterministic safety policies before any action runs.
6. **Explains** the root cause and outcome using **Amazon Bedrock**.
7. **Presents** full auditability in a Streamlit dashboard.

---

## 3. Architecture

```mermaid
flowchart TD
    subgraph Storage ["Amazon S3 / Local Storage"]
        Bronze["s3://.../bronze (Raw CSV - Immutable)"]
        Silver["s3://.../silver (Validated Parquet)"]
        Quarantine["s3://.../quarantine (Isolated Batches)"]
    end

    subgraph Pipeline ["Dagster Orchestration"]
        RawSensor["s3_raw_bronze_sensor"]
        Ingest["orders_bronze_data Asset"]
        Validate["orders_validated_silver Asset"]
        IncSensor["pending_incident_sensor"]
        RemJob["incident_remediation_job"]
    end

    subgraph State ["Database (Amazon RDS / PostgreSQL)"]
        IncTable[("dataops_incidents\n(pending, processing, resolved)")]
    end

    subgraph Agent ["Agent Decision & Execution Engine"]
        Decider["Strands Decider 2B\n(Typed Choice Selection)"]
        Policy["Deterministic Policy Engine\n(Guardrails & Retry Bounds)"]
        Dispatcher["Python Tool Dispatcher"]
        subgraph Tools ["Allowlisted Tools"]
            T1["inspect_schema"]
            T2["profile_nulls"]
            T3["quarantine_batch"]
            T4["retry_pipeline"]
            T5["alert_engineer"]
        end
    end

    subgraph Explanation ["Amazon Bedrock"]
        Bedrock["Bedrock LLM\n(Incident Explanations & Next Steps)"]
    end

    subgraph UI ["Streamlit Dashboard"]
        Dashboard["Incident Control Center & Audit Trail"]
    end

    Bronze -. Polls for new files .-> RawSensor
    RawSensor -- Triggers pipeline -- Ingest --> Validate
    Validate -- Valid batch --> Silver
    Validate -- Failure detected --> IncTable

    IncTable -. Status: pending .-> IncSensor
    IncSensor --> RemJob
    RemJob --> Decider
    Decider --> Policy --> Dispatcher --> Tools
    Tools --> IncTable
    Tools -. Evidence & Results .-> Bedrock
    Bedrock --> IncTable
    IncTable --> Dashboard
```

---

## 4. Complete End-to-End Flow

1. **Upload CSV to S3 Bronze**: The original file lands in `bronze/`. Raw inputs are strictly immutable.
2. **Dagster Starts Pipeline**: Ingestion triggers data validation assets.
3. **Validation & Quality Checks**: pandas verifies column names, required fields, and null thresholds.
   - *If Clean*: Publishes to `silver/`.
   - *If Faulty*: Halts ingestion, prevents dirty writes to Silver, and logs an incident to PostgreSQL.
4. **Record Incident in PostgreSQL**: A structured record is written with an incident ID, pipeline name, source URI, error type, and status `pending`.
5. **Sensor Detects Incident**: A Dagster sensor detects pending rows and acquires an atomic lock to status `processing`.
6. **State Preparation**: Diagnostic context (error summaries, schema differences, null rates) is assembled.
7. **Strands Decider 2B (Diagnosis)**: Decider selects the appropriate diagnostic tool (e.g. `inspect_schema`).
8. **Diagnostic Execution**: The Python dispatcher runs the tool and gathers structured findings.
9. **Strands Decider 2B (Remediation)**: Decider evaluates diagnostic findings and chooses an allowable recovery action (e.g. `quarantine_batch`).
10. **Policy Validation & Action**: The policy engine confirms retry limits and safety rules, executes the tool, and preserves original inputs.
11. **Amazon Bedrock Report Generation**: Bedrock synthesizes the confirmed facts into an audit-ready report.
12. **Incident Closure & Display**: Status updates to `resolved` or `needs_review`, displayed live in the Streamlit dashboard.

---

## 5. Technology Stack & Responsibilities

| Technology | Responsibility | Core Philosophy |
| :--- | :--- | :--- |
| **Amazon S3** | Stores raw (`bronze`), clean (`silver`), and quarantined data | Raw Bronze data is immutable; zero dirty data reaches Silver. |
| **Dagster** | Orchestrates pipelines, data quality assets, and sensors | Deterministic scheduling, atomic run keys, and sensor deduplication. |
| **pandas** | Performs schema checks, null calculations, and transformations | Lightweight, standard data manipulation. |
| **PostgreSQL** | Maintains durable incident records and state lifecycle | Atomic row locking prevents duplicate agent investigations. |
| **Strands Decider 2B** | Selects actions from predefined, typed choices | **Decider chooses**: operates over closed allowlists, not arbitrary code. |
| **Python Dispatcher** | Validates decisions and executes allowlisted functions | **Python executes**: deterministic guardrails and retry limits. |
| **Amazon Bedrock** | Generates incident reports and next steps | **Bedrock explains**: converts verified tool telemetry into readable reports. |
| **Streamlit** | Displays real-time incidents, timelines, and audit records | Complete visibility into agent reasoning and tool outcomes. |

---

## 6. Strands Decider 2B: Decision Architecture

The decision engine operates on strict, typed choices:

```python
CHOICES = [
    "inspect_schema",
    "profile_nulls",
    "quarantine_batch",
    "retry_pipeline",
    "alert_engineer",
]
```

### Two-Stage Decision Lifecycle

```
[Incident Context] -> Decision 1 (Diagnostic Selection) -> Execute Diagnostic Tool
        -> Diagnostic Evidence -> Decision 2 (Remediation Selection) -> Policy Check & Action
```

- **Decision 1 (Diagnosis)**: Decider identifies what additional evidence is required (`inspect_schema` vs `profile_nulls`).
- **Tool Execution**: Diagnostic tool runs and outputs structured metrics.
- **Decision 2 (Remediation)**: Decider evaluates the metrics to select a response (`quarantine_batch`, `retry_pipeline`, `alert_engineer`).
- **Policy Guardrail**: Deterministic Python rules verify safety invariants before execution.

---

## 7. Allowlisted Python Tools

1. **`inspect_schema`** (`dataops_agent/tools/inspect_schema.py`):
   - Compares observed CSV headers and types against data contracts.
   - Outputs: `missing_columns`, `unexpected_columns`, `schema_valid`.
2. **`profile_nulls`** (`dataops_agent/tools/profile_nulls.py`):
   - Computes per-column null counts and percentages.
   - Detects breaches exceeding the 15% threshold.
3. **`quarantine_batch`** (`dataops_agent/tools/quarantine.py`):
   - Copies faulty batches to `quarantine/` with incident timestamps.
   - Confirms byte counts and destination URIs without deleting Bronze data.
4. **`retry_pipeline`** (`dataops_agent/tools/retry_pipeline.py`):
   - Retries transient failures with exponential backoff.
   - Enforces a hard cap (`max_retries = 2`) with duplicate execution protection.
5. **`alert_engineer`** (`dataops_agent/tools/alert_engineer.py`):
   - Dispatches notifications and transitions status to `needs_review`.

---

## 8. Amazon Bedrock Reporting

Amazon Bedrock converts structured incident telemetry into a formal report:
- **Incident Summary**: Context and pipeline name.
- **Probable Cause**: Factual discrepancy identified.
- **Evidence Gathered**: Column diffs, null rates, log excerpts.
- **Action Taken & Confirmed Outcome**: Tool return values and destination paths.
- **Remaining Uncertainty**: Downstream impact considerations.
- **Recommended Next Steps**: Contract updates or manual actions.

> **Integrity Rule**: Bedrock reports only what tools explicitly return. If a tool fails, the report explicitly states the failure.

---

## 9. PostgreSQL Incident Lifecycle

```sql
CREATE TABLE dataops_incidents (
    incident_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    dagster_run_id TEXT,
    source_uri TEXT,
    incident_type TEXT NOT NULL,
    severity TEXT DEFAULT 'medium',
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

**State Transitions**:
- `pending` → `processing` (Atomic sensor acquisition)
- `processing` → `resolved` (Automatic mitigation verified)
- `processing` → `needs_review` (Manual intervention required or policy overridden)

---

## 10. AWS Architecture & $100 Budget Plan

| AWS Service | Planning Budget | Strategy for Cost Optimization |
| :--- | :--- | :--- |
| **Amazon Bedrock** | $20 | Use Claude 3 Haiku / Nova Micro; concise prompts; test locally with fallback. |
| **Amazon S3** | $3 | Low storage footprint; small test CSV files. |
| **Amazon RDS (Optional)** | $25 | Local PostgreSQL used during development; provision RDS only for hosted demos. |
| **Amazon EC2 (Optional)** | $25 | Run Dagster & Streamlit locally; provision single t3.medium if hosting remotely. |
| **CloudWatch & Misc** | $7 | Minimal retention logging; no NAT gateways. |
| **Contingency Reserve** | $20 | Buffer for token spikes or repeated scenario runs. |
| **Total Planning Cap** | **$100** | Monitored via AWS Budgets alerts at 50%, 80%, and 100%. |

---

## 11. Safety and Reliability Controls

- **Raw Bronze Immutability**: Source files are never modified or deleted.
- **Data Isolation**: Unvalidated or corrupted batches never reach Silver storage.
- **Untrusted Input Sanitation**: CSV inputs and logs are sanitized before prompting.
- **Allowlisted Execution**: Only registered Python functions can be triggered.
- **Bounded Retries**: Retries are capped at 2 attempts for transient errors only.
- **Human Oversight**: Unresolved drift or repeated failures trigger `needs_review`.
- **Policy Precedence**: Deterministic policies override model selections when invariants are breached.

---

## 12. Repository Structure

```
agentic-aws-dataops/
├── README.md                           # Main documentation & architecture guide
├── pyproject.toml                      # Project metadata, dependencies & tool configs
├── .env.example                        # Environment variable configuration template
├── .gitignore                          # Git ignore rules
├── dagster_project/                    # Dagster orchestration package
│   ├── __init__.py
│   ├── definitions.py                  # Dagster entrypoint (assets, jobs, sensors, resources)
│   ├── resources.py                    # Reusable S3Resource & RDSResource
│   ├── assets.py                       # Bronze raw ingestion & Silver validation assets
│   ├── jobs.py                         # Ingestion pipeline & incident remediation jobs
│   └── sensors.py                      # Raw Bronze S3 sensor & PostgreSQL incident sensor
├── dataops_agent/                      # Core agent logic & tools
│   ├── __init__.py
│   ├── incident_state.py               # Incident models & status lifecycle
│   ├── decider_client.py               # Strands Decider 2B typed selection client
│   ├── dispatcher.py                   # Python tool dispatcher with policy checks
│   ├── policies.py                     # Deterministic guardrails & retry limits
│   ├── bedrock_report.py               # Amazon Bedrock incident report generator
│   └── tools/                          # Allowlisted diagnostic & recovery tools
│       ├── __init__.py
│       ├── inspect_schema.py           # Schema inspection tool
│       ├── profile_nulls.py            # Null rate profiler
│       ├── quarantine.py               # Batch quarantine tool
│       ├── retry_pipeline.py           # Bounded retry tool
│       └── alert_engineer.py           # Engineer escalation tool
├── dashboard/                          # Real-time incident dashboard
│   └── app.py                          # Streamlit UI
├── sql/                                # Database schemas & triggers
│   └── create_incidents.sql            # PostgreSQL schema script
├── sample_data/                        # Seeded test datasets
│   ├── clean.csv                       # Standard valid batch
│   ├── unexpected_column.csv           # Schema drift scenario
│   └── high_null_rate.csv              # Quality threshold violation scenario
├── tests/                              # Unit & integration tests
│   ├── __init__.py
│   ├── test_resources.py               # S3 & RDS resource configuration tests
│   ├── test_s3_raw_sensor.py           # Raw Bronze S3 discovery & deduplication tests
│   ├── test_tools.py                   # Tool execution tests
│   ├── test_decider.py                 # Strands Decider selection tests
│   └── test_policies.py                # Guardrail & policy tests
└── docs/                               # In-depth architectural documentation
    └── decider-tool-selection-pattern.md # Recipe for bounded decision-making
```

---

## 13. Seeded Scenarios & Expected Outcomes

| Scenario | Input File | Expected Diagnostic | Decider Remediation | Result |
| :--- | :--- | :--- | :--- | :--- |
| **Clean Batch** | `clean.csv` | Schema & null checks pass | None (Pipeline succeeds) | Data written to Silver |
| **Schema Drift** | `unexpected_column.csv` | `inspect_schema` detects `customer_segment` | `quarantine_batch` | Batch isolated, Bedrock report created |
| **High Null Rate** | `high_null_rate.csv` | `profile_nulls` flags 50%+ missing values | `quarantine_batch` | Batch isolated, quality breach logged |
| **Transient Error** | Simulated S3 503 | Diagnostic classifies error as transient | `retry_pipeline` | Retried within retry limit |
| **Repeated Failure** | Persistent transient | Policy engine flags retry limit (2/2) | `alert_engineer` | Status set to `needs_review` |

---

## 14. Quickstart Guide

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/safi-io/agentic-aws-dataops.git
cd agentic-aws-dataops

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your AWS credentials and PostgreSQL database connection
```

### 3. Initialize the Database

```bash
# Apply schema to your PostgreSQL / Amazon RDS instance
psql -h localhost -U postgres -d dataops_db -f sql/create_incidents.sql
```

### 4. Run Test Suite

```bash
pytest tests/ -v
```

### 5. Launch Dagster Orchestration

```bash
dagster dev -f dagster_project/definitions.py
```
*Access the Dagster UI at `http://localhost:3000`.*

### 6. Launch Streamlit Control Center

```bash
streamlit run dashboard/app.py
```
*Access the dashboard at `http://localhost:8501`.*

---

## 15. Hackathon Deliverables Checklist

- [x] Full source code repository with modular structure and typed decision patterns.
- [x] Reproducible sample datasets (`clean.csv`, `unexpected_column.csv`, `high_null_rate.csv`).
- [x] Strands Decider 2B decision recipe (`docs/decider-tool-selection-pattern.md`).
- [x] Comprehensive README detailing problem, architecture, decision design, and evaluation.
- [x] Unit test suite covering tools, policies, and decider workflows.
- [x] Streamlit monitoring dashboard and Dagster pipeline definitions.

