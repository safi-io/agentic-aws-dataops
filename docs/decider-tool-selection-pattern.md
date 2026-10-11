# Strands Decider 2B: Bounded Tool Selection Pattern

**Hackathon Tags**: `#strands-decider-2b` `#tool-selection`

## 1. Overview and Core Philosophy

In traditional AI agent architectures, large foundation models are often prompted in free-form natural language to generate tool calls or write arbitrary executable scripts. While flexible, this approach introduces non-determinism, hallucinatory tool invocations, security vulnerabilities, and unpredictable latency and cost—especially within mission-critical data engineering pipelines.

**Agentic AWS DataOps** adopts a strict separation of concerns:
- **Decider Chooses**: **Strands Decider 2B** operates over a closed set of allowlisted, typed actions based on structured evidence.
- **Python Dispatches & Enforces**: A deterministic dispatcher validates arguments, applies policy rules (e.g., maximum retry bounds, schema protection), and executes tested tool functions.
- **Bedrock Explains**: Amazon Bedrock translates confirmed evidence and execution outcomes into executive-ready incident reports.

```
+-------------------------------------------------------------+
|                     Incident Detected                       |
|           (Schema mismatch, null breach, I/O error)         |
+-------------------------------------------------------------+
                               |
                               v
               +-------------------------------+
               |    Decision 1: Diagnosis      |
               |     (Strands Decider 2B)      |
               +-------------------------------+
                               |
                               v
               +-------------------------------+
               |    Diagnostic Tool Executed   |
               | (inspect_schema/profile_nulls)|
               +-------------------------------+
                               |
                               v
               +-------------------------------+
               |     Decision 2: Remediation   |
               |     (Strands Decider 2B)      |
               +-------------------------------+
                               |
                               v
               +-------------------------------+
               |     Deterministic Policy      |
               |      Check & Dispatcher       |
               +-------------------------------+
                               |
                               v
               +-------------------------------+
               |    Response Action Executed   |
               | (quarantine / retry / alert)  |
               +-------------------------------+
                               |
                               v
               +-------------------------------+
               |   Amazon Bedrock Generates    |
               |     Audit Incident Report     |
               +-------------------------------+
```

---

## 2. Decision Formulation & Typed Choices

### Allowlisted Action Choices

The decision space is explicitly bounded to five allowlisted operations:

```python
CHOICES = [
    "inspect_schema",
    "profile_nulls",
    "quarantine_batch",
    "retry_pipeline",
    "alert_engineer",
]
```

### Two-Stage Decision Lifecycle: Diagnosis Before Remediation

Rather than attempting to guess a remediation immediately upon receiving an opaque error, the pattern enforces a 2-stage lifecycle:

1. **Diagnostic Stage (Decision 1)**:
   - Identify what diagnostic evidence is needed to clarify the failure.
   - Example choices: `["inspect_schema", "profile_nulls", "alert_engineer"]`
2. **Remediation Stage (Decision 2)**:
   - Evaluates the structured diagnostic output.
   - Example choices: `["quarantine_batch", "retry_pipeline", "alert_engineer"]`

---

## 3. Context & Prompt Structure

Strands Decider 2B receives a typed prompt containing:
1. Incident identity and failure classification.
2. Observed symptoms and raw error extracts.
3. Available evidence collected so far.
4. Specific question and the allowlisted choices.

### Example Diagnostic Prompt (Decision 1)

```text
Incident ID: inc-20261011-8f9a
Pipeline: orders_ingestion_pipeline
Failure Type: SchemaValidationFailure
Symptom: Incoming batch has 4 columns; expected contract specifies 3 columns.
Observed Columns: order_id, customer_id, amount, customer_segment

Question:
Which diagnostic action should be executed first to assess the discrepancy?

Choices:
- inspect_schema
- profile_nulls
- alert_engineer
```

**Decider Output**:
```json
{
  "selected_action": "inspect_schema",
  "confidence": 0.96,
  "rationale": "Columns mismatch detected; contract comparison required."
}
```

### Example Remediation Prompt (Decision 2)

```text
Incident ID: inc-20261011-8f9a
Diagnostic Output:
- Missing required columns: None
- Unexpected columns: ['customer_segment']
- Contract status: Unapproved schema drift. Downstream compatibility unknown.
- Retry count: 0

Question:
Given unapproved schema drift with unknown downstream compatibility, which recovery action should be taken?

Choices:
- quarantine_batch
- retry_pipeline
- alert_engineer
```

**Decider Output**:
```json
{
  "selected_action": "quarantine_batch",
  "confidence": 0.94,
  "rationale": "Unapproved schema drift cannot be resolved by retrying; batch must be isolated."
}
```

---

## 4. Safety Guardrails & Policy Enforcement

Even if Strands Decider 2B chooses an action, **the Python dispatcher never executes it blindly**. A deterministic policy engine sits between the model and tool execution:

1. **Allowlist Verification**: Any action not in `CHOICES` is instantly rejected and escalated to `alert_engineer`.
2. **Idempotency & Retry Limits**: If `retry_pipeline` is selected but `retry_count >= MAX_RETRIES` (default: 2), the policy engine overrides the decision and triggers `alert_engineer`.
3. **Bronze Immutability**: The source file in `s3://.../bronze/` is strictly read-only. Quarantining copies or isolates data to `s3://.../quarantine/` without deleting or modifying raw ingestion files.
4. **Human Review for Risky Actions**: High-severity data corruption or unknown schema drifts force an incident status of `needs_review`.
5. **Outcome Verification**: The incident status cannot transition to `resolved` unless the tool returns a verified success payload (`status == "success"`).

---

## 5. Amazon Bedrock Integration

Once the action completes, Amazon Bedrock receives the full telemetry:
- Raw incident facts.
- Diagnostic findings.
- Decider's selected action & confidence score.
- Actual tool execution payload and return code.

Bedrock's job is **explanation, root-cause synthesis, and actionable recommendations**, guaranteeing that reports reflect what actually occurred rather than model speculation.

