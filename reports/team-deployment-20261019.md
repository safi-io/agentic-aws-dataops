# AWS team deployment verification — 2026-10-19

- Stack: `agentic-dataops-team`, `us-east-1`
- Instance: `i-034bd6c9c26097b46`, `t3a.large`, encrypted 40 GB disk
- Team URL: https://44.196.49.100.sslip.io/
- Dagster: https://44.196.49.100.sslip.io/dagster/
- Dashboard: https://44.196.49.100.sslip.io/dashboard/

The login is in AWS Secrets Manager and the ignored local
`.deployment/team-access.txt`; it is not part of this report or the repository.

## Verified

- Local regression suite: **24 passed**. Existing Dagster deprecation warnings
  remain; no test called real AWS services.

- CloudFormation schema validation passed without findings, and the stack reached
  `CREATE_COMPLETE`.
- Source archive exclusion and configured-credential scans passed. The bundle
  excludes the unrelated project, local environment files, AWS credentials,
  datasets, logs, and temporary Dagster instances.
- Public TLS certificate verification passed. HTTP redirects to HTTPS.
- Anonymous and incorrect-password requests returned 401, including Dagster
  GraphQL and Streamlit health endpoints.
- Authenticated landing page, Dagster, dashboard, and health endpoints returned
  200. Dagster loaded only the project's repository.
- The Streamlit page rendered its actual incident table and charts through an
  authenticated WebSocket connection, without configuration or database errors.
- The documented source-update command completed through Systems Manager. The
  HTTP, authentication, repository, and dashboard checks passed again afterward.
- The remediation sensor remained stopped and SNS email delivery was disabled.
- The cloud Decider selected `inspect_schema` for a missing-column example with
  confidence `0.927` and `fallback=False`. This check selected an action without
  executing remediation or sending any alert. The command took approximately
  **7 minutes 3 seconds** on this server.

## Existing S3 batches tested on AWS

| Batch | Rows | Expected and observed | Run ID |
| --- | ---: | --- | --- |
| clean_large | 1,100 | SUCCESS | `2907456d-6615-4a9f-8b44-372f34f19634` |
| late_invalid_amount | 1,100 | FAILURE | `9b1c6dd5-0787-4a71-8957-c1379b025b57` |

The clean output was read and checked on AWS: 1,100 Parquet rows at
`silver/shared-demo/orders/clean_large-2907456d-6615-4a9f-8b44-372f34f19634.parquet`.
PostgreSQL contained exactly one matching incident, `schema_drift` with status
`pending`, for the broken run. There was no incident for the clean run. Dataset
contents were not downloaded to the PC during this deployment verification.

## Operational notes

The initial boot encountered a Caddy password-hashing command incompatibility.
The deployment now uses Python bcrypt and was successfully reapplied through
Systems Manager. The original cloud-init failure remains in historical logs;
the completed-bootstrap marker and active services confirm recovery.

The model downloaded successfully to AWS. Its CPU runtime expands the weights
to float32, exceeding comfortable capacity alongside the apps on an 8 GiB server.
A running inference used about 5.6 GB resident memory plus approximately 2.5 GB
of system swap. The web apps remained responsive, but AI diagnosis is slow on
this instance size. This deployment does not claim a latency guarantee for AI
remediation. See the team guide for cost and memory tradeoffs.

Server compute was quoted by AWS Pricing at $0.0752/hour, approximately $54.90
for 730 hours. Disk, IPv4, Secrets Manager, transfer, S3 and Bedrock usage, and
existing RDS costs are additional. No automatic shutdown schedule was created.
