# Manually test the existing S3 data

The test CSVs already exist in bucket
`agentic-dataops-731451715213-us-east-1`, under:

```text
bronze/mixed-tests/20261018T205950Z-2afc5252/
```

| File | Rows | Expected result |
|---|---:|---|
| `clean_large.csv` | 1,100 | Success; a new Parquet output in S3 Silver |
| `late_invalid_amount.csv` | 1,100 | Failure; one pending schema-drift incident; no Silver output |

The broken file contains an invalid amount in data row 1,051. The validator
checks every row, so a problem late in the file is still detected.

Verified on 2026-10-19: the clean file succeeded and the broken file created one
schema-drift incident with no Silver output. The test process's peak resident
memory was 242.3 MiB on this PC, excluding the already-running web apps. The
DataFrames occupied 0.200 and 0.249 MiB respectively. Existing on-disk asset
intermediates were unchanged.

- [Verified clean run](http://127.0.0.1:3000/runs/38f17859-c22d-4714-aa73-91dfbb89d26e)
- [Verified broken run](http://127.0.0.1:3000/runs/d82b6a05-3323-491e-827a-3031d6b812c2)

## Repeat a test

Open PowerShell in the project folder. Run either command, and wait for it to
finish before starting another:

```powershell
python scripts/retest_s3_data.py --case clean
python scripts/retest_s3_data.py --case broken
```

To run both sequentially:

```powershell
python scripts/retest_s3_data.py --case both
```

The helper uses `.env.aws.local`. It reads the existing CSV directly from S3,
uses the registered `orders_data_quality_job` with an in-memory IO manager, and
verifies the run result, S3 output, and PostgreSQL incident. It neither creates
new source CSVs nor saves CSV/DataFrame intermediates on the PC's disk. Source
objects are left intact. Successful outputs use a unique `silver/manual-retests/`
prefix; each broken test creates a new incident.

`PASS broken: FAILURE` means the test passed: rejection was expected. The helper
returns exit code 0 when actual results match expectations, including intentional
data failures. Every invocation prints a clickable Dagster run URL.

## View the results

1. Open <http://127.0.0.1:3000/runs> and select the run URL printed by the command.
2. For the clean case, check that both assets succeeded. The console prints the
   exact Silver S3 URI.
3. For the broken case, check the `silver_orders` failure. Open
   <http://127.0.0.1:8501>, click **Refresh**, filter **Status** to **pending**,
   and search for `late_invalid_amount.csv`.

The apps can be started with the commands in [README.md](../README.md).
The CLI test can also execute without the web apps running, reducing their
background memory usage; start them afterward to inspect saved run metadata.

## Memory and incoming data

S3 stores the files. The current Python/Dagster process still runs on this PC and
temporarily holds one batch in RAM. The helper prints the DataFrame's measured
size; that is **not total process memory**, which also includes Python, Dagster,
pandas, parsing buffers, and Parquet serialization. Dagster keeps run metadata and
logs locally, but this helper keeps asset data out of local disk storage.

This helper uses one process and runs one batch at a time. The normal Dagster
Launchpad job uses its default IO manager, so use these helper commands when you
want to avoid local asset intermediates. Keep the remediation sensor off when
testing ingestion alone: the local Decider model is a separate, heavier workload.

For incoming data, use separate S3 files as small batches and process them one at
a time. Uploading a new file currently does not start the pipeline automatically:
the project has an incident-remediation sensor, but no S3 arrival sensor yet.
Neither S3 storage nor a streaming download makes processing memory-free. To
avoid processing on this PC entirely, the worker must be deployed to AWS compute.
