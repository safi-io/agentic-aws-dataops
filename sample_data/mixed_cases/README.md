# Mixed orders fixtures

These are synthetic orders for exercising the project's schema and null checks.
`manifest.json` records the expected outcome for each CSV.

| Expected result | Cases |
|---|---|
| Accept | Clean orders, reordered columns, 10% nulls, exactly 15% nulls, 1,100 clean orders |
| Reject | 20% nulls, missing currency, extra column, invalid amount, invalid date, mixed bad rows, bad amount after row 1,000, headers only, empty file, malformed CSV |

The null limit is applied per column. Exactly 15% is allowed; more than 15% fails.
Validation rejects an entire broken batch; it does not remove individual bad rows
or fill missing values. Successful output preserves the original rows.

Run `python scripts/mixed_data_demo.py` from the project root for a local preview.
Run `python scripts/mixed_data_demo.py --live --env-file .env.aws.local` for real
S3/PostgreSQL execution and a saved report. Live runs add unique test objects and
incidents. The runner executes the registered Dagster job sequentially in one
process and stores runs in `.dagster_home/`. It does not invoke remediation;
an enabled remediation sensor can process the pending incidents afterward.

These cases check the existing schema/null contract. They do not establish rules
for unique order IDs, allowed currency/status values, or positive amounts.
