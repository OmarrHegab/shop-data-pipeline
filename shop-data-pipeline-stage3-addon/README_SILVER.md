# Stage 3: bronze to silver

This adds a Spark batch job that reads the bronze Parquet records you created in
stage 2 and produces clean, typed orders. Keep your existing stage 1 and stage 2
folders. Extract this ZIP into a sibling folder named exactly
`shop-data-pipeline-stage3-addon` inside your main `shop-data-pipeline` folder.

## Folder layout

Your main folder should contain these three sibling folders:

| Folder | Contents |
| --- | --- |
| shop-data-pipeline-stage1 | compose.yaml, producer, data, checkpoints |
| shop-data-pipeline-stage2-addon | compose.spark.yaml, spark |
| shop-data-pipeline-stage3-addon | compose.silver.yaml, spark, README_SILVER.md |

Inside the new folder, `compose.silver.yaml` and `spark` must be directly visible;
avoid an extra nested copy of the same folder.

All data stays under stage 1's `data` folder. Docker Compose resolves relative
paths from the first Compose file. The new silver service therefore mounts
`./data` from stage 1 and the scripts from the sibling stage 3 folder.
Your stage 2 script mount should still be
`../shop-data-pipeline-stage2-addon/spark:/opt/project/spark:ro`.

## Run in PowerShell

Open Docker Desktop and run these commands from the main folder. Finish the
stage 2 ingestion job before running silver. No new Python installation or image
download is needed if you already ran stage 2 successfully.

```powershell
cd "C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline"
```

Validate the combined configuration. Successful validation produces no output:

```powershell
docker compose -f .\shop-data-pipeline-stage1\compose.yaml -f .\shop-data-pipeline-stage2-addon\compose.spark.yaml -f .\shop-data-pipeline-stage3-addon\compose.silver.yaml config --quiet
```

Build silver:

```powershell
docker compose -f .\shop-data-pipeline-stage1\compose.yaml -f .\shop-data-pipeline-stage2-addon\compose.spark.yaml -f .\shop-data-pipeline-stage3-addon\compose.silver.yaml run --rm silver
```

Verify the saved result:

```powershell
docker compose -f .\shop-data-pipeline-stage1\compose.yaml -f .\shop-data-pipeline-stage2-addon\compose.spark.yaml -f .\shop-data-pipeline-stage3-addon\compose.silver.yaml run --rm silver /opt/project/silver-jobs/inspect_silver.py --expected-count 10
```

For the original 10 orders you shared, expect:

```text
INPUT ROWS: 10
CLEAN ORDERS: 10
REJECTED ROWS: 0
DUPLICATE ROWS: 0
TOTAL ORDER VALUE EUR: 1663.10
```

The build ends with `SILVER BUILD COMPLETE`. The inspector also prints the schema
and orders, then ends with `SILVER VERIFICATION PASSED`.
If you generate more orders, change `--expected-count` to your expected number
of distinct valid orders, or omit it to run the other verification checks.

## What the files do

| File | Purpose |
| --- | --- |
| compose.silver.yaml | Adds a one-off Spark service named silver using the same Spark image as stage 2. |
| spark/bronze_to_silver.py | Parses, validates, deduplicates, calculates totals, and publishes a verified snapshot. |
| spark/inspect_silver.py | Reads the published snapshot and checks counts, unique IDs, decimal money, and totals. |

Bronze preserves the original messages and Kafka positions. Silver is the first
business-facing table: one row per valid completed order, with real integer,
decimal, timestamp, and date columns. `order_total_eur` is quantity multiplied by
unit price. Money uses decimals with two places. Timestamps and event dates use
UTC, and orders are partitioned by event date.

## Validation and duplicate policy

- Required IDs, product name, and country must contain text; outer spaces are trimmed.
- Quantity must parse as a positive integer.
- Price must be positive, fit the decimal type, and use plain decimal notation
  with at most two decimal places. Excess precision is rejected rather than rounded.
- Event time must parse as a timestamp and explicitly include `Z` or a numeric timezone offset.
- Schema version must be 1.
- Identical business details for the same order ID produce one clean order.
  Extra copies are retained in the duplicate audit output.
- Different business details for the same order ID are rejected as conflicts.
  An event ID used for multiple order IDs is also rejected.

This demo treats an order as an immutable completed order. Supporting order
updates, cancellations, and refunds requires a different business model later.
Rejected and duplicate outputs retain the original JSON and Kafka trace columns.
Rejected rows include an array of reasons so you can inspect and fix the source.
Every input row is accounted for as clean, rejected, or duplicate.

## Saved data and safe reruns

The output is under `shop-data-pipeline-stage1/data/silver`:

- `current.json` identifies the current complete snapshot.
- `runs/<run-id>/orders` contains clean Parquet orders.
- `runs/<run-id>/rejected` contains rejected records.
- `runs/<run-id>/duplicates` contains duplicate audit records.
- `runs/<run-id>/quality.json` contains counts and the total order value.

Each run recomputes the entire available bronze table and writes a fresh snapshot.
It checks the saved counts before replacing `current.json`. The inspector reads
only that snapshot. A rerun with unchanged bronze data therefore still shows
10 clean orders rather than 20. Earlier snapshots remain available for inspection
and consume disk space. Readers should follow `current.json`; reading every run
folder together would include historical copies.

Stage 2's checkpoint prevents re-reading the same Kafka positions. Silver's
order-ID rule handles the separate case where the producer publishes the same
business order again at another Kafka offset.

This full-table approach is intentionally small and easy to inspect. It uses one
writer and a local filesystem; it is not an incremental lakehouse or a distributed
transaction protocol. Run silver after bronze finishes. Later, Airflow will
schedule these steps in that order.

## Next milestones

After silver verification: build gold analytics, load queryable tables into
PostgreSQL, orchestrate the steps with Airflow, and show results in a dashboard.
Keep code in GitHub Desktop; exclude generated data and checkpoints from commits.
