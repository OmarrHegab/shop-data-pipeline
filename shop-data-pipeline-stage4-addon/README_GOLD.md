# Stage 4: silver to gold analytics

Silver contains one valid completed order per row. Gold summarizes those rows
into business metrics that can be queried in PostgreSQL and displayed in a
dashboard later. This stage uses the existing Spark image and reads Parquet.

## Extract beside the other stages

Inside your main `shop-data-pipeline` repository, keep four sibling folders:

| Folder | Purpose |
| --- | --- |
| shop-data-pipeline-stage1 | Kafka, producer, generated data, checkpoints |
| shop-data-pipeline-stage2-addon | Kafka to bronze jobs |
| shop-data-pipeline-stage3-addon | Bronze to silver jobs |
| shop-data-pipeline-stage4-addon | Silver to gold jobs |

Inside stage 4, directly place `compose.gold.yaml`, `spark`, and this README.
Avoid an extra nested copy of the stage 4 folder. Leave the existing stages in
place. All data stays inside stage 1's `data` folder.

## What gold creates

| Table | One row represents | Grouping columns |
| --- | --- | --- |
| daily_sales | One UTC day | event_date |
| country_sales | One country on one UTC day | event_date, country |
| product_sales | One product ID/name on one UTC day | event_date, product_id, product_name |

Every table contains these metrics:

| Column | Meaning |
| --- | --- |
| order_count | Number of clean orders in that group |
| units_sold | Sum of quantity |
| unique_customers | Distinct customer IDs inside that group |
| revenue_eur | Sum of quantity multiplied by unit price |
| average_order_value_eur | Group revenue divided by order count, rounded to cents |

Here, revenue means the value of our generated completed orders. Taxes,
discounts, returns, and payment settlement are outside this data model.
Money remains decimal throughout. Days use UTC, as in silver. Each demo order
contains a single product. Product name is part of the product grouping key;
a renamed product can therefore appear as another row for the same product ID.

Order counts, quantities, and revenue can be summed across the groups within
any one table. Do not add totals from all three tables together: they are three
views of the same orders. Distinct customer counts are not additive across days,
countries, or products because a customer can appear in more than one group.
To combine average order values, divide total revenue by total orders; do not
take an unweighted average of group averages.

## Run in PowerShell

Open Docker Desktop. Finish silver before running gold. From the main repository
folder, define the file list again so this works in a fresh PowerShell window:

```powershell
cd "C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline"

$composeFiles = @(
    "-f", ".\shop-data-pipeline-stage1\compose.yaml",
    "-f", ".\shop-data-pipeline-stage2-addon\compose.spark.yaml",
    "-f", ".\shop-data-pipeline-stage3-addon\compose.silver.yaml",
    "-f", ".\shop-data-pipeline-stage4-addon\compose.gold.yaml"
)

docker compose @composeFiles config --quiet
```

Successful configuration validation prints nothing. All relative mounts resolve
from the FIRST Compose file, stage 1. The gold scripts mount therefore uses
`../shop-data-pipeline-stage4-addon/spark`, while `./data` refers to stage 1's data.

Build gold:

```powershell
docker compose @composeFiles run --rm gold
```

Verify and display the saved tables:

```powershell
docker compose @composeFiles run --rm gold /opt/project/gold-jobs/inspect_gold.py --expected-count 10 --expected-revenue 1663.10
```

For your original 10 orders, both commands report:

```text
INPUT ORDERS: 10
TOTAL UNITS SOLD: 19
TOTAL REVENUE EUR: 1663.10
DAILY SALES ROWS: 1
COUNTRY SALES ROWS: 3
PRODUCT SALES ROWS: 4
```

The build ends with `GOLD BUILD COMPLETE`. The inspector prints the three tables
and ends with `GOLD VERIFICATION PASSED`.
The daily table shows average order value `166.31`.
Country totals for these orders are:

| Country | Orders | Units | Revenue EUR |
| --- | ---: | ---: | ---: |
| Egypt | 2 | 3 | 479.70 |
| France | 4 | 6 | 809.40 |
| Germany | 4 | 10 | 374.00 |

If you generated more events, run bronze and silver first, then gold. Adjust the
expected count and revenue, or omit both flags to keep the other checks.

## Files

- `compose.gold.yaml` adds the one-off `gold` Spark service.
- `spark/silver_to_gold.py` validates source silver, aggregates its orders,
  writes three Parquet tables, checks the saved values, and publishes gold.
- `spark/inspect_gold.py` verifies the current gold tables against the exact
  silver snapshot used to build them, then displays their rows.

## Outputs, checks, and reruns

Generated outputs are under `shop-data-pipeline-stage1/data/gold`:

- `current.json` points to the latest complete gold snapshot.
- `runs/<run-id>/daily_sales`, `country_sales`, and `product_sales` hold Parquet.
- `runs/<run-id>/quality.json` records source provenance, counts, and totals.

Gold reads silver's `current.json` once and pins that source snapshot. If silver
is rebuilt later, the existing gold inspector still verifies the older source
used for its tables. Rebuild gold to reflect the new silver snapshot. Retain that
source silver snapshot while its gold snapshot is in use.

Before publication, checks confirm silver's count and value, unique order IDs,
required fields, consistent dates and order values, unique aggregate keys,
saved rows equal to the computed aggregates, and reconciliation of every table's
order count, quantity, and revenue to silver. Empty silver input creates readable
empty tables and zero totals.

A run writes a new snapshot and updates `current.json` only after all three tables
pass. A failed build leaves the previous pointer unchanged. Re-running unchanged
silver data creates another snapshot with the same totals; it does not append
duplicate aggregate rows. Older snapshots remain on disk. Read the current
snapshot rather than combining all historical run directories.

This is a small full-table local-filesystem pipeline with one writer per layer.
It recomputes all clean orders each run. It is not an incremental warehouse.

## Validation performed before delivery

The jobs were exercised with PySpark 3.5.8 and Java 17 locally. Checks covered
the original 10 orders, independent Python decimal/grouping calculations,
unchanged reruns, multiple days, repeated customers across countries, decimal
rounding, empty inputs, failed publication, and mismatched silver counts.
Docker Compose execution on Windows is the next verification you perform using
the commands above.

## Next milestones

Next we load gold tables into PostgreSQL, then add Airflow to schedule the pipeline,
and finally build a dashboard. Commit code and Compose files with GitHub Desktop;
keep generated data, checkpoints, and local credentials out of commits.
