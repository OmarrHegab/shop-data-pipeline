# Stage 5: gold to PostgreSQL

This stage adds a persistent PostgreSQL database and a small Python loader.
The loader reads your already-verified gold Parquet summaries. SQL users and
the future dashboard can query PostgreSQL without starting Spark for each query.

## Folder layout

Extract this ZIP into `shop-data-pipeline-stage5-addon`, beside stages 1–4 inside
your main `shop-data-pipeline` repository. The new folder should directly contain
`compose.postgres.yaml`, `loader`, `sql`, and this README, without another nested
copy of the stage 5 folder.

| Folder | Purpose |
| --- | --- |
| shop-data-pipeline-stage1 | Kafka, producer, Parquet data, checkpoints |
| shop-data-pipeline-stage2-addon | Kafka to bronze |
| shop-data-pipeline-stage3-addon | Bronze to silver |
| shop-data-pipeline-stage4-addon | Silver to gold |
| shop-data-pipeline-stage5-addon | Gold to PostgreSQL |

Keep Docker Desktop running and finish gold before loading PostgreSQL.

## 1. Define the Compose file list

Copy this into PowerShell, including the updated list with all five files:

```powershell
cd "C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline"

$composeFiles = @(
    "-f", ".\shop-data-pipeline-stage1\compose.yaml",
    "-f", ".\shop-data-pipeline-stage2-addon\compose.spark.yaml",
    "-f", ".\shop-data-pipeline-stage3-addon\compose.silver.yaml",
    "-f", ".\shop-data-pipeline-stage4-addon\compose.gold.yaml",
    "-f", ".\shop-data-pipeline-stage5-addon\compose.postgres.yaml"
)

docker compose @composeFiles config --quiet
```

Successful validation prints nothing. Relative mounts and build paths resolve
from the FIRST Compose file, which is still stage 1's `compose.yaml`.

## 2. Start PostgreSQL

```powershell
docker compose @composeFiles up -d --wait postgres
```

The first run downloads the official PostgreSQL 17 image and initializes the
database. Expect a healthy `postgres` container. The persistent database uses a
named Docker volume, separate from your bronze/silver/gold files.

## 3. Build the loader

```powershell
docker compose @composeFiles build warehouse
```

The loader image uses Python 3.12, PyArrow to read Parquet, and Psycopg to connect
to PostgreSQL. It reads only gold summaries and does not run Spark. The gold
data mount is read-only. Python dependencies are installed inside Docker, so you
do not need to install them in Windows.

## 4. Load gold

```powershell
docker compose @composeFiles run --rm warehouse
```

This creates the analytics schema and tables on the first load, then refreshes
all three tables in one transaction. It uses parameterized SQL inserts, primary
keys, database constraints, and row-by-row comparisons with gold before commit.
Expect `POSTGRES LOAD COMPLETE` at the end.

## 5. Verify the database

```powershell
docker compose @composeFiles run --rm warehouse inspect_postgres.py --expected-count 10 --expected-revenue 1663.10
```

For your original orders, expect:

```text
INPUT ORDERS: 10
TOTAL UNITS SOLD: 19
TOTAL REVENUE EUR: 1663.10
DAILY SALES ROWS: 1
COUNTRY SALES ROWS: 3
PRODUCT SALES ROWS: 4
COUNTRY | ORDERS | UNITS | REVENUE EUR
France | 4 | 6 | 809.40
Egypt | 2 | 3 | 479.70
Germany | 4 | 10 | 374.00
POSTGRES VERIFICATION PASSED
```

The gold snapshot ID also appears. The inspector compares every database row
to that loaded gold snapshot, including dates, grouping fields, integer counts,
decimal revenue, and average order values. It uses a consistent read transaction.

If you generated more orders, adjust or omit the expected flags. The other
verification checks still run without those flags.

## 6. Query with SQL

You can use `psql` inside the existing PostgreSQL container, without installing
another program:

```powershell
docker compose @composeFiles exec postgres psql -U shop -d shop_analytics -c "SELECT country, SUM(revenue_eur) AS revenue_eur FROM analytics.country_sales GROUP BY country ORDER BY revenue_eur DESC;"
```

Run all the bundled example queries:

```powershell
Get-Content .\shop-data-pipeline-stage5-addon\sql\example_queries.sql | docker compose @composeFiles exec -T postgres psql -U shop -d shop_analytics -v ON_ERROR_STOP=1
```

Or open an interactive SQL prompt:

```powershell
docker compose @composeFiles exec postgres psql -U shop -d shop_analytics
```

At that prompt, use `\dt analytics.*` to list tables, `SELECT * FROM
analytics.daily_sales;` to inspect daily metrics, and `\q` to exit.

## Tables and provenance

| PostgreSQL table | Grain / primary key |
| --- | --- |
| analytics.daily_sales | event_date |
| analytics.country_sales | event_date, country |
| analytics.product_sales | event_date, product_id, product_name |
| analytics.pipeline_state | One row identifying the current database load |

The three sales tables retain gold's order counts, units, distinct customer
counts, revenue, and average order value. PostgreSQL stores money as `numeric`,
not floating point. `pipeline_state` records the gold run ID, source silver run
ID, load time, and the full gold manifest for verification.

Each sales table is a different view of the same orders. Do not add their
revenues together. Distinct customer counts are not additive across groups.
For an overall average order value, divide total revenue by total orders.

## Safe reloads and failures

Re-run the load and inspection commands with unchanged gold data. Counts stay at
1 daily row, 3 country rows, and 4 product rows. The current load metadata is
refreshed, but sales rows are not duplicated.

Before writing, the loader checks gold row counts, grouping-key uniqueness,
positive metrics, decimal money, average calculations, overall totals, and
country/product totals for every day. It reads `gold/current.json` once to pin a
snapshot. Loading replaces the existing analytics rows rather than appending;
an empty current gold snapshot clears the sales tables and records zero totals.

All three sales tables and load metadata change in the same transaction. A
failed insert, constraint check, or verification rolls the transaction back,
preserving the previous committed contents. Cooperating loaders serialize with
a transaction-scoped advisory lock. Table locks briefly block readers during
the refresh. This is a small full-snapshot loader, not an incremental warehouse.

The inspector follows the gold snapshot recorded in PostgreSQL. If you build a
newer gold snapshot, it tells you a refresh is available. Retain the loaded gold
snapshot while it is in use; run the loader again to publish newer gold data.

## Connection details

These are local demo credentials supplied by this Compose file:

| Setting | Value |
| --- | --- |
| Host from Windows | localhost |
| Port from Windows | 5433 |
| Database | shop_analytics |
| User | shop |
| Password | shop_local_dev |
| Host from another project container | postgres |
| Port inside Docker | 5432 |

Only the local computer's loopback interface exposes the database port. We use
5433 on Windows to avoid a common conflict with an existing database on 5432.
The Docker volume retains database data across container restarts. Initialization
credentials apply when the volume is first created; changing them later requires
changing the existing database role as well. Keep these credentials for this
local learning project.

## Troubleshooting

- Missing gold snapshot: finish stage 4 before running the loader.
- Missing build context: check the sibling stage 5 folder name and ensure
  `loader/Dockerfile` is directly inside it.
- Port 5433 already in use: change the LEFT side of the port mapping, for
  example to `127.0.0.1:5434:5432`. Container connections still use port 5432.
- After editing Python loader code, rebuild `warehouse`; the scripts are copied
  into the image during the build.
- For database startup problems, run:

```powershell
docker compose @composeFiles logs --tail 80 postgres
```

## Validation before delivery

Parquet reading and dependency imports were tested with the pinned Python
packages. SQL, decimal values, safe reloads, changed snapshots, empty loads,
constraints, and rollback were exercised against a local PostgreSQL engine
through PGlite. Its protocol differs from native PostgreSQL, so the fault tests
used Psycopg's simple-protocol client cursor. The Docker PostgreSQL 17 image and
the Windows Compose workflow still need the checks above on your computer.

## Next

After database verification, stage 6 adds Airflow to schedule the pipeline steps,
track their outcomes, and retry failures. A dashboard and a final full-pipeline
test follow. You can commit this stage's code with GitHub Desktop; keep generated
data and checkpoints out of commits.
