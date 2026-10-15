# Stage 7 — Shop sales dashboard

A local Streamlit dashboard connected to your existing PostgreSQL warehouse.
Requires stages 1–6 and a successful load_postgres task. Keep all seven stage
folders as siblings under shop-data-pipeline. No data reset or Airflow DAG change
is needed. This dashboard does not produce orders or trigger pipeline jobs.

## Start on Windows

Open Docker Desktop, extract this ZIP into the root repository, then run:

```powershell
cd "C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline"
powershell -NoProfile -ExecutionPolicy Bypass -File .\shop-data-pipeline-stage7-addon\start-dashboard.ps1
```

Open http://localhost:8501 (no dashboard login). First startup builds the image.
The script combines all seven Compose files and starts dashboard plus its
PostgreSQL dependency. Existing Airflow and Kafka services are not required for
viewing already loaded data. Start them using the stage 6 instructions when
processing new orders. PowerShell variables created inside this script do not
persist in your parent terminal.

## Expected first view

For the dataset verified on October 15, 2026, select October 9 through October 15:

| Metric | Expected |
|---|---:|
| Revenue | EUR 2,337.20 |
| Orders | 15 |
| Units | 28 |
| Average order value | EUR 155.81 |

Daily revenue: October 9 EUR 1,663.10; October 15 EUR 674.10.
Country revenue: Egypt EUR 1,004.10, France EUR 809.40, Germany EUR 523.70.
Selecting only October 15 yields 5 orders, 9 units, EUR 674.10, AOV EUR 134.82.
These are verification expectations, not hard-coded dashboard data.

## Use

- Date range filters every chart, table, and CSV export by UTC order date.
- Four KPI cards, daily revenue chart, country/product revenue charts.
- Three detail tabs with CSV downloads.
- Refresh warehouse data reads PostgreSQL again; there is no cached data.
- After producing orders, wait for Airflow to finish before refreshing.
- Warehouse load time and Gold snapshot ID provide data provenance.
- To include newly arriving dates, extend the selected end date.

## Correctness

All tables and metadata are read in one READ ONLY, REPEATABLE READ transaction,
so a concurrent atomic warehouse refresh cannot mix snapshots. Currency math
uses Decimal. AOV is recomputed as revenue / orders, never averaged from averages.
Unique customers are deliberately not summed across dates or dimensions; those
counts are not additive. Product and country tables are separate aggregations,
so no misleading combined country/product filtering is offered. Dates without
rows are not invented as zero-sales days. CSV currency values preserve decimals;
charts convert currency to floats only for display.

## Configuration and troubleshooting

Connection uses PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD environment variables
from compose.dashboard.yaml. PostgreSQL stays inside Docker's network, avoiding
the previous Windows 5433 issue. The dashboard uses the existing local development
credentials with read-only transactions; this is not a separately restricted
production database role. Dashboard binds only 127.0.0.1:8501.

If port 8501 is unavailable, change only the host side of the port mapping to
127.0.0.1:8502:8501, rerun the script, then open http://localhost:8502.
If the UI reports missing tables, run the Airflow DAG and refresh.
To inspect container logs from the root folder:

```powershell
docker logs --tail 100 shop-data-pipeline-dashboard-1
```

For code changes, rerun start-dashboard.ps1 to rebuild. Docker volumes and
pipeline data are retained. Do not use `down -v` to stop the project.

## Verification of this package

Tested with Streamlit AppTest against representative warehouse fixtures: totals,
date filtering, CSV controls, empty data, and database failure handling. Pure
aggregation tests cover weighted AOV and Decimal totals. The Docker deployment
and live database connection must be verified on your machine; they were not
available in the authoring environment.
