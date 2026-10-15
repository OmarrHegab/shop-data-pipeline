# Run the complete project

Use Docker Desktop (Linux containers) and PowerShell. All Python, Java, Spark,
and Airflow dependencies run in containers. Keep all seven stage folders as
siblings at the root; do not nest one stage inside another.

## 1. Open the repository

Clone through GitHub Desktop, select **Repository > Open in Terminal**, and
use PowerShell. From the repository root:

```powershell
. .\scripts\compose.ps1
docker compose @composeFiles config --quiet
```

The leading dot and space dot-source the helper, making `$composeFiles`
available in your current shell. Run it again in each new PowerShell window.
Use the complete Compose list to avoid orphan warnings about other stages.

## 2. Start Kafka and create the topic

```powershell
docker compose @composeFiles up -d --wait kafka
docker compose @composeFiles exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1
docker compose @composeFiles build producer
```

The Kafka permissions initializer exits after assigning ownership of the data
volume. Successful exit is expected. Single-broker replication is for this local
project and does not provide broker-failure tolerance.

## 3. Generate sample data

On a fresh installation, run once:

```powershell
docker compose @composeFiles run --rm producer --count 10 --interval 0.2
```

Every invocation generates new UUIDs and appends more orders. The screenshots
use a particular captured dataset; random fresh runs will produce different
revenue and product mixes.

## 4. Prepare Airflow

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\shop-data-pipeline-stage6-addon\prepare.ps1
docker compose @composeFiles up -d --wait --wait-timeout 600 airflow
```

Wait for `AIRFLOW PREPARATION COMPLETE` and a healthy Airflow container.
Preparation builds the images, creates job templates, discovers their Docker
mounts, and writes a machine-specific configuration under stage 6's `config/`.
That generated directory is excluded from Git. Run preparation on each new
clone/machine, or after moving the repository. Pause the DAG and wait for active
runs to finish before re-preparing an existing installation.

Open http://localhost:8080 and sign in with the local demo account:

- Username: `omar`
- Password: `airflow`

Open **Dags > shop_orders_pipeline**, unpause it and trigger a run. Wait for all
eight tasks to succeed. First-time Spark connector downloads can take longer.

To enable the demonstrated five-minute schedule, confirm the DAG file contains:

```python
schedule="*/5 * * * *",
catchup=False,
max_active_runs=1,
```

If it still says `schedule=None`, replace that line. Save the file; the mounted
DAG directory is parsed automatically. Confirm a run with **Run Type: Scheduled**.
Do not run manual Spark jobs concurrently with an active DAG run.

## 5. Start the dashboard

In `shop-data-pipeline-stage7-addon/compose.dashboard.yaml`, use:

```yaml
ports:
  - "127.0.0.1:8502:8501"
```

Then:

```powershell
docker compose @composeFiles up -d --build --wait --wait-timeout 300 dashboard
```

Open http://localhost:8502. Use the full date range to see all data. The dashboard
refresh button queries PostgreSQL; it does not start Airflow. Extend the selected
end date when orders arrive on a new day.

The original stage 7 startup script prints a fixed URL. If using that script,
update its final `Write-Host` message to match your chosen host port.

## Daily use

```powershell
. .\scripts\compose.ps1
docker compose @composeFiles up -d --wait --wait-timeout 600 kafka postgres airflow dashboard
docker compose @composeFiles run --rm producer --count 5 --interval 0.2
```

Wait for the next scheduled run (or trigger one), then refresh the dashboard.
For logs and status:

```powershell
docker compose @composeFiles ps -a
docker compose @composeFiles logs --tail 100 airflow dashboard
```

To stop without deleting persistent data:

```powershell
docker compose @composeFiles stop
```

Do not use `down -v` unless deliberately deleting named-volume data. Keep the
Bronze file-sink output and its checkpoint together. Neither generated data nor
Docker volumes are stored in GitHub.

## Common issues

| Symptom | Action |
|---|---|
| Script not found | Check for an accidentally nested ZIP extraction folder |
| Port already allocated | Change the host port, e.g. `8502:8501`; keep the container port unchanged |
| Orphan-container warning | Use all seven Compose files; do not remove the other stage containers |
| No DAG / DAG import error | Check stage 6 preparation succeeded and `config/jobs.json` exists locally |
| Dashboard cannot read tables | Confirm PostgreSQL is healthy and Airflow completed `load_postgres` |
| Startup succeeds but displayed link is wrong | Correct the startup script's fixed printed URL |

Local credentials in Compose are demo defaults. The PostgreSQL service is
internal-only with the stage 6 override. The Airflow Docker socket mount grants
substantial local Docker access; the setup is intended for a trusted development
machine.
