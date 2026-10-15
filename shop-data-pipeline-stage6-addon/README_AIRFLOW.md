# Stage 6: Airflow orchestration

Stages 1–5 already work individually. Airflow now coordinates those same jobs,
records their logs and outcomes, and retries failures. Your first run is manual;
the DAG starts paused and has no automatic schedule until you enable one later.

## What is added

- Airflow 3.3.2 with the Docker provider, using the LocalExecutor.
- A separate PostgreSQL database for Airflow metadata: run status, task state,
  and scheduling information. Your `shop_analytics` database remains the destination
  for business data.
- An eight-task DAG named `shop_orders_pipeline`.
- A Windows preparation script that discovers the actual Docker-side paths of
  your existing mounts, including the named Spark Ivy cache volume.

For this local learning setup, Airflow's standalone command starts its API/UI,
scheduler, DAG processor, and triggerer in one container. These are separate
processes managed together. The scheduler's LocalExecutor runs task processes,
and DockerOperator starts the existing Spark or Python job in its own container.
This is a development setup rather than a production deployment.

## 1. Extract beside stages 1–5

Extract this ZIP into:

```text
C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline\shop-data-pipeline-stage6-addon
```

The main repository should now contain six sibling stage folders. Inside stage
6, directly place `compose.airflow.yaml`, `prepare.ps1`, `airflow`, `dags`, and
this README. The preparation script creates a `config` directory automatically.
Do not add an extra nested copy of the stage 6 folder.

## 2. Prepare Airflow

Open Docker Desktop. Finish any manually running Spark jobs before continuing,
including any `--live` job. From PowerShell in the main repository:

```powershell
cd "C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline"
powershell -NoProfile -ExecutionPolicy Bypass -File .\shop-data-pipeline-stage6-addon\prepare.ps1
```

`-ExecutionPolicy Bypass` applies only to this new PowerShell process; it does
not change your saved Windows policy. The script:

1. Validates all six Compose files.
2. Builds the warehouse and Airflow images.
3. Starts Kafka, analytics PostgreSQL, and Airflow's metadata PostgreSQL.
4. Runs the existing Spark storage-permissions initializer.
5. Creates four stopped job-template containers without running their jobs.
6. Inspects those templates and generates `config/jobs.json` for Airflow.
7. Checks Docker access, images, services, and the DAG import.

The script ends with:

```text
AIRFLOW SETUP CHECK PASSED
AIRFLOW PREPARATION COMPLETE
```

The first build downloads Airflow and may take several minutes. The mount
configuration is generated using Docker's own inspect output; Windows paths
are not guessed or rewritten manually. Do not commit the generated JSON files.
Re-run preparation after moving the repository, rebuilding job images, or
changing their Compose configuration.

Stage 6 retains your PostgreSQL port fix by clearing its Windows port mapping.
It also sets the Spark script mount to your sibling stage 2 folder. Compose's
`!reset` syntax is supported by your installed Compose version.

## 3. Define the updated Compose list

The preparation script runs in another PowerShell process, so define this list
again in your current window:

```powershell
$composeFiles = @(
    "-f", ".\shop-data-pipeline-stage1\compose.yaml",
    "-f", ".\shop-data-pipeline-stage2-addon\compose.spark.yaml",
    "-f", ".\shop-data-pipeline-stage3-addon\compose.silver.yaml",
    "-f", ".\shop-data-pipeline-stage4-addon\compose.gold.yaml",
    "-f", ".\shop-data-pipeline-stage5-addon\compose.postgres.yaml",
    "-f", ".\shop-data-pipeline-stage6-addon\compose.airflow.yaml"
)
```

## 4. Start Airflow

```powershell
docker compose @composeFiles up -d --wait --wait-timeout 600 airflow
```

Airflow initializes its metadata database and starts its components. The health
check waits for the API, database, scheduler, and DAG processor. Wait for Healthy.

Open [http://localhost:8080](http://localhost:8080).

| Login setting | Local demo value |
| --- | --- |
| Username | omar |
| Password | airflow |

The password file and task logs use named Docker volumes and survive container
restarts. The metadata database also uses a named volume. The UI is bound to
your local computer's loopback interface.

Airflow uses its development Simple Auth Manager. Its socket mount lets it
control Docker containers for these jobs; keep this setup for your local project.
The launcher grants the normal `airflow` user access to the socket's existing
group, then drops root privileges before starting Airflow.

## 5. Trigger your first DAG run

In the UI:

1. Find `shop_orders_pipeline` in the DAG list. Allow a short time for parsing.
2. Switch it from paused to enabled.
3. Use the Trigger control, usually shown with a play icon, to start a run.
4. Open the run's Graph or task view and inspect the tasks and their logs.

The eight tasks run in this order:

| Task ID | Action |
| --- | --- |
| ingest_bronze | Consume available Kafka events into bronze, using the existing checkpoint |
| verify_bronze | Check bronze records and Kafka positions |
| build_silver | Validate orders and publish a clean silver snapshot |
| verify_silver | Check silver counts, IDs, and money calculations |
| build_gold | Publish daily, country, and product summaries |
| verify_gold | Compare saved gold aggregates to source silver |
| load_postgres | Refresh the analytics database in one transaction |
| verify_postgres | Compare database rows to the loaded gold snapshot |

The run succeeds when all eight tasks succeed. The final task log should end
with `POSTGRES VERIFICATION PASSED`. If you have only the original events, it
should still show 10 orders, 19 units, and revenue EUR 1663.10.

`ingest_bronze` may report zero NEW records on the first Airflow run because the
previous manual job already consumed those events. Bronze still contains the
original 10 records, and subsequent tasks process them normally.

The verification tasks do not hardcode 10 orders or EUR 1663.10. They verify the
actual current data, so the same DAG can handle additional events later.

## Retries, scheduling, and reruns

Each task allows two retries, 30 seconds apart, with a 20-minute execution
timeout. A failed task blocks downstream tasks until it succeeds. Job logs are
captured in Airflow; task containers are removed afterward.

`max_active_runs=1` and `max_active_tasks=1` keep this DAG's jobs sequential.
Do not run manual bronze/silver/gold jobs alongside an active DAG run; manual
commands are outside Airflow's concurrency limits. The existing bronze checkpoint,
silver/gold publication checks, and PostgreSQL transaction behavior make normal
reruns safe. Silver and gold runs still retain historical snapshots on disk.

The producer stays separate because it generates fresh UUIDs. Automatically
retrying data generation would create additional orders. Airflow orchestrates
processing of the events already sent to Kafka.

After one successful run, trigger the DAG again without producing new events.
The totals should stay unchanged. This verifies the complete orchestration path.

Only after manual runs work, change the DAG's `schedule=None` to:

```python
schedule="*/5 * * * *",
```

Airflow will then schedule processing every five minutes while the services and
computer are running. `catchup=False` avoids creating historical interval runs.
Keep the DAG paused when you do not want scheduled runs. Enable this schedule
after inspecting the first manual runs, since each run stores new silver/gold
snapshots and consumes disk space.

## Optional new-event check

After the first unchanged rerun succeeds, generate five new orders:

```powershell
docker compose @composeFiles run --rm producer --count 5 --interval 0.2
```

Trigger the DAG again. If the dataset previously contained exactly the original
10 orders and all five new events are valid, it should now show 15 clean orders.
Revenue changes according to the generated products and quantities.

## Troubleshooting

Show Airflow logs:

```powershell
docker compose @composeFiles logs --tail 100 airflow
```

Show DAG import errors after startup:

```powershell
docker compose @composeFiles exec --user airflow airflow airflow dags list-import-errors
```

- Port 8080 blocked or in use: change only the LEFT side of Airflow's port
  mapping to `127.0.0.1:18080:8080`, then rerun the Airflow startup command and
  visit `http://localhost:18080`. Internal API URLs remain on port 8080.
- Missing `jobs.json`: rerun `prepare.ps1` before starting Airflow.
- Socket access failure: include the full preparation output. The startup
  launcher handles the socket group automatically.
- New repository location: rerun preparation so daemon-side mount paths are
  regenerated.
- Container exits with 137 or components keep restarting: check Docker Desktop's
  available memory. Airflow's Docker guide suggests at least 4 GB, ideally 8 GB;
  this project also runs Kafka and one Spark job at a time.
- Failed task: read that task's log first, fix its cause, then clear/retry it in
  Airflow rather than deleting data or checkpoints.

On a later day, start the services again using the same six-file list:

```powershell
docker compose @composeFiles up -d --wait kafka postgres
docker compose @composeFiles up -d --wait --wait-timeout 600 airflow
```

Do not remove the named volumes when simply stopping the project; they contain
Kafka events, database contents, Airflow metadata, and logs.

## Validation before delivery

The DAG was imported and serialized using real Airflow 3.3.2, task SDK 1.3.2,
Docker provider 4.5.9, and their official Python 3.12 constraints. Checks covered
all seven dependency edges, single-run settings, manual scheduling, retries,
job commands, Windows daemon-side bind paths, named-volume mapping, private
password handling, and missing/duplicate template guards. The preparation script
was parsed and exercised in PowerShell 7 with a fake Docker command to check
arguments, UTF-8 JSON encoding, cleanup, and stopping on failed commands. Python,
shell, and Compose YAML syntax were checked. Docker Desktop execution and the complete DAG
run still require your first run above.

## What remains

After the Airflow run and unchanged rerun succeed, we add the dashboard, test new
events through the full pipeline, and finish the repository documentation for
GitHub Desktop.

Official references: [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/4.5.9/_api/airflow/providers/docker/operators/docker/index.html),
[Airflow Docker setup](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html),
and [Simple Auth Manager](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html).
