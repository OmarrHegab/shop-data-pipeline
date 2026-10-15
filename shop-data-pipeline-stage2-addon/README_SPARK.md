# Stage 2: Kafka to bronze Parquet with Spark

Add the three items in this archive to your EXISTING project root:

- `compose.spark.yaml`
- the `spark` folder (two Python scripts)
- this `README_SPARK.md`

Your original `compose.yaml`, producer, Kafka topic, and Kafka volume are reused.
Always use the same project folder; relative mounts are based on the first
Compose file. The archive has no enclosing project folder.

## What changes

Spark is now another Kafka consumer. It stores original key/value bytes, a JSON
text view, headers, topic, partition, offset, Kafka timestamp/type, and ingestion
time. Files are partitioned by UTC ingestion date. This is the bronze layer;
business-field validation and deduplication are later silver-layer work.

Spark uses local mode with two worker threads and a 1 GB Java driver heap.
This is a single-machine learning setup, not a distributed multi-machine cluster.
The container also uses memory outside that heap. Python and Java are included
in the image; no host installation is needed.

`compose.spark.yaml` extends your original Compose file. `spark-storage-init`
sets directory ownership before Spark runs as UID/GID 185. A named `spark-ivy`
volume caches the connector dependencies so they can be reused between runs.

## First run: ingest your existing 10 orders

From PowerShell, in your existing project folder:

```powershell
cd "C:\Users\omarh\OneDrive\Desktop\shop-data-pipeline\shop-data-pipeline-stage1"
New-Item -ItemType Directory -Force -Path data, checkpoints | Out-Null
docker compose -f compose.yaml -f compose.spark.yaml config --quiet
docker compose -f compose.yaml -f compose.spark.yaml pull spark
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark
```

The first pull downloads Spark. The first job resolves the matching Kafka
connector from Maven; progress messages before your application starts are normal.
The startup helper exits successfully after fixing directory ownership; this is
expected. Kafka must become healthy before ingestion begins.

The default mode uses Structured Streaming's `availableNow` trigger: process
records available when the query starts, commit them, and exit. Look for
`BRONZE INGESTION COMPLETE`. Records arriving after that boundary are picked
up on the next run (or by the live mode below).

On a fresh checkpoint, `startingOffsets=earliest` reads the oldest retained
records. Console consumption does not prevent Spark from reading those records.
On subsequent runs, Spark resumes from its own persisted checkpoint.

## Verify

```powershell
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark /opt/project/spark/inspect_bronze.py --expected-count 10
```

Expected after the first ingestion of the original ten orders:

```text
BRONZE ROW COUNT: 10
UNIQUE KAFKA POSITIONS: 10
DUPLICATE KAFKA POSITIONS: 0
ROWS MISSING KAFKA METADATA: 0
BRONZE VERIFICATION PASSED
```

This command fails visibly if the expected count differs or Kafka position
checks fail. If you intentionally published additional orders, omit
`--expected-count 10` or use the correct total. The displayed order fields are
extracted only for inspection; the saved bronze payload remains intact.

## Check that restarting does not append the same Kafka records again

Run ingestion again WITHOUT publishing more orders:

```powershell
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark /opt/project/spark/inspect_bronze.py --expected-count 10
```

The total should remain ten. This verifies checkpoint resume with the same query
and file-sink output. It does not remove duplicate business events that were
published as separate Kafka records; those will be handled in the silver layer.

## Add five orders and ingest only the new records

After the tests above pass:

```powershell
docker compose run --rm producer --count 5 --interval 1
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark /opt/project/spark/inspect_bronze.py --expected-count 15
```

## Live ingestion (optional)

In terminal A, from the same project root:

```powershell
docker compose -f compose.yaml -f compose.spark.yaml run --rm spark /opt/project/spark/kafka_to_bronze.py --live
```

This keeps running with five-second micro-batches. In terminal B:

```powershell
docker compose run --rm producer --count 20 --interval 1
```

Spark saves those records as they arrive. When done, press Ctrl+C in terminal A
and allow the job to stop before inspecting or starting another ingestion run.
Do not run two writers with this checkpoint/output pair at the same time.
Live mode here means repeatedly running micro-batches, not Spark's experimental
continuous-processing engine.

## Where to find the files

Under your existing project folder:

- `data/bronze/orders/ingestion_date=YYYY-MM-DD/`: Parquet data files.
- `data/bronze/orders/_spark_metadata/`: the file sink's committed-file log.
- `checkpoints/bronze-orders/`: query identity, offsets, and committed batches.

Parquet is a binary, column-oriented format; use Spark to inspect it. Its
folders and checkpoints persist outside the temporary job containers.
Keep the checkpoint and output together. Removing or changing them independently
can produce missing or inconsistent results. Future schema changes need a
planned migration; do not arbitrarily reset the checkpoint.

The original `.gitignore` already excludes `data/` and `checkpoints/`. Commit
the new configuration and scripts through GitHub Desktop after verification.

## Troubleshooting and validation

If a job fails, copy the final error/traceback and the preceding lines. If image
pull or dependency resolution fails, check the Docker Desktop connection and
whether your network allows Docker Hub and Maven Central. A file-permissions
error should include its exact path so we can distinguish the data directory,
checkpoint, and dependency cache. Spark prints some startup WARN messages even
on successful runs; use the completion and verification messages to assess this
milestone.

Preparation checks: Python syntax, YAML structure, cross-file service references,
and archive integrity. Local PySpark 3.5.8 checks also passed: all ten captured
order payloads and Kafka positions survive the Parquet write; resuming from the
same checkpoint keeps the count at ten; an additional malformed JSON payload
and binary headers are preserved; and the shipped inspector passes. Those
checkpoint checks used a replayable file source with the Kafka source's schema.
Docker/Kafka network integration could not be run in the preparation environment.
The commands above verify that remaining part against your actual local broker.

## Official references

- https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html
- https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html
- https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/

Spark image and Kafka connector are both pinned to 3.5.8 / Scala 2.12.
