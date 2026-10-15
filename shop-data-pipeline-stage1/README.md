# Shop data pipeline — stage 1

Goal: send 10 simulated completed orders into Kafka and read them back.
Python runs inside Docker, so you do not need to install Python on Windows.
Use GitHub Desktop for version control. Airflow and Spark come in later stages.

## Files

- `compose.yaml`: a Kafka broker/controller in KRaft mode and a producer service.
- `producer/Dockerfile`: builds the Python producer image.
- `producer/requirements.txt`: pins the Python Kafka client.
- `producer/producer.py`: generates orders and reports confirmed deliveries.

## Run in PowerShell

Open Docker Desktop and select Linux containers. Extract the archive into your
existing project folder: `compose.yaml` should be directly inside that folder.
Change PowerShell to that folder, not `C:\WINDOWS\system32`.

```powershell
cd "C:\path\to\shop-data-pipeline"
Get-ChildItem
docker compose config --quiet
docker compose up -d --wait kafka
```

Replace the example path with your actual folder. A successful `config --quiet`
prints nothing. Startup downloads the Kafka image on the first run.

Create the topic explicitly. Replication factor 1 is appropriate for this
one-broker local demo; it does not provide broker failure tolerance.

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic orders
```

Build the producer and send 10 orders:

```powershell
docker compose build producer
docker compose run --rm producer --count 10 --interval 1
```

Read the first 10 available events back:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders --from-beginning --max-messages 10 --timeout-ms 30000
```

On a fresh topic, these are the 10 orders you just generated. Later runs append
more orders; `--from-beginning` continues to replay the oldest retained records.
Partitioned records are not guaranteed to print in their original overall order.

## Watch new events

In terminal A, from the project folder:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders
```

Once it is running, in terminal B, from the same project folder:

```powershell
docker compose run --rm producer --count 100 --interval 0.5
```

The consumer prints incoming JSON. Press Ctrl+C in terminal A when finished.

## Concepts

- Producer: the Python process that publishes records.
- Broker: the Kafka server that stores records.
- Topic: the named event stream `orders`.
- Partition: an ordered portion of a topic; this topic has three.
- Offset: a record's position within its partition.
- Consumer: the process that reads records. Spark will be another consumer.
- Message key: our order ID; Kafka uses it to select a partition.

Container clients use `kafka:19092`. Clients running directly on Windows use
`localhost:9092`. Inside a container, `localhost` refers to that container.
The producer waits for delivery confirmation and fails visibly on delivery errors.
Producer idempotence helps with transport retries; business-level duplicate
orders still require deduplication in our processing layer.

## Stop and resume

```powershell
docker compose down
docker compose up -d --wait kafka
```

Kafka data stays in the named Docker volume after `down`. It is not stored in
GitHub. Do not add `--volumes` unless you deliberately want to delete that data.

## Troubleshooting

If Kafka fails to start:

```powershell
docker compose ps -a
docker compose logs --tail 80 kafka
```

If port 9092 is already used, stop the other service or change the host side of
the port mapping and the EXTERNAL advertised port together. Leave the INTERNAL
listener as `kafka:19092`.

## Next stages

1. Spark Structured Streaming: Kafka to bronze Parquet, preserving raw JSON,
   Kafka topic/partition/offset, and ingestion time; persist checkpoints.
2. Spark batch: validate schemas, separate invalid records, and deduplicate
   orders into silver Parquet. Use decimal values for monetary calculations.
3. Aggregate silver orders into gold analytics tables in PostgreSQL, using
   replacement or upserts so a retry cannot double-count revenue.
4. Airflow: schedule finite processing jobs, quality checks, and safe reruns.
5. Streamlit: display revenue, products, and data quality metrics.

## Validation status

Python syntax and YAML structure were checked before packaging. The starter has
not been run against a Docker daemon in the preparation environment. The commands
above verify the actual Docker networking, startup, delivery, and replay on your PC.

## References

- https://kafka.apache.org/41/getting-started/docker/
- https://docs.confluent.io/kafka-clients/python/current/overview.html

Pinned starting versions: Apache Kafka 4.1.2 and confluent-kafka 2.12.0.
These are explicit versions for reproducibility, not a claim of latest releases.
