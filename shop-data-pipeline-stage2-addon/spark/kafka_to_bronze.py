"""Read Kafka into bronze Parquet, preserving original bytes and metadata.

Default: process all records available at startup and finish (AvailableNow).
--live: keep processing new records in five-second micro-batches.
"""

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def to_bronze(source):
    """Keep the payload intact; add metadata without parsing business fields."""
    return (
        source.select(
            F.col("key").alias("raw_key"),
            F.col("value").alias("raw_value"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType").alias("kafka_timestamp_type"),
            F.col("headers").alias("kafka_headers"),
            F.current_timestamp().alias("ingested_at"),
        )
        .withColumn("ingestion_date", F.to_date("ingested_at"))
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    topic = os.environ.get("KAFKA_TOPIC", "orders")
    output = os.environ.get("BRONZE_PATH", "/opt/project/data/bronze/orders")
    checkpoint = os.environ.get("CHECKPOINT_PATH", "/opt/project/checkpoints/bronze-orders")
    Path(output).mkdir(parents=True, exist_ok=True)
    Path(checkpoint).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("ShopKafkaToBronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("includeHeaders", "true")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", 10000)
            .load()
        )
        writer = (
            to_bronze(source).writeStream
            .queryName("shop_orders_bronze")
            .format("parquet")
            .outputMode("append")
            .option("path", output)
            .option("checkpointLocation", checkpoint)
            .option("compression", "snappy")
            .partitionBy("ingestion_date")
        )
        if args.live:
            writer = writer.trigger(processingTime="5 seconds")
        else:
            writer = writer.trigger(availableNow=True)

        print(f"Starting Kafka ingestion: topic={topic}, server={bootstrap}", flush=True)
        print(f"Bronze output: {output}", flush=True)
        print(f"Checkpoint: {checkpoint}", flush=True)
        print("Mode: live micro-batches" if args.live else "Mode: available-now", flush=True)
        query = writer.start()
        query.awaitTermination()
        for progress in query.recentProgress:
            print(
                f"Completed batch {progress['batchId']}: "
                f"{progress['numInputRows']} input records",
                flush=True,
            )
        print("BRONZE INGESTION COMPLETE", flush=True)
    except KeyboardInterrupt:
        if query is not None:
            query.stop()
        print("Ingestion stopped; checkpoint retained.", flush=True)
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
