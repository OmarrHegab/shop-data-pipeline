"""Read committed bronze Parquet rows and check transport-level uniqueness."""

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()
    output = os.environ.get("BRONZE_PATH", "/opt/project/data/bronze/orders")
    if not any(Path(output).rglob("*.parquet")):
        raise SystemExit("No Parquet files found. Run kafka_to_bronze.py after publishing orders.")

    spark = (
        SparkSession.builder.appName("InspectShopBronze")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Read the root so Spark honors the file sink's _spark_metadata log.
        orders = spark.read.parquet(output).cache()
        total = orders.count()
        unique_positions = orders.select("topic", "partition", "offset").distinct().count()
        duplicates = total - unique_positions
        missing_metadata = orders.filter(
            F.col("topic").isNull() | F.col("partition").isNull() | F.col("offset").isNull()
        ).count()

        print(f"BRONZE ROW COUNT: {total}", flush=True)
        print(f"UNIQUE KAFKA POSITIONS: {unique_positions}", flush=True)
        print(f"DUPLICATE KAFKA POSITIONS: {duplicates}", flush=True)
        print(f"ROWS MISSING KAFKA METADATA: {missing_metadata}", flush=True)
        orders.printSchema()
        orders.select(
            "topic", "partition", "offset",
            F.get_json_object("raw_json", "$.order_id").alias("order_id"),
            F.get_json_object("raw_json", "$.product_name").alias("product"),
            F.get_json_object("raw_json", "$.quantity").alias("quantity"),
            F.get_json_object("raw_json", "$.unit_price_eur").alias("unit_price_eur"),
            "ingestion_date",
        ).orderBy("topic", "partition", "offset").show(20, truncate=False)

        if duplicates or missing_metadata:
            raise RuntimeError("Bronze Kafka position checks failed; inspect the output above.")
        if args.expected_count is not None and total != args.expected_count:
            raise RuntimeError(f"Expected {args.expected_count} rows, found {total}.")
        print("BRONZE VERIFICATION PASSED", flush=True)
        orders.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
