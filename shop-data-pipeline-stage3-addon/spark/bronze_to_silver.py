"""Validate bronze orders and publish a complete silver snapshot."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, Window, functions as F, types as T


ORDER_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("product_name", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price_eur", T.DecimalType(18, 2)),
    T.StructField("country", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("_corrupt_record", T.StringType()),
])
TEXT_FIELDS = ["event_id", "order_id", "customer_id", "product_id", "product_name", "country"]
BUSINESS_FIELDS = ["customer_id", "product_id", "product_name", "quantity",
                   "unit_price_eur", "country", "event_time", "schema_version"]


def split_orders(bronze):
    """Return clean orders, rejected records, and duplicate-record audit data."""
    parsed = bronze.select(
        "raw_json", "ingested_at",
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json("raw_json", ORDER_SCHEMA, {
            "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record",
        }).alias("payload"),
    )
    parsed = (
        parsed.withColumn("json_ok", F.col("payload").isNotNull()
                          & F.col("payload._corrupt_record").isNull())
        .select("*", "payload.*").drop("payload", "_corrupt_record")
        .withColumn("event_time_text", F.col("event_time"))
        .withColumn("price_text", F.get_json_object("raw_json", "$.unit_price_eur"))
        .withColumn("event_time", F.try_to_timestamp("event_time_text"))
    )
    for field in TEXT_FIELDS:
        parsed = parsed.withColumn(field, F.trim(F.col(field)))

    def failure(valid, reason):
        return F.when(~F.coalesce(valid, F.lit(False)), F.lit(reason))

    checks = [failure(F.col("json_ok"), "invalid_json_or_field_type")]
    checks.extend(failure(F.length(F.col(field)) > 0, f"missing_{field}") for field in TEXT_FIELDS)
    checks.extend([
        failure(F.col("quantity") > 0, "quantity_must_be_positive_integer"),
        failure(F.col("unit_price_eur") > 0, "price_must_be_positive_decimal"),
        failure(F.col("price_text").rlike(r"^-?\d+(\.\d{1,2})?$"),
                "price_requires_plain_decimal_with_at_most_two_places"),
        failure(F.col("event_time").isNotNull()
                & F.col("event_time_text").rlike(r"(Z|[+-]\d{2}:\d{2})$"),
                "event_time_requires_valid_timestamp_and_timezone"),
        failure(F.col("schema_version") == 1, "unsupported_schema_version"),
    ])
    checked = parsed.withColumn("rejection_reasons", F.filter(F.array(*checks), lambda x: x.isNotNull()))
    valid = checked.filter(F.size("rejection_reasons") == 0)

    # Completed orders are immutable in this demo. Conflicting versions go to
    # rejection rather than silently choosing one price/customer/product.
    valid = valid.withColumn("business_hash", F.sha2(F.to_json(F.struct(*BUSINESS_FIELDS)), 256))
    order_conflicts = (
        valid.groupBy("order_id").agg(F.countDistinct("business_hash").alias("versions"))
        .filter(F.col("versions") > 1).select("order_id")
        .withColumn("order_conflict", F.lit(True))
    )
    event_conflicts = (
        valid.groupBy("event_id").agg(F.countDistinct("order_id").alias("orders"))
        .filter(F.col("orders") > 1).select("event_id")
        .withColumn("event_conflict", F.lit(True))
    )
    joined = valid.join(order_conflicts, "order_id", "left").join(event_conflicts, "event_id", "left")
    joined = joined.withColumn("rejection_reasons", F.filter(F.array(
        F.when(F.col("order_conflict"), F.lit("conflicting_order_payload")),
        F.when(F.col("event_conflict"), F.lit("event_id_used_for_multiple_orders")),
    ), lambda x: x.isNotNull())).drop("order_conflict", "event_conflict", "business_hash")

    rejected = checked.filter(F.size("rejection_reasons") > 0).unionByName(
        joined.filter(F.size("rejection_reasons") > 0)
    )
    eligible = joined.filter(F.size("rejection_reasons") == 0)
    window = Window.partitionBy("order_id").orderBy(
        "ingested_at", "kafka_topic", "kafka_partition", "kafka_offset", "event_id",
    )
    ranked = eligible.withColumn("order_rank", F.row_number().over(window))
    duplicates = ranked.filter(F.col("order_rank") > 1).drop("order_rank")
    clean = (
        ranked.filter(F.col("order_rank") == 1)
        .withColumn("order_total_eur", (F.col("quantity") * F.col("unit_price_eur")).cast("decimal(28,2)"))
        .withColumn("event_date", F.to_date("event_time"))
        .select("event_id", "order_id", *BUSINESS_FIELDS, "order_total_eur", "event_date",
                "kafka_topic", "kafka_partition", "kafka_offset", "ingested_at")
    )
    return clean, rejected, duplicates


def publish_snapshot(spark, bronze, silver_root):
    """Publish only after writing and checking a new snapshot's complete data."""
    root = Path(silver_root)
    root.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    relative = Path("runs") / run_id
    run_dir = root / relative
    run_dir.mkdir(parents=True)

    bronze = bronze.cache()
    clean, rejected, duplicates = split_orders(bronze)
    clean, rejected, duplicates = clean.cache(), rejected.cache(), duplicates.cache()
    try:
        counts = {
            "input_rows": bronze.count(), "clean_orders": clean.count(),
            "rejected_rows": rejected.count(), "duplicate_rows": duplicates.count(),
        }
        if counts["input_rows"] != sum(counts[k] for k in ["clean_orders", "rejected_rows", "duplicate_rows"]):
            raise RuntimeError("Input record accounting does not reconcile.")
        if clean.select("order_id").distinct().count() != counts["clean_orders"]:
            raise RuntimeError("Clean order IDs are not unique.")

        writer = clean.write.mode("errorifexists")
        # An empty partitioned directory may contain no schema-bearing file.
        # Keep empty snapshots readable by writing an unpartitioned empty file.
        if counts["clean_orders"]:
            writer = writer.partitionBy("event_date")
        writer.parquet(str(run_dir / "orders"))
        rejected.write.mode("errorifexists").parquet(str(run_dir / "rejected"))
        duplicates.write.mode("errorifexists").parquet(str(run_dir / "duplicates"))
        for label, expected in [("orders", counts["clean_orders"]),
                                ("rejected", counts["rejected_rows"]),
                                ("duplicates", counts["duplicate_rows"])]:
            if spark.read.parquet(str(run_dir / label)).count() != expected:
                raise RuntimeError(f"Saved {label} count differs from the computed count.")

        value = clean.agg(F.sum("order_total_eur").alias("value")).first().value
        manifest = {
            "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat(),
            "paths": {label: str(relative / label) for label in ["orders", "rejected", "duplicates"]},
            "counts": counts, "total_order_value_eur": str(value) if value is not None else "0.00",
        }
        (run_dir / "quality.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        pending = root / f".current-{run_id}.tmp"
        pending.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        os.replace(pending, root / "current.json")
        return manifest
    finally:
        for frame in [bronze, clean, rejected, duplicates]:
            frame.unpersist()


def main():
    bronze_path = os.environ.get("BRONZE_PATH", "/opt/project/data/bronze/orders")
    silver_root = os.environ.get("SILVER_ROOT", "/opt/project/data/silver")
    spark = (SparkSession.builder.appName("ShopBronzeToSilver")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.adaptive.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = publish_snapshot(spark, spark.read.parquet(bronze_path), silver_root)
        for key, value in result["counts"].items():
            print(f"{key.upper().replace('_', ' ')}: {value}", flush=True)
        print(f"TOTAL ORDER VALUE EUR: {result['total_order_value_eur']}", flush=True)
        print(f"CURRENT SILVER SNAPSHOT: {result['run_id']}", flush=True)
        print("SILVER BUILD COMPLETE", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
