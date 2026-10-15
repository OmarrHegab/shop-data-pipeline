"""Build verified daily sales aggregates from one complete silver snapshot."""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F, types as T

GRAINS = {
    "daily_sales": ["event_date"],
    "country_sales": ["event_date", "country"],
    "product_sales": ["event_date", "product_id", "product_name"],
}


def build_tables(orders):
    tables = {}
    for name, keys in GRAINS.items():
        tables[name] = (
            orders.groupBy(*keys).agg(
                F.count("*").alias("order_count"),
                F.sum("quantity").cast("long").alias("units_sold"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("order_total_eur").alias("revenue_eur"),
            ).withColumn("average_order_value_eur",
                         (F.col("revenue_eur") / F.col("order_count")).cast("decimal(28,2)"))
        )
    return tables


def load_silver(spark, silver_root, source_manifest=None):
    root = Path(silver_root)
    if source_manifest is None:
        pointer = root / "current.json"
        if not pointer.exists():
            raise RuntimeError("No published silver snapshot. Run stage 3 first.")
        source_manifest = json.loads(pointer.read_text(encoding="utf-8"))
    orders = spark.read.parquet(str(root / source_manifest["paths"]["orders"])).cache()
    try:
        if not isinstance(orders.schema["order_total_eur"].dataType, T.DecimalType):
            raise RuntimeError("Silver order values must use decimals.")
        required = ["order_id", "customer_id", "country", "product_id", "product_name", "event_date"]
        invalid = F.lit(False)
        for col in required:
            invalid = invalid | F.col(col).isNull()
        invalid = (invalid | F.col("quantity").isNull() | (F.col("quantity") <= 0)
                   | F.col("order_total_eur").isNull() | (F.col("order_total_eur") <= 0)
                   | F.col("event_time").isNull()
                   | (F.col("event_date") != F.to_date("event_time"))
                   | F.col("unit_price_eur").isNull()
                   | (F.col("order_total_eur") != F.col("quantity") * F.col("unit_price_eur")))
        if orders.filter(invalid).limit(1).count():
            raise RuntimeError("Silver contains invalid order data.")
        count = orders.count()
        if count != source_manifest["counts"]["clean_orders"]:
            raise RuntimeError("Silver row count differs from its manifest.")
        if orders.select("order_id").distinct().count() != count:
            raise RuntimeError("Silver order IDs are not unique.")
        total = orders.agg(F.sum("order_total_eur").alias("revenue"),
                           F.sum("quantity").alias("units")).first()
        revenue = total.revenue if total.revenue is not None else Decimal("0.00")
        if revenue != Decimal(source_manifest["total_order_value_eur"]):
            raise RuntimeError("Silver order value differs from its manifest.")
        metrics = {"input_orders": count, "total_units": total.units or 0,
                   "total_revenue_eur": str(revenue)}
        return orders, source_manifest, metrics
    except Exception:
        orders.unpersist()
        raise


def verify_table(actual, expected, name, metrics):
    """Check saved rows, grouping keys, and reconciliation to silver totals."""
    actual = actual.select(*expected.columns)
    for col in ["revenue_eur", "average_order_value_eur"]:
        if not isinstance(actual.schema[col].dataType, T.DecimalType):
            raise RuntimeError(f"{name}: money is not stored as a decimal.")
    count = actual.count()
    if actual.select(*GRAINS[name]).distinct().count() != count:
        raise RuntimeError(f"{name}: duplicate grouping keys.")
    if (actual.exceptAll(expected).limit(1).count()
            or expected.exceptAll(actual).limit(1).count()):
        raise RuntimeError(f"{name}: saved aggregates differ from silver.")
    totals = actual.agg(F.sum("order_count").alias("orders"),
                        F.sum("units_sold").alias("units"),
                        F.sum("revenue_eur").alias("revenue")).first()
    if ((totals.orders or 0) != metrics["input_orders"]
            or (totals.units or 0) != metrics["total_units"]
            or (totals.revenue or Decimal("0.00")) != Decimal(metrics["total_revenue_eur"])):
        raise RuntimeError(f"{name}: totals do not reconcile with silver.")
    return count


def publish_gold(spark, silver_root, gold_root):
    # Read current.json once; this run remains tied to that immutable snapshot.
    orders, source, metrics = load_silver(spark, silver_root)
    tables = {name: frame.cache() for name, frame in build_tables(orders).items()}
    try:
        root = Path(gold_root)
        root.mkdir(parents=True, exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
        relative = Path("runs") / run_id
        run_dir = root / relative
        run_dir.mkdir(parents=True)
        counts = {}
        for name, expected in tables.items():
            path = run_dir / name
            expected.write.mode("errorifexists").parquet(str(path))
            counts[name] = verify_table(spark.read.parquet(str(path)), expected, name, metrics)
        result = {
            "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat(),
            "source_silver": source, "metrics": metrics, "table_rows": counts,
            "paths": {name: str(relative / name) for name in GRAINS},
        }
        contents = json.dumps(result, indent=2)
        (run_dir / "quality.json").write_text(contents, encoding="utf-8")
        pending = root / f".current-{run_id}.tmp"
        pending.write_text(contents, encoding="utf-8")
        os.replace(pending, root / "current.json")
        return result
    finally:
        orders.unpersist()
        for frame in tables.values():
            frame.unpersist()


def print_summary(manifest):
    print(f"SOURCE SILVER SNAPSHOT: {manifest['source_silver']['run_id']}", flush=True)
    print(f"INPUT ORDERS: {manifest['metrics']['input_orders']}", flush=True)
    print(f"TOTAL UNITS SOLD: {manifest['metrics']['total_units']}", flush=True)
    print(f"TOTAL REVENUE EUR: {manifest['metrics']['total_revenue_eur']}", flush=True)
    for name, count in manifest["table_rows"].items():
        print(f"{name.upper().replace('_', ' ')} ROWS: {count}", flush=True)


def main():
    spark = (SparkSession.builder.appName("ShopSilverToGold")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.adaptive.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = publish_gold(spark, os.environ.get("SILVER_ROOT", "/opt/project/data/silver"),
                              os.environ.get("GOLD_ROOT", "/opt/project/data/gold"))
        print_summary(result)
        print("GOLD BUILD COMPLETE", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
