"""Verify the current published silver snapshot."""
import argparse
import json
import os
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-count", type=int)
    args = parser.parse_args()
    root = Path(os.environ.get("SILVER_ROOT", "/opt/project/data/silver"))
    pointer = root / "current.json"
    if not pointer.exists():
        raise SystemExit("No published silver snapshot. Run bronze_to_silver.py first.")
    manifest = json.loads(pointer.read_text(encoding="utf-8"))
    spark = (SparkSession.builder.appName("InspectShopSilver")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        orders = spark.read.parquet(str(root / manifest["paths"]["orders"])).cache()
        actual = {
            "clean_orders": orders.count(),
            "rejected_rows": spark.read.parquet(str(root / manifest["paths"]["rejected"])).count(),
            "duplicate_rows": spark.read.parquet(str(root / manifest["paths"]["duplicates"])).count(),
        }
        for key, count in actual.items():
            if count != manifest["counts"][key]:
                raise RuntimeError(f"Manifest mismatch for {key}.")
        if sum(actual.values()) != manifest["counts"]["input_rows"]:
            raise RuntimeError("Input record accounting does not reconcile.")
        if orders.select("order_id").distinct().count() != actual["clean_orders"]:
            raise RuntimeError("Duplicate order IDs in silver.")
        if orders.select("event_id").distinct().count() != actual["clean_orders"]:
            raise RuntimeError("Duplicate event IDs in silver.")
        if not isinstance(orders.schema["unit_price_eur"].dataType, T.DecimalType):
            raise RuntimeError("Money is not stored as a decimal.")
        if orders.filter(
            F.col("order_total_eur").isNull()
            | (F.col("order_total_eur") != F.col("quantity") * F.col("unit_price_eur"))
        ).count():
            raise RuntimeError("Order total calculation is inconsistent.")
        total_value = orders.agg(F.sum("order_total_eur").alias("value")).first().value or Decimal("0.00")
        if total_value != Decimal(manifest["total_order_value_eur"]):
            raise RuntimeError("Total order value does not match the manifest.")
        if args.expected_count is not None and actual["clean_orders"] != args.expected_count:
            raise RuntimeError(f"Expected {args.expected_count} clean orders, found {actual['clean_orders']}.")
        print(f"INPUT ROWS: {manifest['counts']['input_rows']}", flush=True)
        print(f"CLEAN ORDERS: {actual['clean_orders']}", flush=True)
        print(f"REJECTED ROWS: {actual['rejected_rows']}", flush=True)
        print(f"DUPLICATE ROWS: {actual['duplicate_rows']}", flush=True)
        print(f"TOTAL ORDER VALUE EUR: {total_value}", flush=True)
        orders.printSchema()
        orders.select("order_id", "product_name", "quantity", "unit_price_eur",
                      "order_total_eur", "country", "event_time").orderBy("order_id").show(20, truncate=False)
        print("SILVER VERIFICATION PASSED", flush=True)
        orders.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
