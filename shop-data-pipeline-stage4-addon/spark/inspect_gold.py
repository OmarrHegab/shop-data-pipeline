"""Verify and display the current gold snapshot against its source silver."""
import argparse
import json
import os
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession
from silver_to_gold import build_tables, load_silver, print_summary, verify_table


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-count", type=int, help="Expected input orders, not aggregate rows.")
    parser.add_argument("--expected-revenue", type=Decimal)
    args = parser.parse_args()
    root = Path(os.environ.get("GOLD_ROOT", "/opt/project/data/gold"))
    pointer = root / "current.json"
    if not pointer.exists():
        raise SystemExit("No published gold snapshot. Run silver_to_gold.py first.")
    manifest = json.loads(pointer.read_text(encoding="utf-8"))
    spark = (SparkSession.builder.appName("InspectShopGold")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.adaptive.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    orders = None
    try:
        orders, _, metrics = load_silver(
            spark, os.environ.get("SILVER_ROOT", "/opt/project/data/silver"), manifest["source_silver"])
        if metrics != manifest["metrics"]:
            raise RuntimeError("Gold manifest metrics differ from source silver.")
        for name, expected in build_tables(orders).items():
            actual = spark.read.parquet(str(root / manifest["paths"][name])).cache()
            try:
                count = verify_table(actual, expected, name, metrics)
                if count != manifest["table_rows"][name]:
                    raise RuntimeError(f"{name}: row count differs from gold manifest.")
                print(f"\n{name.upper()}", flush=True)
                actual.orderBy(*[c for c in expected.columns if c in ["event_date", "country", "product_id", "product_name"]]).show(20, truncate=False)
            finally:
                actual.unpersist()
        if args.expected_count is not None and metrics["input_orders"] != args.expected_count:
            raise RuntimeError(f"Expected {args.expected_count} input orders, found {metrics['input_orders']}.")
        if args.expected_revenue is not None and Decimal(metrics["total_revenue_eur"]) != args.expected_revenue:
            raise RuntimeError("Revenue differs from --expected-revenue.")
        print_summary(manifest)
        print("GOLD VERIFICATION PASSED", flush=True)
    finally:
        if orders is not None:
            orders.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
