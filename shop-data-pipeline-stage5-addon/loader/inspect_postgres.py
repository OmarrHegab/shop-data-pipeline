"""Verify database contents against the loaded gold snapshot."""
import argparse
import json
import os
from decimal import Decimal
from pathlib import Path

from warehouse_common import connect, read_gold, print_summary, verify_database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--expected-revenue", type=Decimal)
    args = parser.parse_args()
    root = Path(os.environ.get("GOLD_ROOT", "/opt/project/data/gold"))
    with connect() as conn:
        with conn.transaction():
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            state = conn.execute("SELECT gold_manifest FROM analytics.pipeline_state "
                                 "WHERE singleton_id = 1").fetchone()
            if state is None:
                raise RuntimeError("No database load recorded. Run load_gold.py first.")
            manifest, tables = read_gold(root, state[0])
            counts = verify_database(conn, manifest, tables)
            metrics = manifest["metrics"]
            if args.expected_count is not None and metrics["input_orders"] != args.expected_count:
                raise RuntimeError(f"Expected {args.expected_count} orders, found {metrics['input_orders']}.")
            if args.expected_revenue is not None and Decimal(metrics["total_revenue_eur"]) != args.expected_revenue:
                raise RuntimeError("Revenue differs from --expected-revenue.")
            rows = conn.execute("SELECT country, SUM(order_count), SUM(units_sold), SUM(revenue_eur) "
                                "FROM analytics.country_sales GROUP BY country ORDER BY SUM(revenue_eur) DESC").fetchall()
    print_summary(manifest, counts)
    print("COUNTRY | ORDERS | UNITS | REVENUE EUR", flush=True)
    for country, orders, units, value in rows:
        print(f"{country} | {orders} | {units} | {value}", flush=True)
    current = root / "current.json"
    if current.exists() and json.loads(current.read_text(encoding="utf-8"))["run_id"] != manifest["run_id"]:
        print("A newer gold snapshot is available. Run the loader to refresh PostgreSQL.", flush=True)
    print("POSTGRES VERIFICATION PASSED", flush=True)


if __name__ == "__main__":
    main()
