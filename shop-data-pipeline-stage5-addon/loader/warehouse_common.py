"""Read verified gold snapshots and compare them to PostgreSQL."""
import json
import os
from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import psycopg
from psycopg import sql

GRAINS = {
    "daily_sales": ["event_date"],
    "country_sales": ["event_date", "country"],
    "product_sales": ["event_date", "product_id", "product_name"],
}
METRICS = ["order_count", "units_sold", "unique_customers", "revenue_eur", "average_order_value_eur"]


def connect():
    # Standard PG* environment variables are consumed by libpq.
    return psycopg.connect(autocommit=True, application_name="shop-warehouse")


def read_gold(root, manifest=None):
    root = Path(root)
    if manifest is None:
        pointer = root / "current.json"
        if not pointer.exists():
            raise RuntimeError("No published gold snapshot. Run stage 4 first.")
        manifest = json.loads(pointer.read_text(encoding="utf-8"))
    tables = {}
    expected = manifest["metrics"]
    for name, keys in GRAINS.items():
        dataset = ds.dataset(str(root / manifest["paths"][name]), format="parquet",
                             ignore_prefixes=[".", "_"])
        columns = keys + METRICS
        for money in ["revenue_eur", "average_order_value_eur"]:
            if not pa.types.is_decimal(dataset.schema.field(money).type):
                raise RuntimeError(f"{name}: gold money must use decimals.")
        rows = dataset.to_table(columns=columns).to_pylist()
        if len(rows) != manifest["table_rows"][name]:
            raise RuntimeError(f"{name}: row count differs from gold manifest.")
        seen = set()
        orders, units, revenue = 0, 0, Decimal("0.00")
        for row in rows:
            key = tuple(row[k] for k in keys)
            if key in seen:
                raise RuntimeError(f"{name}: duplicate grouping key.")
            seen.add(key)
            if not isinstance(row["event_date"], date):
                raise RuntimeError(f"{name}: missing or invalid date.")
            if any(not isinstance(row[k], str) or not row[k].strip() for k in keys if k != "event_date"):
                raise RuntimeError(f"{name}: missing grouping field.")
            count, qty, customers = (row[k] for k in ["order_count", "units_sold", "unique_customers"])
            if (any(type(n) is not int for n in [count, qty, customers]) or count <= 0
                    or qty < count or not 1 <= customers <= count):
                raise RuntimeError(f"{name}: invalid count metric.")
            value, average = row["revenue_eur"], row["average_order_value_eur"]
            if (not isinstance(value, Decimal) or not value.is_finite() or value <= 0
                    or not isinstance(average, Decimal) or not average.is_finite()):
                raise RuntimeError(f"{name}: invalid money metric.")
            with localcontext() as context:
                context.prec = 50
                calculated = (value / count).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                revenue += value
            if average != calculated:
                raise RuntimeError(f"{name}: average order value is inconsistent.")
            orders += count
            units += qty
        if (orders != expected["input_orders"] or units != expected["total_units"]
                or revenue != Decimal(expected["total_revenue_eur"])):
            raise RuntimeError(f"{name}: totals differ from gold manifest.")
        tables[name] = rows
    # Country and product totals must also agree with daily totals for each day.
    daily = {r["event_date"]: (r["order_count"], r["units_sold"], r["revenue_eur"])
             for r in tables["daily_sales"]}
    for name in ["country_sales", "product_sales"]:
        grouped = defaultdict(lambda: [0, 0, Decimal("0.00")])
        with localcontext() as context:
            context.prec = 50
            for row in tables[name]:
                group = grouped[row["event_date"]]
                group[0] += row["order_count"]
                group[1] += row["units_sold"]
                group[2] += row["revenue_eur"]
        if {key: tuple(value) for key, value in grouped.items()} != daily:
            raise RuntimeError(f"{name}: daily totals do not reconcile.")
    return manifest, tables


def verify_database(conn, manifest, tables):
    counts = {}
    for name, keys in GRAINS.items():
        columns = keys + METRICS
        query = sql.SQL("SELECT {} FROM analytics.{} ORDER BY {}").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(name),
            sql.SQL(", ").join(map(sql.Identifier, keys)))
        actual = conn.execute(query).fetchall()
        expected = sorted(tuple(r[c] for c in columns) for r in tables[name])
        if actual != expected:
            raise RuntimeError(f"{name}: PostgreSQL rows differ from gold Parquet.")
        counts[name] = len(actual)
    state = conn.execute("SELECT gold_run_id, source_silver_run_id, gold_manifest "
                         "FROM analytics.pipeline_state WHERE singleton_id = 1").fetchone()
    if (state is None or state[0] != manifest["run_id"]
            or state[1] != manifest["source_silver"]["run_id"] or state[2] != manifest):
        raise RuntimeError("Database load provenance differs from the expected snapshot.")
    return counts


def print_summary(manifest, counts):
    print(f"GOLD SNAPSHOT: {manifest['run_id']}", flush=True)
    print(f"INPUT ORDERS: {manifest['metrics']['input_orders']}", flush=True)
    print(f"TOTAL UNITS SOLD: {manifest['metrics']['total_units']}", flush=True)
    print(f"TOTAL REVENUE EUR: {manifest['metrics']['total_revenue_eur']}", flush=True)
    for name, count in counts.items():
        print(f"{name.upper().replace('_', ' ')} ROWS: {count}", flush=True)
