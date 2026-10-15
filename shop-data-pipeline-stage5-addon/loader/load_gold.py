"""Refresh all three analytics tables in one PostgreSQL transaction."""
import os
from pathlib import Path

from psycopg import sql
from psycopg.types.json import Jsonb
from warehouse_common import GRAINS, METRICS, connect, read_gold, print_summary, verify_database


def load_snapshot(conn, manifest, tables):
    with conn.transaction():
        # Serializes cooperating loaders, including retries from future Airflow.
        conn.execute("SELECT pg_advisory_xact_lock(73190501)")
        conn.execute((Path(__file__).parent / "schema.sql").read_text(encoding="utf-8"))
        # The inspector reads pipeline_state first; acquire locks in that same
        # order so a concurrent inspection cannot create a lock-order deadlock.
        conn.execute("LOCK TABLE analytics.pipeline_state, analytics.daily_sales, "
                     "analytics.country_sales, analytics.product_sales IN ACCESS EXCLUSIVE MODE")
        for name, keys in GRAINS.items():
            conn.execute(sql.SQL("DELETE FROM analytics.{}").format(sql.Identifier(name)))
            columns = keys + METRICS
            insert_query = sql.SQL("INSERT INTO analytics.{} ({}) VALUES ({})").format(
                sql.Identifier(name), sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns))
            with conn.cursor() as cursor:
                # Gold contains summary rows, so a simple parameterized insert
                # per row keeps this local loader easy to inspect and debug.
                for row in tables[name]:
                    cursor.execute(insert_query, tuple(row[c] for c in columns))
        conn.execute("""
            INSERT INTO analytics.pipeline_state
                (singleton_id, gold_run_id, source_silver_run_id, gold_manifest)
            VALUES (1, %s, %s, %s)
            ON CONFLICT (singleton_id) DO UPDATE SET
                gold_run_id = EXCLUDED.gold_run_id,
                source_silver_run_id = EXCLUDED.source_silver_run_id,
                gold_manifest = EXCLUDED.gold_manifest,
                loaded_at = CURRENT_TIMESTAMP
        """, (manifest["run_id"], manifest["source_silver"]["run_id"], Jsonb(manifest)))
        counts = verify_database(conn, manifest, tables)
    return counts


def main():
    manifest, tables = read_gold(os.environ.get("GOLD_ROOT", "/opt/project/data/gold"))
    with connect() as conn:
        counts = load_snapshot(conn, manifest, tables)
    print_summary(manifest, counts)
    print("POSTGRES LOAD COMPLETE", flush=True)


if __name__ == "__main__":
    main()
