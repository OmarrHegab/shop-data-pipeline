"""Read one coherent warehouse snapshot; connection uses standard PG* variables."""
import psycopg
from psycopg.rows import dict_row


def read_snapshot():
    with psycopg.connect(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cur.execute("SET LOCAL statement_timeout = '15s'")
            cur.execute("SELECT gold_run_id, loaded_at FROM analytics.pipeline_state WHERE singleton_id = 1")
            state = cur.fetchone()
            result = {"state": state}
            # Table names are hard-coded, never supplied by a user.
            for name in ("daily_sales", "country_sales", "product_sales"):
                cur.execute(f"SELECT * FROM analytics.{name} ORDER BY event_date")
                result[name] = cur.fetchall()
    return result
