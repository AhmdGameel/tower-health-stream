"""
Batch Analytics Runner (Iceberg -> DuckDB -> dbt -> PostgreSQL).

1. Loads the latest ACID snapshot of `telecom.valid_events` from the Apache Iceberg REST Catalog.
2. Materializes it into DuckDB as `raw_iceberg_telemetry`.
3. Executes `dbt run` and `dbt test` to build and validate analytics marts.
4. Atomically exports the validated marts to PostgreSQL so Grafana reflects live batch insights.
"""
import logging
import os
import subprocess
import sys

import duckdb
import psycopg2
from psycopg2.extras import execute_values
from pyiceberg.catalog.rest import RestCatalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://localhost:8182")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/app/data/tower_health.duckdb")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/app/dbt_project")
POSTGRES_RETENTION_HOURS = int(os.getenv("POSTGRES_RETENTION_HOURS", "168"))  # 7 days hot serving retention

DB_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5434")),
    dbname=os.getenv("POSTGRES_DB", "telecom_analytics"),
    user=os.getenv("POSTGRES_USER", "telecom"),
    password=os.getenv("POSTGRES_PASSWORD", "telecom"),
)

MARTS = ["tower_health_scores", "operator_performance", "regional_risk_index"]


def sync_iceberg_to_duckdb() -> int:
    log.info("Loading Iceberg table telecom.valid_events from %s...", ICEBERG_REST_URI)
    catalog = RestCatalog(name="rest", uri=ICEBERG_REST_URI)
    table = catalog.load_table("telecom.valid_events")
    arrow_table = table.scan().to_arrow()
    row_count = arrow_table.num_rows
    log.info("Loaded %d telemetry rows from Iceberg snapshot", row_count)

    os.makedirs(os.path.dirname(DUCKDB_PATH) or ".", exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    try:
        con.register("iceberg_arrow", arrow_table)
        con.execute("CREATE OR REPLACE TABLE raw_iceberg_telemetry AS SELECT * FROM iceberg_arrow")
    finally:
        con.close()
    return row_count


def run_dbt() -> None:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_PROJECT_DIR
    for cmd in (["dbt", "run"], ["dbt", "test"]):
        log.info("Executing: %s", " ".join(cmd))
        result = subprocess.run(cmd, cwd=DBT_PROJECT_DIR, env=env, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"Command {' '.join(cmd)} failed with exit code {result.returncode}")


def _duckdb_type_to_pg(dtype: str) -> str:
    d = dtype.upper()
    if "BIGINT" in d or "HUGEINT" in d:
        return "BIGINT"
    if "INT" in d:
        return "INTEGER"
    if "DOUBLE" in d or "FLOAT" in d or "DECIMAL" in d:
        return "DOUBLE PRECISION"
    if "BOOL" in d:
        return "BOOLEAN"
    if "TIMESTAMP" in d:
        return "TIMESTAMP"
    return "TEXT"


def export_marts_to_postgres() -> None:
    duck = duckdb.connect(DUCKDB_PATH, read_only=True)
    pg = psycopg2.connect(**DB_CONFIG)
    pg.autocommit = False

    try:
        with pg.cursor() as cur:
            for mart in MARTS:
                cols_info = duck.execute(f"DESCRIBE {mart}").fetchall()
                col_defs = ", ".join(f"{name} {_duckdb_type_to_pg(dtype)}" for name, dtype, *_ in cols_info)
                col_names = [name for name, *_ in cols_info]
                rows = duck.execute(f"SELECT * FROM {mart}").fetchall()

                tmp_table = f"{mart}_staging"
                cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
                cur.execute(f"CREATE TABLE {tmp_table} ({col_defs})")
                if rows:
                    execute_values(
                        cur,
                        f"INSERT INTO {tmp_table} ({', '.join(col_names)}) VALUES %s",
                        rows,
                        page_size=1000,
                    )
                # Atomic swap so Grafana never sees an empty table during refresh
                cur.execute(f"DROP TABLE IF EXISTS {mart}")
                cur.execute(f"ALTER TABLE {tmp_table} RENAME TO {mart}")
                log.info("Exported %d rows to PostgreSQL table '%s'", len(rows), mart)
        pg.commit()
    except Exception:
        pg.rollback()
        raise
    finally:
        pg.close()
        duck.close()


def enforce_postgres_retention() -> None:
    """Keep PostgreSQL lean for fast Grafana queries; full history lives in Apache Iceberg."""
    pg = psycopg2.connect(**DB_CONFIG)
    pg.autocommit = False
    try:
        with pg.cursor() as cur:
            cur.execute(
                "DELETE FROM realtime_alerts WHERE created_at < NOW() - (%s || ' hours')::INTERVAL",
                (POSTGRES_RETENTION_HOURS,),
            )
            deleted_alerts = cur.rowcount
            cur.execute(
                "DELETE FROM regional_outages WHERE created_at < NOW() - (%s || ' hours')::INTERVAL",
                (POSTGRES_RETENTION_HOURS,),
            )
            deleted_outages = cur.rowcount
        pg.commit()
        log.info(
            "Retention policy (%dh): pruned %d old alerts and %d old outages from PostgreSQL",
            POSTGRES_RETENTION_HOURS, deleted_alerts, deleted_outages,
        )
    except Exception as exc:
        pg.rollback()
        log.warning("Skipping retention pruning: %s", exc)
    finally:
        pg.close()


def main() -> None:
    row_count = sync_iceberg_to_duckdb()
    if row_count == 0:
        log.warning("Iceberg table has 0 rows yet; skipping dbt run.")
        sys.exit(0)
    run_dbt()
    export_marts_to_postgres()
    enforce_postgres_retention()
    log.info("Batch analytics refresh completed successfully.")


if __name__ == "__main__":
    main()
