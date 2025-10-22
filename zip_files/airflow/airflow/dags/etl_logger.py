# etl_logger.py
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

#  PostgreSQL connection (adjust if needed for container)
engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

# ----------------------------------------------------------
# 🧩 Function 1: Log ETL Run (Success or Failure)
# ----------------------------------------------------------
def log_etl_run(table_name, status='success'):
    """
    Logs ETL run status and timestamp for a given table.
    Automatically creates the log table if not present.
    """
    current_time = datetime.now()
    with engine.begin() as conn:
        # Ensure table exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_run_log (
                etl_log_key UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_name VARCHAR(100) NOT NULL,
                last_run_timestamp TIMESTAMP NOT NULL,
                status VARCHAR(20) DEFAULT 'success'
            )
        """))

        # Insert ETL log entry
        conn.execute(
            text("""
                INSERT INTO etl_run_log (table_name, last_run_timestamp, status)
                VALUES (:tbl, :ts, :st)
            """),
            {"tbl": table_name, "ts": current_time, "st": status}
        )

    print(f" Logged ETL run for {table_name}: {status} at {current_time}")


# ----------------------------------------------------------
#  Function 2: Fetch Last Successful Run Timestamp
# ----------------------------------------------------------
def get_last_success_run_date(table_name):
    """
    Fetches the timestamp of the last successful ETL run for the given table.
    Returns None if there has been no successful run yet.
    """
    query = text("""
        SELECT last_run_timestamp
        FROM etl_run_log
        WHERE table_name = :tbl
          AND status = 'success'
        ORDER BY last_run_timestamp DESC
        LIMIT 1
    """)
    df = pd.read_sql(query, engine, params={"tbl": table_name})

    if not df.empty:
        last_run = df.iloc[0, 0]
        print(f" Last SUCCESS run for {table_name}: {last_run}")
        return last_run
    else:
        print(f" No successful run found for {table_name} — will perform full load.")
        return None
