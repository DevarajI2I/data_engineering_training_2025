import pandas as pd
from sqlalchemy import create_engine,text
from typing import List, Dict

def get_engine():
    """Return PostgreSQL SQLAlchemy engine"""
    return create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

def load_csv():
    """Load main CSV file"""
    csv_path = "/opt/airflow/data/data.csv"
    df = pd.read_csv(csv_path, sep=',', encoding='latin1', on_bad_lines='skip', engine='python')
    df = df.head(10)
    return df

def _hyphen_uuid(u: str) -> str:
    s = u.replace("-", "").lower()
    if len(s) != 32:
        return u
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"

def upsert_dimension(
    conn,
    table_name: str,
    rows: List[Dict],
    pk_column: str,
    delta_key_columns: List[str],
    update_columns: List[str],
) -> None:
    """
    Generic batch upsert using ON CONFLICT (...) DO UPDATE.
    - conn: SQLAlchemy connection (inside engine.begin())
    - table_name: target table name
    - rows: list of dicts (column->value)
    - pk_column: surrogate PK column name (e.g. customer_key)
    - delta_key_columns: columns used for conflict detection (business key(s))
    - update_columns: columns to update on conflict
    """
    if not rows:
        print("No rows to upsert.")
        return

    # prepare rows: hyphenate uuid if provided, and drop bad rows missing delta keys
    rows_prepped = []
    for r in rows:
        nr = dict(r)
        # fix uuid format if needed
        pkval = nr.get(pk_column)
        if isinstance(pkval, str):
            nr[pk_column] = _hyphen_uuid(pkval)
        # skip rows missing any delta key
        if not all(nr.get(k) not in (None, "") for k in delta_key_columns):
            continue
        rows_prepped.append(nr)

    if not rows_prepped:
        print("No valid rows after filtering delta keys.")
        return

    # Build SQL - use named params (sqlalchemy.text handles execution with list of dicts)
    all_columns = [pk_column] + delta_key_columns + update_columns
    placeholders = ", ".join(f":{c}" for c in all_columns)
    assignments = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_columns)
    conflict_cols = ", ".join(delta_key_columns)

    sql = text(
        f"""
        INSERT INTO {table_name} ({', '.join(all_columns)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols}) DO UPDATE
        SET {assignments}
        """
    )

    # Execute as batch; SQLAlchemy will execute many rows in one go
    conn.execute(sql, rows_prepped)
    print(f"Upserted {len(rows_prepped)} rows into {table_name}")
