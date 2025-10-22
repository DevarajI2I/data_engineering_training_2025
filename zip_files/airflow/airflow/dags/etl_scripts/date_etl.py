import pandas as pd
from datetime import datetime
from sqlalchemy import inspect, text
from etl_scripts.common_utils import get_engine, load_csv,upsert_dimension
from etl_logger import get_last_success_run_date, log_etl_run


def run_date_etl():
    """
    ETL job for dim_date
    - Loads date data from CSV (using InDate)
    - Extracts Year, Month, Day, Quarter
    - Performs delta load based on last ETL run
    """

    print(" Starting dim_date ETL pipeline")

    # -------------------------------------------------------------------------
    # 1️⃣ Setup
    # -------------------------------------------------------------------------
    table_name = "dim_date"
    engine = get_engine()

    last_success = get_last_success_run_date(table_name)
    if last_success:
        print(f" Last successful run found: {last_success}")
    else:
        print(" No successful run found — performing full load.")

    # -------------------------------------------------------------------------
    #  Load raw CSV data
    # -------------------------------------------------------------------------
    df = load_csv()

    # Convert InDate to datetime
    df["InDate"] = pd.to_datetime(df["InDate"], errors="coerce")

    # Rename if InvoiceDate exists
    if "InvoiceDate" in df.columns:
        df = df.rename(columns={"InvoiceDate": "invoice_date"})
    else:
        df["invoice_date"] = df["InDate"]

    # -------------------------------------------------------------------------
    #  Filter only new records since last successful run
    # -------------------------------------------------------------------------
    if last_success:
        new_data = df[df["InDate"] > last_success].copy()
        print(f" Found {len(new_data)} new date records since last run.")
    else:
        new_data = df.copy()
        print(f" First load — total date records: {len(new_data)}")

    if new_data.empty:
        print(" No new date records to insert.")
        log_etl_run(table_name, status="success")
        return

    # -------------------------------------------------------------------------
    #  Extract date components
    # -------------------------------------------------------------------------
    new_data["year"] = new_data["InDate"].dt.year
    new_data["month"] = new_data["InDate"].dt.month
    new_data["day"] = new_data["InDate"].dt.day
    new_data["quarter"] = new_data["InDate"].dt.quarter
    new_data["updated_at"] = datetime.now()

    #  Convert invoice_date first, then create date_key
    new_data["invoice_date"] = pd.to_datetime(new_data["invoice_date"], errors="coerce")
    new_data["date_key"] = new_data["invoice_date"].dt.strftime("%Y%m%d").astype(int)

    # -------------------------------------------------------------------------
    #  Prepare final dimension table
    # -------------------------------------------------------------------------
    dim_date = new_data[
        ["date_key", "invoice_date", "year", "month", "day", "quarter", "updated_at"]
    ].drop_duplicates().reset_index(drop=True)

    # Rename to lowercase to match DB
    dim_date = dim_date.rename(columns={"invoice_date": "date"})

    # -------------------------------------------------------------------------
    # 6️⃣ Load into PostgreSQL dim_date table
    # -------------------------------------------------------------------------
    with engine.begin() as conn:
        rows = dim_date.to_dict(orient="records")

        upsert_dimension(
            conn=conn,
            table_name=table_name,
            rows=rows,
            pk_column="date_key",               # Surrogate key (unique per date)
            delta_key_columns=["date"],     # Business key to detect duplicates
            update_columns=["year", "month", "day", "quarter", "updated_at"]
        )

    print(f"✅ Upsert completed: {len(dim_date)} records processed into {table_name}.")

    # -------------------------------------------------------------------------
    #  Log ETL status
    # -------------------------------------------------------------------------
    #log_etl_run(table_name, status="success")
    print(" dim_date ETL completed successfully.")


# Run locally for testing
if __name__ == "__main__":
    run_date_etl()
