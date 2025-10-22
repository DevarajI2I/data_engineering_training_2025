import pandas as pd
import uuid
import random
from datetime import datetime
from sqlalchemy import inspect, text
from etl_scripts.common_utils import get_engine, load_csv
from etl_logger import get_last_success_run_date, log_etl_run

def run_sales_etl():
    """
    ETL job for fact_sales
    - Loads sales data from CSV
    - Merges with dimension keys
    - Calculates total_price, tax, returns, shipping
    - Loads or appends to fact_sales table
    """

    print(" Starting fact_sales ETL pipeline")

    # -------------------------------------------------------------------------
    #  Setup
    # -------------------------------------------------------------------------
    table_name = "fact_sales"
    engine = get_engine()

    # Get last successful run timestamp
    last_success = get_last_success_run_date(table_name)
    if last_success:
        print(f" Last successful run found: {last_success}")
    else:
        print(" No successful run found — performing full load.")

    # -------------------------------------------------------------------------
    #  Load CSV
    # -------------------------------------------------------------------------
    df = load_csv()
    df["InDate"] = pd.to_datetime(df["InDate"], errors="coerce")

    # -------------------------------------------------------------------------
    #  Filter for new data since last ETL run
    # -------------------------------------------------------------------------
    if last_success:
        new_data = df[df["InDate"] > last_success]
        print(f" Found {len(new_data)} new sales records since last run.")
    else:
        new_data = df.copy()
        print(f" First load — total sales records: {len(new_data)}")

    if new_data.empty:
        print(" No new sales records to insert.")
        log_etl_run(table_name, status='success')
        return

    customer_dim = pd.read_sql("select * from dim_customer", engine)
    product_dim = pd.read_sql("select * from dim_product", engine)
    date_dim = pd.read_sql("select * from dim_date", engine)
    date_dim.rename(columns={"date": "invoice_date"}, inplace=True)
    
    new_data.rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Quantity": "quantity",
        "InDate": "invoice_date",   # keep standardized name for consistency
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country"
    }, inplace=True)

    # -------------------------------------------------------------------------
    #  Merge with dimension keys
    # -------------------------------------------------------------------------
    # Assuming customer_dim, product_dim, date_dim are already loaded
    new_data = new_data.merge(customer_dim[['customer_id','customer_key']], on='customer_id', how='left') \
                       .merge(product_dim[['stock_code','product_key']], on='stock_code', how='left') \
                       .merge(date_dim[['invoice_date','date_key']], on='invoice_date', how='left')

    # -------------------------------------------------------------------------
    #  Generate surrogate key and derived columns
    # -------------------------------------------------------------------------
    new_data['sales_key'] = [str(uuid.uuid4()) for _ in range(len(new_data))]
    new_data['total_price'] = new_data['quantity'] * new_data['unit_price']
    new_data['tax_amount'] = new_data['total_price'] * 0.18  # 18% GST
    new_data['returns_flag'] = [random.choice([True] + [False]*9) for _ in range(len(new_data))]

    # Return reasons
    reasons = ["Damaged product", "Wrong item delivered", "Quality not satisfactory", "Late delivery", "Other"]
    new_data['return_reason'] = [random.choice(reasons) if flag else None for flag in new_data['returns_flag']]

    # Shipping methods
    shipping_methods = ["Standard", "Express", "Same-Day", "Overnight", "Pickup Point"]
    new_data['shipping_method'] = [random.choice(shipping_methods) for _ in range(len(new_data))]

    # Select only required columns
    fact_sales = new_data[['sales_key', 'invoice_no', 'customer_key', 'product_key', 'date_key',
                           'quantity', 'unit_price', 'total_price', 'tax_amount',
                           'returns_flag', 'return_reason', 'shipping_method']].drop_duplicates().reset_index(drop=True)

    # Add updated_at timestamp
    fact_sales['updated_at'] = datetime.now()

    # -------------------------------------------------------------------------
    #  Load into PostgreSQL
    # -------------------------------------------------------------------------
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        print(f" {table_name} table not found — creating new table.")
        if_exists_mode = "replace"
    else:
        if_exists_mode = "append"

    fact_sales.to_sql(table_name, engine, if_exists=if_exists_mode, index=False)
    print(f" Successfully loaded {len(fact_sales)} records into {table_name}")

    # -------------------------------------------------------------------------
    #  Update missing timestamps
    # -------------------------------------------------------------------------
    with engine.connect() as conn:
        conn.execute(text(f"UPDATE {table_name} SET updated_at = NOW() WHERE updated_at IS NULL"))

    # -------------------------------------------------------------------------
    #  Log ETL status
    # -------------------------------------------------------------------------
    #log_etl_run(table_name, status="success")
    print(" fact_sales ETL completed successfully.")


# Run locally for testing
if __name__ == "__main__":
    run_sales_etl()
