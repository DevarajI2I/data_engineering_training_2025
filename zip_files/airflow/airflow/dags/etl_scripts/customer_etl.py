import pandas as pd
import uuid
from faker import Faker
from datetime import date, datetime
from sqlalchemy import inspect, text
from etl_scripts.common_utils import get_engine, load_csv,upsert_dimension
from etl_logger import get_last_success_run_date, log_etl_run

def run_customer_etl():
    """
    ETL job for dim_customer
    - Loads customer data from CSV
    - Generates fake demographic details
    - Loads or updates into dim_customer
    """

    print(" Starting dim_customer ETL pipeline")

    # -------------------------------------------------------------------------
    #  Setup
    # -------------------------------------------------------------------------
    fake = Faker()
    table_name = "dim_customer"
    engine = get_engine()

    # Get last successful run timestamp
    last_success = get_last_success_run_date(table_name)
    if last_success:
        print(f" Last successful run found: {last_success}")
    else:
        print(" No successful run found — performing full load.")

    # -------------------------------------------------------------------------
    #  Load raw data from CSV
    # -------------------------------------------------------------------------
    df = load_csv()
    df = df.rename(columns={
        "CustomerID": "customer_id",
        "Country": "country",
        "InDate" : "in_date"
    })

    # Convert in_date column to datetime
    df["in_date"] = pd.to_datetime(df["in_date"], errors="coerce")

    # -------------------------------------------------------------------------
    #  Generate fake customer demographic data
    # -------------------------------------------------------------------------
    first_names, last_names, genders, dobs, ages, titles, marital_status, phones, addresses, cities, pincodes, regions, customer_types, channels, languages, emails = ([] for _ in range(16))

    for _ in range(len(df)):
        gender = fake.random_element(["Male", "Female"])
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        age = date.today().year - dob.year - ((date.today().month, date.today().day) < (dob.month, dob.day))

        first_names.append(fake.first_name_male() if gender == "Male" else fake.first_name_female())
        last_names.append(fake.last_name())
        titles.append("Mr." if gender == "Male" else "Ms.")
        genders.append(gender)
        dobs.append(dob)
        ages.append(age)
        marital_status.append(fake.random_element(["Single", "Married", "Divorced"]))
        phones.append(fake.phone_number())
        addresses.append(fake.address().replace("\n", ", "))
        cities.append(fake.city())
        pincodes.append(fake.postcode())
        regions.append(fake.state())
        customer_types.append(fake.random_element(["Retail", "Wholesale", "Corporate"]))
        channels.append(fake.random_element(["Email", "Phone", "SMS", "WhatsApp"]))
        languages.append(fake.random_element(["English", "Spanish", "French", "German", "Hindi"]))
        emails.append(fake.email())

    # Add new columns
    df["first_name"] = first_names
    df["last_name"] = last_names
    df["gender"] = genders
    df["dob"] = dobs
    df["age"] = ages
    df["title"] = titles
    df["marital_status"] = marital_status
    df["phone_no"] = phones
    df["address"] = addresses
    df["city"] = cities
    df["pincode"] = pincodes
    df["region"] = regions
    df["customer_type"] = customer_types
    df["preferred_channel"] = channels
    df["language"] = languages
    df["email"] = emails
    df["updated_at"] = datetime.now()  # Set timestamp here
    df["customer_id"] = df["customer_id"].astype("Int64")
    # -------------------------------------------------------------------------
    #  Filter only new/updated data since last run
    # -------------------------------------------------------------------------
    if last_success:
        new_data = df[df["in_date"] > last_success]
        print(f"Found {len(new_data)} new/updated records since last run.")
    else:
        new_data = df.copy()
        print(f"First load — total records to insert: {len(new_data)}")

    if new_data.empty:
        print("ℹ No new records to insert.")
        log_etl_run(table_name, status='success')
        return

    # -------------------------------------------------------------------------
    #  Prepare final dimension data
    # -------------------------------------------------------------------------
    customer_dim = new_data[[
        "customer_id", "country", "first_name", "last_name", "title", "gender",
        "dob", "age", "marital_status", "phone_no", "email", "address", "city",
        "pincode", "region", "customer_type", "preferred_channel", "language", "updated_at"
    ]].drop_duplicates(subset=['customer_id']).reset_index(drop=True)

    # Generate surrogate key (UUID)
    customer_dim["customer_key"] = [str(uuid.uuid4()) for _ in range(len(customer_dim))]

    # Reorder columns
    customer_dim = customer_dim[["customer_key"] + [col for col in customer_dim.columns if col != "customer_key"]]

    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        print(f"Table '{table_name}' not found. Creating now...")
        create_table_sql = text(f"""
            CREATE TABLE {table_name} (
                customer_key UUID PRIMARY KEY,
                customer_id INT UNIQUE,
                country VARCHAR(100),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                title VARCHAR(10),
                gender VARCHAR(10),
                dob DATE,
                age INT,
                marital_status VARCHAR(50),
                phone_no VARCHAR(50),
                email VARCHAR(255),
                address TEXT,
                city VARCHAR(100),
                pincode VARCHAR(20),
                region VARCHAR(100),
                customer_type VARCHAR(50),
                preferred_channel VARCHAR(50),
                language VARCHAR(50),
                updated_at TIMESTAMP
            );
        """)
        with engine.begin() as conn:
            conn.execute(create_table_sql)
        print(f"✅ Table '{table_name}' created successfully.")

        # Refresh inspector to see the new table
        inspector = inspect(engine)

    # -------------------------------------------------------------------------
    #  Insert or upsert data
    # -------------------------------------------------------------------------
    rows = customer_dim.to_dict(orient="records")
    with engine.begin() as conn:
        if last_success is None:  # first load
            all_cols = list(rows[0].keys())
            upsert_dimension(
                conn=conn,
                table_name=table_name,
                rows=rows,
                pk_column="customer_key",
                delta_key_columns=[],  # no conflict keys for first load
                update_columns=[c for c in all_cols if c != "customer_key"]
            )
            print(f"Inserted {len(rows)} rows into newly created table.")
        else:  # table exists, do incremental upsert
            upsert_dimension(
                conn=conn,
                table_name=table_name,
                rows=rows,
                pk_column="customer_key",
                delta_key_columns=["customer_id"],  # conflict key
                update_columns=[
                    "country", "first_name", "last_name", "title", "gender",
                    "dob", "age", "marital_status", "phone_no", "email", "address",
                    "city", "pincode", "region", "customer_type",
                    "preferred_channel", "language", "updated_at"
                ]
            )
            print(f"Upsert completed: {len(rows)} records processed into {table_name}.")

    # -------------------------------------------------------------------------
    #  Log ETL status
    # -------------------------------------------------------------------------
    #log_etl_run(table_name, status="success")
    print("dim_customer ETL completed successfully.")

# Run locally (for debugging)
if __name__ == "__main__":
    run_customer_etl()
