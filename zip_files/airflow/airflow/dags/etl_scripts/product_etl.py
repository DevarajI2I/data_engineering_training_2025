import pandas as pd
import uuid
import random
from faker import Faker
from datetime import datetime
from sqlalchemy import inspect, text
from etl_scripts.common_utils import get_engine, load_csv,upsert_dimension
from etl_logger import get_last_success_run_date, log_etl_run


def run_product_etl():
    """
    ETL job for dim_product
    - Loads product data from CSV
    - Generates fake product attributes
    - Performs delta load based on last ETL run
    """

    print("🚀 Starting dim_product ETL pipeline")

    # -------------------------------------------------------------------------
    #  Setup
    # -------------------------------------------------------------------------
    fake = Faker()
    table_name = "dim_product"
    engine = get_engine()

    # Get last successful run timestamp
    last_success = get_last_success_run_date(table_name)
    if last_success:
        print(f" Last successful run found: {last_success}")
    else:
        print(" No successful run found — performing full load.")

    # -------------------------------------------------------------------------
    #  Load raw product data from CSV
    # -------------------------------------------------------------------------
    df = load_csv()
    df = df.rename(columns={
        "StockCode": "stock_code",
        "Description": "description",
        "InDate": "in_date"
    })

    # Convert InDate column to datetime for filtering
    df["in_date"] = pd.to_datetime(df["in_date"], errors="coerce")

    # -------------------------------------------------------------------------
    #  Generate fake product attributes
    # -------------------------------------------------------------------------
    categories, sub_categories, brands, colors, sizes, materials, origins, suppliers, ratings, weights, warranties, packages, certs, audiences, usages = ([] for _ in range(15))

    for _ in range(len(df)):
        categories.append(fake.random_element(["Electronics", "Clothing", "Furniture", "Toys", "Books"]))
        sub_categories.append(fake.word())
        brands.append(fake.company())
        colors.append(fake.color_name())
        sizes.append(fake.random_element(["Small", "Medium", "Large", "XL"]))
        materials.append(fake.random_element(["Plastic", "Metal", "Wood", "Cotton", "Leather"]))
        origins.append(fake.country())
        suppliers.append(fake.company())
        ratings.append(round(random.uniform(1, 5), 2))
        weights.append(round(random.uniform(0.2, 25.0), 2))
        warranties.append(fake.random_element(["6 months", "1 year", "2 years", "3 years"]))
        packages.append(fake.random_element(["Box", "Bag", "Carton", "Wrapper"]))
        certs.append(fake.random_element(["ISO", "CE", "BIS", "FDA", "None"]))
        audiences.append(fake.random_element(["Kids", "Adults", "Unisex", "Professionals"]))
        usages.append(fake.random_element(["Personal", "Commercial", "Industrial"]))

    df["category"] = categories
    df["sub_category"] = sub_categories
    df["brand"] = brands
    df["color"] = colors
    df["size"] = sizes
    df["material_type"] = materials
    df["country_of_origin"] = origins
    df["supplier_name"] = suppliers
    df["product_weight"] = weights
    df["warranty_period"] = warranties
    df["package_type"] = packages
    df["certification"] = certs
    df["target_audience"] = audiences
    df["usage_type"] = usages
    df["rating"] = ratings
    df["updated_at"] = datetime.now()

    # -------------------------------------------------------------------------
    #  Filter only new/updated products since last run
    # -------------------------------------------------------------------------
    if last_success:
        new_data = df[df["in_date"] > last_success]
        print(f"🔍 Found {len(new_data)} new/updated product records since last run.")
    else:
        new_data = df.copy()
        print(f" First load — total records to insert: {len(new_data)}")

    if new_data.empty:
        print(" No new product records to insert.")
        log_etl_run(table_name, status='success')
        return
    
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key UUID PRIMARY KEY,
                stock_code VARCHAR(50) UNIQUE NOT NULL,
                description TEXT,
                category VARCHAR(100),
                sub_category VARCHAR(100),
                brand VARCHAR(100),
                color VARCHAR(50),
                size VARCHAR(20),
                material_type VARCHAR(100),
                country_of_origin VARCHAR(100),
                supplier_name VARCHAR(150),
                rating FLOAT,
                product_weight FLOAT,
                warranty_period VARCHAR(50),
                package_type VARCHAR(50),
                certification VARCHAR(50),
                target_audience VARCHAR(50),
                usage_type VARCHAR(50),
                updated_at TIMESTAMP
            )
        """))
    print("✅ Verified: dim_product table exists in database.")


    # -------------------------------------------------------------------------
    #  Prepare final dimension data
    # -------------------------------------------------------------------------
    product_dim = new_data[[
        "stock_code", "description", "category", "sub_category", "brand", "color",
        "size", "material_type", "country_of_origin", "supplier_name", "rating",
        "product_weight", "warranty_period", "package_type", "certification",
        "target_audience", "usage_type", "updated_at"
    ]].drop_duplicates(subset=['stock_code']).reset_index(drop=True)

    # Generate surrogate key (UUID)
    product_dim["product_key"] = [str(uuid.uuid4()) for _ in range(len(product_dim))]

    # Reorder columns
    product_dim = product_dim[["product_key"] + [col for col in product_dim.columns if col != "product_key"]]

    # -------------------------------------------------------------------------
    #  Load into PostgreSQL dim_product table
    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    #  Load into PostgreSQL dim_product table (using UPSERT)
    # -------------------------------------------------------------------------
    with engine.begin() as conn:
        rows = product_dim.to_dict(orient="records")

        upsert_dimension(
            conn=conn,
            table_name="dim_product",
            rows=rows,
            pk_column="product_key",                # Surrogate UUID key
            delta_key_columns=["stock_code"],       # Business key to detect duplicates
            update_columns=[
                "description", "category", "sub_category", "brand", "color", "size",
                "material_type", "country_of_origin", "supplier_name", "rating",
                "product_weight", "warranty_period", "package_type", "certification",
                "target_audience", "usage_type", "updated_at"
            ]
        )

    print(f"Upsert completed: {len(product_dim)} records processed into dim_product.")

    # -------------------------------------------------------------------------
    # 8️⃣ Log ETL status
    # -------------------------------------------------------------------------
    #log_etl_run(table_name, status="success")
    print(" dim_product ETL completed successfully.")


# Run locally for testing
if __name__ == "__main__":
    run_product_etl()
