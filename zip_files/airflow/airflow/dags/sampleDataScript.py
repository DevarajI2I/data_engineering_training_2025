import pandas as pd
import os
import csv
import uuid
import random
from sqlalchemy import create_engine, inspect
from faker import Faker
from datetime import date

def run_script():
    # -----------------------------
    # CSV path inside container
    # -----------------------------
    csv_path = "/opt/airflow/data/data.csv"  # your CSV mounted in DAGs folder

    df = pd.read_csv(
        csv_path,
        sep=',',
        encoding='latin1',
        engine='python',
        quoting=csv.QUOTE_MINIMAL,
        on_bad_lines='skip',
        nrows=30  # limit for testing
    )
    print("CSV loaded successfully!")

    # -----------------------------
    # Rename columns
    # -----------------------------
    df = df.rename(columns={
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country"
    })

    # -----------------------------
    # Faker for synthetic data
    # -----------------------------
    fake = Faker()
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

    df["title"] = titles
    df["first_name"] = first_names
    df["last_name"] = last_names
    df["gender"] = genders
    df["dob"] = dobs
    df["age"] = ages
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
    
    # Create Customer Dimension
    customer_dim = df[['customer_id', 'country','first_name', 'last_name', 'title',
        'gender', 'dob', 'age', 'marital_status', 'phone_no', 'email',
        'address', 'city', 'pincode', 'region',
        'customer_type', 'preferred_channel', 'language']].drop_duplicates(subset=['customer_id']).reset_index(drop=True)
    customer_dim['customer_key'] = customer_dim.apply(lambda x: str(uuid.uuid4()), axis=1)
    customer_dim = customer_dim[['customer_key','customer_id', 'country','first_name', 'last_name', 'title',
        'gender', 'dob', 'age', 'marital_status', 'phone_no', 'email',
        'address', 'city', 'pincode', 'region',
        'customer_type', 'preferred_channel', 'language']]

    print("\nCustomer Dimension:")
    
    print("\nProduct Dimension Table InProcess")
    #add fake columns and data
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

    product_dim = df[['stock_code', 'description', 'category', 'sub_category', 'brand', 'color',
                    'size', 'material_type', 'country_of_origin', 'supplier_name',
                    'rating', 'product_weight', 'warranty_period', 'package_type',
                    'certification', 'target_audience', 'usage_type']].drop_duplicates().reset_index(drop=True)
    product_dim['product_key'] = product_dim.apply(lambda x: str(uuid.uuid4()), axis=1)
    product_dim = product_dim[['product_key', 'stock_code', 'description', 'category', 'sub_category', 'brand', 'color',
                            'size', 'material_type', 'country_of_origin', 'supplier_name',
                            'rating', 'product_weight', 'warranty_period', 'package_type',
                            'certification', 'target_audience', 'usage_type']]
    print("\nProduct Dimension:")
    
    # -----------------------------
    # PostgreSQL Connection
    # -----------------------------
    # Inside Docker container (Airflow tasks)
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

    # -----------------------------
    # Delta load for dim_customer
    # -----------------------------
    inspector = inspect(engine)
    if 'dim_customer' in inspector.get_table_names():
        existing_customers = pd.read_sql("SELECT customer_id FROM dim_customer", engine)
        new_customers = customer_dim[~customer_dim['customer_id'].isin(existing_customers['customer_id'])]
    else:
        new_customers = customer_dim

    if not new_customers.empty:
        new_customers.to_sql('dim_customer', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_customers)} new customers")

    if 'product_dim' in inspector.get_table_names():
        existing_customers = pd.read_sql("SELECT customer_id FROM dim_customer", engine)
        new_customers = customer_dim[~customer_dim['customer_id'].isin(existing_customers['customer_id'])]
    else:
        new_customers = customer_dim

    if not new_customers.empty:
        new_customers.to_sql('dim_customer', engine, if_exists='append', index=False)
        print(f"Inserted {len(new_customers)} new customers")