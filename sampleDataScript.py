import pandas as pd
import os
import csv
import uuid
import random
from sqlalchemy import create_engine
from faker import Faker
from datetime import date

# CSV path file
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, "data.csv")  # use correct filename

# Read CSV safely as comma-separated
df = pd.read_csv(
    csv_path,
    sep=',',                 # <-- FIXED: comma-separated
    encoding='latin1',
    engine='python',
    quoting=csv.QUOTE_MINIMAL,  # handle quotes properly
    on_bad_lines='skip'
)

print("CSV loaded successfully!")
#print(df.head(10))   # check first 10 rows
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

fake = Faker()
first_names, last_names, genders, dobs, ages, titles, marital_status, phones, addresses, cities, pincodes, regions, customer_types, channels, languages,emails = ([] for _ in range(16))

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
print("\nColumns:", df.columns)

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
print(customer_dim.head())

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
print(product_dim.head())

# Convert InvoiceDate to datetime in df
df['invoice_date'] = pd.to_datetime(df['invoice_date'], errors='coerce')

date_dim = df[['invoice_date']].drop_duplicates().reset_index(drop=True)
date_dim['invoice_date'] = pd.to_datetime(date_dim['invoice_date'])
# Create surrogate key in YYYYMMDD format
date_dim['date_key'] = date_dim['invoice_date'].dt.strftime('%Y%m%d').astype(int)
date_dim['year'] = date_dim['invoice_date'].dt.year
date_dim['month'] = date_dim['invoice_date'].dt.month
date_dim['day'] = date_dim['invoice_date'].dt.day
date_dim = date_dim[['date_key', 'invoice_date', 'year', 'month', 'day']]
print("\nDate Dimension:")
print(date_dim.head())

# Ensure unique keys in dimension tables
customer_dim = customer_dim.drop_duplicates(subset=['customer_id']).reset_index(drop=True)
product_dim = product_dim.drop_duplicates(subset=['stock_code']).reset_index(drop=True)
date_dim = date_dim.drop_duplicates(subset=['invoice_date']).reset_index(drop=True)

# Merge with dimension keys
fact_df = df.merge(customer_dim[['customer_id','customer_key']], on='customer_id', how='left') \
            .merge(product_dim[['stock_code','product_key']], on='stock_code', how='left') \
            .merge(date_dim[['invoice_date','date_key']], on='invoice_date', how='left')

# Create surrogate FactKey
fact_df['sales_key'] = fact_df.apply(lambda x: str(uuid.uuid4()), axis=1)
#create total_price based on qty * unit_price
fact_df['total_price'] = fact_df['quantity'] * fact_df['unit_price']
#random tax value for all products 18% GST
fact_df['tax_amount'] = fact_df['total_price'] * 0.18
# Add return flag (10% chance of return)
fact_df['returns_flag'] = [random.choice([True, False, False, False, False, False, False, False, False, False]) for _ in range(len(fact_df))]

# Return reasons (only if returns_flag = True)
reasons = ["Damaged product", "Wrong item delivered", "Quality not satisfactory", "Late delivery", "Other"]
fact_df['return_reason'] = [
    random.choice(reasons) if flag else None for flag in fact_df['returns_flag']
]

# Shipping methods
shipping_methods = ["Standard", "Express", "Same-Day", "Overnight", "Pickup Point"]
fact_df['shipping_method'] = [random.choice(shipping_methods) for _ in range(len(fact_df))]
# Select columns for fact table
fact_table = fact_df[['sales_key', 'invoice_no', 'customer_key', 'product_key', 'date_key',
                       'quantity', 'unit_price', 'total_price',
                      'tax_amount', 'returns_flag', 'return_reason', 'shipping_method']]
print("\n Fact Sales Table")
print(fact_table.head())

# Replace with your PostgreSQL credentials
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/sales_database')
  
customer_dim = customer_dim.drop_duplicates(subset=['customer_id']).reset_index(drop=True)
product_dim = product_dim.drop_duplicates(subset=['stock_code']).reset_index(drop=True)
date_dim = date_dim.drop_duplicates(subset=['invoice_date']).reset_index(drop=True)

# Load dimension tables
customer_dim.to_sql('dim_customer', engine, if_exists='replace', index=False)
product_dim.to_sql('dim_product', engine, if_exists='replace', index=False)
date_dim.to_sql('dim_date', engine, if_exists='replace', index=False)

# Load fact table
fact_table.to_sql('fact_sales', engine, if_exists='replace', index=False)