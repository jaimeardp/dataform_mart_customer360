import pandas as pd
import random
import string
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = ".\sa-file.json"


# SETTINGS
PROJECT_ID = "project-id"
DATASET_ID = "customer360_raw"
N_ROWS = 10_000
SEED = datetime.utcnow().timestamp()

# BigQuery client
client = bigquery.Client(project=PROJECT_ID, credentials=service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))

# Helpers
def random_string(length=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def random_date(start_days_ago=30):
    return datetime.utcnow().date() - timedelta(days=random.randint(0, start_days_ago))

# 1. Customers
customers_df = pd.DataFrame({
    "customer_id": [f"CUST_{i:05}" for i in range(N_ROWS)],
    "full_name": [f"Customer_{random_string(4)}" for _ in range(N_ROWS)],
    "updated_at": [datetime.utcnow() - timedelta(minutes=random.randint(0, 5000)) for _ in range(N_ROWS)],
    "is_deleted": [random.choice([False, False, False, True]) for _ in range(N_ROWS)]
})
client.load_table_from_dataframe(customers_df, f"{PROJECT_ID}.{DATASET_ID}.customers_stg").result()

# 2. Addresses
addresses_df = pd.DataFrame({
    "customer_id": customers_df["customer_id"],
    "city": [random.choice(["New York", "Lima", "Paris", "Tokyo", "Bogotá"]) for _ in range(N_ROWS)],
    "state": [random.choice(["NY", "LI", "IDF", "TY", "BO"]) for _ in range(N_ROWS)],
    "country": [random.choice(["USA", "Peru", "France", "Japan", "Colombia"]) for _ in range(N_ROWS)],
    "updated_at": [datetime.utcnow() - timedelta(hours=random.randint(0, 72)) for _ in range(N_ROWS)],
    "is_deleted": [random.choice([False, False, True]) for _ in range(N_ROWS)]
})
client.load_table_from_dataframe(addresses_df, f"{PROJECT_ID}.{DATASET_ID}.addresses_stg").result()

# 3. Orders (simulate multiple orders per customer)
orders_data = []
for cust_id in customers_df["customer_id"]:
    for _ in range(random.randint(1, 3)):
        orders_data.append({
            "customer_id": cust_id,
            "order_id": f"{cust_id}_ORD_{random_string(3)}",
            "total_amount": round(random.uniform(5, 1000), 2),
            "order_date": random_date()
        })
orders_df = pd.DataFrame(orders_data)
client.load_table_from_dataframe(orders_df, f"{PROJECT_ID}.{DATASET_ID}.orders_stg").result()

# 4. Customer Activity
activity_data = []
for cust_id in customers_df["customer_id"]:
    for _ in range(random.randint(1, 2)):
        activity_data.append({
            "customer_id": cust_id,
            "interaction_type": random.choice(["call", "app", "email"]),
            "interaction_date": random_date(15)
        })
activity_df = pd.DataFrame(activity_data)
client.load_table_from_dataframe(activity_df, f"{PROJECT_ID}.{DATASET_ID}.customer_activity_stg").result()

print("✅ Loaded 10,000+ rows into raw tables for Dataform test.")
