from google.cloud import bigquery
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = ".\sa-file.json"

# Configuración
project_id = "project-id"
dataset_id = "customer360_raw"

client = bigquery.Client(project=project_id, credentials=service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE))

# Crear el dataset si no existe
dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
dataset_ref.location = "US"
client.create_dataset(dataset_ref, exists_ok=True)
print(f"✅ Dataset '{dataset_id}' listo.")

# Definición de tablas RAW
tables = {
    "customers_stg": [
        bigquery.SchemaField("customer_id", "STRING", description="Customer unique ID."),
        bigquery.SchemaField("full_name", "STRING", description="Full name."),
        bigquery.SchemaField("updated_at", "TIMESTAMP", description="CDC timestamp."),
        bigquery.SchemaField("is_deleted", "BOOLEAN", description="Soft delete flag.")
    ],
    "addresses_stg": [
        bigquery.SchemaField("customer_id", "STRING", description="Customer unique ID."),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("is_deleted", "BOOLEAN")
    ],
    "orders_stg": [
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("order_id", "STRING"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("order_date", "DATE")
    ],
    "customer_activity_stg": [
        bigquery.SchemaField("customer_id", "STRING"),
        bigquery.SchemaField("interaction_type", "STRING"),
        bigquery.SchemaField("interaction_date", "DATE")
    ]
}

# Crear tablas
for table_name, schema in tables.items():
    table_ref = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"✅ Tabla '{table_name}' creada o ya existente.")
