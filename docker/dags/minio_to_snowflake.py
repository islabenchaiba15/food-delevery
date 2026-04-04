from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ==============================
# CONFIG
# ==============================
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"      # Updated to match .env
MINIO_SECRET_KEY = "admin123"   # Updated to match .env
BUCKET_NAME = "food-delivery-raw"

SNOWFLAKE_CONFIG = {
    # Pulls directly from Airflow's environment (from docker/.env)
    "user": os.getenv("SNOWFLAKE_USER", "YOUR_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD", "YOUR_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT", "YOUR_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "FOOD_DELIVERY_DW"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),  # Medallion Architecture Landing Zone
    "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
}

LOCAL_TMP_DIR = "/opt/airflow/tmp_minio"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    "minio_to_snowflake_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",  # runs automatically every 10 mins
    catchup=False,
    max_active_runs=1
)

def process_minio_to_snowflake():
    # 1. Connect to MinIO
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    
    if not client.bucket_exists(BUCKET_NAME):
        print(f"Bucket {BUCKET_NAME} does not exist.")
        return

    objects = list(client.list_objects(BUCKET_NAME, recursive=True))
    parquet_objects = [obj for obj in objects if obj.object_name.endswith(".parquet")]
    
    if not parquet_objects:
        print("No new parquet files to process.")
        return
        
    print(f"Found {len(parquet_objects)} raw files in MinIO.")
    
    # 2. Connect to Snowflake (without DB/Schema initially so it doesn't crash if they don't exist)
    init_config = SNOWFLAKE_CONFIG.copy()
    db_name = init_config.pop("database")
    schema_name = init_config.pop("schema")
    
    conn = snowflake.connector.connect(**init_config)
    
    # Ensure Database and BRONZE schema exist before we copy data
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE DATABASE {db_name}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    cursor.execute(f"USE SCHEMA {schema_name}")
    cursor.close()
    
    loaded_count = 0
    
    try:
        # 3. Process each file
        for obj in parquet_objects:
            file_name = obj.object_name
            local_path = os.path.join(LOCAL_TMP_DIR, file_name.replace("/", "_"))
            
            # Download file from MinIO
            client.fget_object(BUCKET_NAME, file_name, local_path)
            
            # Read Parquet
            df = pd.read_parquet(local_path)
            if df.empty:
                os.remove(local_path)
                continue
                
            # Name table based on topic folder (e.g. orders/orders_123.parquet -> ORDERS)
            table_name = file_name.split("/")[0].upper()
            
            # Use Snowflake's native ultra-fast bulk loader (upload to internal stage + COPY INTO)
            success, nchunks, nrows, _ = write_pandas(
                conn, 
                df, 
                table_name,
                auto_create_table=True, # Will actively create tables if they don't exist
                quote_identifiers=False
            )
            
            if success:
                print(f"✅ Loaded {nrows} rows into {table_name}")
                loaded_count += 1
                # DELETE the file from MinIO so we don't process it again next run!
                client.remove_object(BUCKET_NAME, file_name)
                
            # Cleanup local Airflow temp file
            os.remove(local_path)
            
    finally:
        conn.close()
        print(f"🏁 Finished processing {loaded_count} files!")

# We combined fetch & load into one task so we don't pass giant data through Airflow XComs
process_task = PythonOperator(
    task_id="process_and_load",
    python_callable=process_minio_to_snowflake,
    dag=dag
)