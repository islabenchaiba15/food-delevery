import io
import pandas as pd
from minio import Minio

client = Minio('localhost:9002', access_key='admin', secret_key='admin123', secure=False)
bucket = 'food-delivery-raw'

print(f"Scanning bucket '{bucket}'...")
objects = list(client.list_objects(bucket, recursive=True))
print(f"Found {len(objects)} Parquet files. Downloading and counting rows (this takes ~10-20 seconds)...")

total_rows = 0
for i, obj in enumerate(objects, 1):
    response = client.get_object(bucket, obj.object_name)
    df = pd.read_parquet(io.BytesIO(response.read()))
    total_rows += len(df)
    
    # friendly progress update
    if i % 50 == 0 or i == len(objects):
        print(f"Processed {i}/{len(objects)} files... ({total_rows} rows so far)")
        
    response.close()
    response.release_conn()

print("\n" + "="*40)
print(f"✅ VERIFICATION COMPLETE!")
print(f"Total files: {len(objects)}")
print(f"Total events safely saved in MinIO: {total_rows}")
print("="*40)
