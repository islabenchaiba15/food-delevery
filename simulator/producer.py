import json
import pandas as pd
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================
# NUMPY-SAFE JSON ENCODER
# ==============================
class NumpySafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        return super().default(obj)

# ==============================
# CONFIG
# ==============================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
DATA_PATH = "C:/Users/21365/Downloads/food/"
TOPICS = [
    "orders", "payments", "delivery", "reviews",
    "users", "billing", "promotions", "drivers",
    "restaurants", "customers"
]

# ==============================
# KAFKA PRODUCER
# ==============================
failed_sends = []

def on_send_error(excp, topic="unknown"):
    failed_sends.append((topic, str(excp)))
    print(f"❌ SEND ERROR on topic '{topic}': {excp}")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, cls=NumpySafeEncoder).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks='all',             # ✅ wait for all replicas to ack (prevents silent loss)
    retries=5,              # ✅ retry on transient errors
    linger_ms=50,           # ✅ small batching window
    batch_size=64 * 1024,   # ✅ 64KB batches
    buffer_memory=128 * 1024 * 1024,  # ✅ 128MB buffer for 448k messages
    max_block_ms=60000,     # ✅ block up to 60s if buffer is full (instead of dropping)
)

# ==============================
# UTILS
# ==============================
def clean_row(row_dict):
    return {k: (None if pd.isna(v) else v) for k, v in row_dict.items()}

def enrich_event(data, event_type):
    data["event_type"] = event_type
    data["event_time"] = datetime.utcnow().isoformat()
    return data

def send_event(topic, key, value):
    future = producer.send(topic, key=key, value=value)
    future.add_errback(lambda excp: on_send_error(excp, topic))

# ==============================
# LOAD CSVs
# ==============================
dataframes = {key: pd.read_csv(DATA_PATH + f"{key}.csv") for key in TOPICS}

# ==============================
# SEQUENTIAL SENDING
# ==============================
def load_all_sequential():
    print("⚡ LOADING ALL DATA SEQUENTIALLY...")

    for topic in TOPICS:
        df = dataframes[topic]
        print(f"Processing {topic} ({len(df)} rows)...")

        for _, row in df.iterrows():
            data = clean_row(row.to_dict())
            event = enrich_event(data, f"{topic}_loaded")
            key_val = list(data.values())[0]
            send_event(topic, key_val, event)

        producer.flush()
        print(f"✅ Finished {topic} and flushed.")

    if failed_sends:
        print(f"\n⚠️  WARNING: {len(failed_sends)} messages failed to send!")
        for topic, err in failed_sends[:10]:
            print(f"   - [{topic}] {err}")
    else:
        print("🎉 ALL DATA LOADED SUCCESSFULLY — 0 failures")

# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    print("🚀 PRODUCER STARTED")

    try:
        load_all_sequential()

    except KeyboardInterrupt:
        print("\n🛑 STOPPED")

    finally:
        producer.flush()
        producer.close()
        print("✅ PRODUCER CLOSED")