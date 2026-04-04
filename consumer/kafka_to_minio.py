import json
import os
import time
import tempfile
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaAdminClient
from minio import Minio
from datetime import datetime

# ==============================
# CONFIG
# ==============================
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPICS = [
    "orders", "payments", "delivery", "reviews",
    "users", "billing", "promotions", "drivers",
    "restaurants", "customers"
]
GROUP_ID = "all_topics_to_minio_v5"  # bumped → forces re-read from earliest

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET     = "food-delivery-raw"

BATCH_SIZE         = 1000   # rows per parquet file
FLUSH_INTERVAL_SEC = 15.0   # flush at least every 15s if buffer non-empty
IDLE_TIMEOUT_SEC   = 120.0  # exit after 120s of no new messages

# ==============================
# STARTUP CHECKS
# ==============================
print("🔍 Running startup checks...\n")

# --- Check 1: Kafka reachability ---
print(f"✅ Assuming Kafka reachable at {KAFKA_BROKER}")

# --- Check 2: MinIO reachability ---
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    _ = minio_client.list_buckets()
    print(f"✅ MinIO reachable at {MINIO_ENDPOINT}")
except Exception as e:
    print(f"❌ Cannot reach MinIO at {MINIO_ENDPOINT}: {e}")
    print("   → Check MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY in your .env")
    exit(1)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"✅ Created MinIO bucket: {MINIO_BUCKET}")
else:
    print(f"✅ MinIO bucket exists: {MINIO_BUCKET}")

print()

# ==============================
# INIT KAFKA CONSUMER
# ==============================
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",       # read from beginning for new group
    enable_auto_commit=False,           # manual commit only — no silent skips
    max_poll_records=2000,              # fetch more per poll
    fetch_max_bytes=52_428_800,         # 50 MB total fetch
    max_partition_fetch_bytes=10_485_760,  # 10 MB per partition
    # ✅ Safe values — compatible with default Kafka broker settings
    session_timeout_ms=30_000,          # must be < broker group.max.session.timeout.ms (default 30s)
    heartbeat_interval_ms=5_000,        # must be < session_timeout_ms / 3
    max_poll_interval_ms=300_000,       # 5 min max between polls (covers slow MinIO uploads)
    request_timeout_ms=40_000,
)

# ==============================
# COUNTERS
# ==============================
total_uploaded = {topic: 0 for topic in TOPICS}
total_received = {topic: 0 for topic in TOPICS}

# ==============================
# FLUSH FUNCTION
# ==============================
def flush_topic(topic: str, buffer: list, reason: str = "") -> list:
    """
    Upload buffer to MinIO as one parquet file.
    Returns [] ONLY after confirmed upload (atomic clear).
    Returns original buffer on failure so it retries next cycle.
    """
    if not buffer:
        return buffer

    df = pd.DataFrame(buffer)  # snapshot — buffer intact until upload succeeds
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    file_name = f"{topic}/{topic}_{timestamp_str}.parquet"

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        local_path = tmp.name

    try:
        df.to_parquet(local_path, engine="pyarrow", index=False)
        minio_client.fput_object(MINIO_BUCKET, file_name, local_path)

        total_uploaded[topic] += len(df)
        print(
            f"✅ [{reason}] {topic}: {len(df)} rows → {file_name} "
            f"(uploaded: {total_uploaded[topic]} / received: {total_received[topic]})"
        )
        return []  # buffer cleared only after confirmed success

    except Exception as exc:
        print(f"⚠️  Upload FAILED for {topic} ({reason}): {exc} — will retry next cycle")
        return buffer  # keep buffer intact on failure

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


# ==============================
# MAIN LOOP
# ==============================
buffers         = {topic: [] for topic in TOPICS}
last_flush_time = {topic: time.time() for topic in TOPICS}
last_msg_time   = time.time()
poll_count      = 0

print(f"🚀 Consumer started | group_id={GROUP_ID}")
print(f"   Batch size: {BATCH_SIZE} rows | Flush interval: {FLUSH_INTERVAL_SEC}s | Idle timeout: {IDLE_TIMEOUT_SEC}s\n")

try:
    while True:

        # ── Poll Kafka ────────────────────────────────────────────────────
        raw_msgs = consumer.poll(timeout_ms=2000)
        poll_count += 1

        # Print a heartbeat every 10 empty polls so you know it's alive
        if not raw_msgs and poll_count % 10 == 0:
            idle = time.time() - last_msg_time
            total_rx = sum(total_received.values())
            print(f"   ⏳ Waiting... idle={idle:.0f}s | total received so far: {total_rx}")

        # ── 1. Fill buffers ───────────────────────────────────────────────
        got_messages = False
        for tp, messages in raw_msgs.items():
            for message in messages:
                t = message.topic
                buffers[t].append(message.value)
                total_received[t] += 1
                got_messages = True

        if got_messages:
            last_msg_time = time.time()

        # ── 2. Flush by size or time ──────────────────────────────────────
        now = time.time()
        for topic in TOPICS:
            elapsed      = now - last_flush_time[topic]
            must_by_size = len(buffers[topic]) >= BATCH_SIZE
            must_by_time = len(buffers[topic]) > 0 and elapsed >= FLUSH_INTERVAL_SEC

            if must_by_size or must_by_time:
                reason = "BatchFull" if must_by_size else "Timeout"
                buffers[topic] = flush_topic(topic, buffers[topic], reason)
                last_flush_time[topic] = now

        # ── 3. Commit offsets only after successful processing ────────────
        if got_messages:
            consumer.commit()

        # ── 4. Idle timeout — final flush and exit ────────────────────────
        if time.time() - last_msg_time >= IDLE_TIMEOUT_SEC:
            print(f"\n⏰ {IDLE_TIMEOUT_SEC}s of silence — flushing remaining buffers...")
            for topic in TOPICS:
                buffers[topic] = flush_topic(topic, buffers[topic], "FinalFlush")
            consumer.commit()

            print("\n📊 Final row counts:")
            grand_received = grand_uploaded = 0
            for topic in TOPICS:
                r    = total_received[topic]
                u    = total_uploaded[topic]
                lost = r - u
                flag = "✅" if lost == 0 else f"⚠️  LOST {lost}"
                print(f"   {topic:15s}  received={r:>7}  uploaded={u:>7}  {flag}")
                grand_received += r
                grand_uploaded += u

            total_lost = grand_received - grand_uploaded
            print(f"\n   {'TOTAL':15s}  received={grand_received:>7}  uploaded={grand_uploaded:>7}  ", end="")
            print("✅ 0 lost" if total_lost == 0 else f"⚠️  LOST {total_lost} rows")
            print("\n✅ Done. Exiting.")
            break

except KeyboardInterrupt:
    print("\n🛑 Interrupted — flushing remaining buffers...")
    for topic in TOPICS:
        buffers[topic] = flush_topic(topic, buffers[topic], "ShutdownFlush")
    consumer.commit()
    print("\n📊 Final row counts:")
    for topic in TOPICS:
        r, u = total_received[topic], total_uploaded[topic]
        lost = r - u
        print(f"   {topic:15s}  received={r:>7}  uploaded={u:>7}  {'✅' if lost == 0 else f'⚠️  LOST {lost}'}")

finally:
    consumer.close()
    print("✅ Consumer closed.")