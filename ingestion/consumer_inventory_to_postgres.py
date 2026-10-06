"""
consumer_inventory_to_postgres.py
---------------------------------
Kafka Consumer: đọc ảnh chụp tồn kho từ topic `inventory.logs` (do producer.py phát định kỳ)
và upsert vào bảng `Fact_Inventory_Daily` trên PostgreSQL.

Mỗi SKU giữ đúng 1 dòng / ngày (unique index `uq_fact_inventory_product_date`):
snapshot mới hơn trong cùng ngày ghi đè snapshot cũ → bảng luôn phản ánh tồn kho
mới nhất, là nguồn `current_stock` cho API /inventory/reorder-alert.

Offset Kafka chỉ được commit sau khi transaction PostgreSQL thành công (at-least-once,
upsert idempotent nên đọc lại không gây trùng).

Chạy:
  python ingestion/consumer_inventory_to_postgres.py
"""

import json
import logging
import os
import signal
import sys
import time

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from warehouse.etl.load import get_engine  # noqa: E402

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
TOPIC_INVENTORY = os.getenv("KAFKA_TOPIC_INVENTORY", "inventory.logs")
BUSINESS_TZ = os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh")
BATCH_SIZE = int(os.getenv("INVENTORY_CONSUMER_BATCH_SIZE", "200"))
BATCH_TIMEOUT_SEC = int(os.getenv("INVENTORY_CONSUMER_BATCH_TIMEOUT", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka-consumer-inventory")

_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("Nhận tín hiệu dừng (%s), đang ghi batch cuối...", sig)
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def build_consumer(retries: int = 5, retry_delay: float = 3.0) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC_INVENTORY,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="inventory-postgres-writer-group",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )
            logger.info("Kết nối Kafka Consumer thành công: %s", KAFKA_BOOTSTRAP_SERVERS)
            return consumer
        except KafkaError as e:
            logger.warning("Kafka chưa sẵn sàng (lần %d/%d): %s", attempt, retries, e)
            time.sleep(retry_delay)
    raise ConnectionError(f"Không thể kết nối Kafka sau {retries} lần thử: {KAFKA_BOOTSTRAP_SERVERS}")


def prepare_snapshots(records: list[dict]) -> pd.DataFrame:
    """
    Chuẩn hóa batch snapshot: quy đổi thời điểm chụp về ngày nghiệp vụ và
    giữ snapshot mới nhất cho mỗi (sku, ngày).
    """
    df = pd.DataFrame(records)
    if df.empty or "sku" not in df.columns:
        return pd.DataFrame()

    ts = pd.to_datetime(df.get("loaded_at"), errors="coerce", utc=True)
    ts = ts.fillna(pd.Timestamp.now(tz="UTC"))
    df["snapshot_ts"] = ts.dt.tz_convert(BUSINESS_TZ)
    df["snapshot_date"] = df["snapshot_ts"].dt.date

    df = (
        df.dropna(subset=["sku"])
        .sort_values("snapshot_ts")
        .drop_duplicates(subset=["sku", "snapshot_date"], keep="last")
    )
    return df


def upsert_inventory(engine, df: pd.DataFrame) -> int:
    """Upsert Dim_Products (nếu SKU mới) và Fact_Inventory_Daily trong 1 transaction."""
    if df.empty:
        return 0

    products = [
        {
            "sku": str(r["sku"]),
            "name": r.get("product_name") or str(r["sku"]),
            "cat": r.get("category") or "Chung",
        }
        for r in df[["sku", "product_name", "category"]].to_dict(orient="records")
    ] if {"product_name", "category"}.issubset(df.columns) else [
        {"sku": str(s), "name": str(s), "cat": "Chung"} for s in df["sku"].unique()
    ]

    rows = [
        {
            "sku": str(r["sku"]),
            "full_date": r["snapshot_date"],
            "stock": int(r.get("stock_on_hand") or 0),
            "ss": int(r["safety_stock"]) if pd.notna(r.get("safety_stock")) else None,
            "rop": int(r["reorder_point"]) if pd.notna(r.get("reorder_point")) else None,
        }
        for r in df.to_dict(orient="records")
    ]

    with engine.begin() as conn:
        # SKU chưa từng có đơn hàng vẫn phải có product_key để lưu tồn kho
        conn.execute(text("""
            INSERT INTO Dim_Products (sku, product_name, category)
            VALUES (:sku, :name, :cat)
            ON CONFLICT (sku) DO NOTHING
        """), products)

        result = conn.execute(text("""
            INSERT INTO Fact_Inventory_Daily (product_key, date_key, stock_on_hand, safety_stock, reorder_point)
            SELECT p.product_key, d.date_key, v.stock, v.ss, v.rop
            FROM (VALUES (:sku, CAST(:full_date AS DATE), :stock, CAST(:ss AS INTEGER), CAST(:rop AS INTEGER)))
                 AS v(sku, full_date, stock, ss, rop)
            JOIN Dim_Products p ON p.sku = v.sku
            JOIN Dim_Dates d ON d.full_date = v.full_date
            ON CONFLICT (product_key, date_key) DO UPDATE SET
                stock_on_hand = EXCLUDED.stock_on_hand,
                safety_stock  = EXCLUDED.safety_stock,
                reorder_point = EXCLUDED.reorder_point,
                loaded_at     = NOW()
        """), rows)

    logger.info("Fact_Inventory_Daily: upsert %d snapshot (rowcount=%s)", len(rows), result.rowcount)
    return len(rows)


def main():
    logger.info("═" * 60)
    logger.info("  Kafka Consumer → PostgreSQL (Fact_Inventory_Daily)")
    logger.info("  Topic: %s | Batch: %d msg hoặc %ds", TOPIC_INVENTORY, BATCH_SIZE, BATCH_TIMEOUT_SEC)
    logger.info("═" * 60)

    consumer = build_consumer()
    engine = get_engine()

    buffer: list[dict] = []
    last_flush = time.time()
    total = 0

    def flush():
        nonlocal buffer, last_flush, total
        if buffer:
            n = upsert_inventory(engine, prepare_snapshots(buffer))
            consumer.commit()  # chỉ commit offset sau khi DB commit thành công
            total += n
            buffer = []
        last_flush = time.time()

    try:
        while _running:
            for message in consumer:
                buffer.append(message.value)
                if len(buffer) >= BATCH_SIZE or time.time() - last_flush >= BATCH_TIMEOUT_SEC:
                    flush()
                if not _running:
                    break
            if time.time() - last_flush >= BATCH_TIMEOUT_SEC:
                flush()
    except KeyboardInterrupt:
        logger.info("Nhận Ctrl+C, đang dừng...")
    finally:
        try:
            flush()
        except Exception as e:
            logger.error("Lỗi ghi batch cuối: %s", e)
        consumer.close()
        logger.info("Consumer tồn kho đã dừng. Tổng snapshot đã upsert: %d", total)


if __name__ == "__main__":
    main()
