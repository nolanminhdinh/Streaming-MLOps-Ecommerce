"""
consumer_to_minio.py
---------------------
Kafka Consumer: đọc message từ topic `ecom.orders.raw`, gom theo batch
(mỗi 60 giây hoặc mỗi 500 message, cái nào đến trước) và ghi xuống MinIO
Data Lake dưới định dạng Parquet, phân vùng theo ngày + sàn TMĐT.

Path trên MinIO:
  raw/orders/platform={shopee|tiktok}/year=YYYY/month=MM/day=DD/part-<timestamp>.parquet

Cấu hình được đọc từ file `.env` ở thư mục gốc dự án (xem `.env.example`).
Nếu không có `.env`, sẽ dùng giá trị mặc định phù hợp với docker-compose.yml.

Hoàn thiện Tuần 3: gom batch → DataFrame → Parquet → upload MinIO.
"""

import io
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error

load_dotenv()  # đọc file .env nếu có

# ─── Cấu hình ───
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# Batch config
BATCH_SIZE = int(os.getenv("CONSUMER_BATCH_SIZE", "500"))      # flush mỗi N messages
BATCH_TIMEOUT_SEC = int(os.getenv("CONSUMER_BATCH_TIMEOUT", "60"))  # hoặc mỗi N giây

# ─── Logging ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka-consumer-minio")

# ─── Graceful shutdown ───
_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("Nhận tín hiệu dừng (%s), đang flush batch cuối...", sig)
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ─── Kafka Consumer ───

def build_consumer(retries: int = 5, retry_delay: float = 3.0) -> KafkaConsumer:
    """Tạo KafkaConsumer có retry khi Kafka chưa sẵn sàng."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC_ORDERS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="minio-writer-group",
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                consumer_timeout_ms=1000,  # poll timeout 1s để check _running
            )
            logger.info("Kết nối Kafka Consumer thành công: %s", KAFKA_BOOTSTRAP_SERVERS)
            return consumer
        except KafkaError as e:
            logger.warning(
                "Kafka chưa sẵn sàng (lần %d/%d): %s — thử lại sau %.0fs...",
                attempt, retries, e, retry_delay,
            )
            time.sleep(retry_delay)

    raise ConnectionError(
        f"Không thể kết nối Kafka sau {retries} lần thử: {KAFKA_BOOTSTRAP_SERVERS}"
    )


# ─── MinIO Client ───

def build_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    logger.info("MinIO client khởi tạo: %s (secure=%s)", MINIO_ENDPOINT, MINIO_SECURE)
    return client


def ensure_bucket(client: Minio, bucket: str = MINIO_BUCKET):
    """Tạo bucket nếu chưa tồn tại."""
    try:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            logger.info("Đã tạo bucket: %s", bucket)
        else:
            logger.info("Bucket đã tồn tại: %s", bucket)
    except S3Error as e:
        logger.error("Lỗi kiểm tra/tạo bucket: %s", e)
        raise


# ─── Batch Processing ───

def flush_batch_to_minio(
    batch: list[dict],
    minio_client: Minio,
    bucket: str = MINIO_BUCKET,
) -> int:
    """
    Chuyển batch records thành Parquet và upload lên MinIO.

    Phân vùng theo platform + ngày để dễ query sau này:
      raw/orders/platform=shopee/year=2026/month=09/day=24/part-<ts>.parquet
      raw/orders/platform=tiktok/year=2026/month=09/day=24/part-<ts>.parquet

    Returns:
        Số file đã upload.
    """
    if not batch:
        return 0

    df = pd.DataFrame(batch)

    # Xác định platform từ trường _platform (shopee | tiktok)
    if "_platform" not in df.columns:
        df["_platform"] = "unknown"

    # Lấy timestamp ghi nhận
    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%dT%H%M%S")

    files_uploaded = 0

    # Group by platform để ghi file riêng
    for platform, group_df in df.groupby("_platform"):
        # Xác định ngày dựa trên create_time (Shopee) hoặc created_time (TikTok)
        time_col = "create_time" if "create_time" in group_df.columns else "created_time"
        if time_col in group_df.columns:
            try:
                first_ts = pd.to_datetime(group_df[time_col].iloc[0])
                year = first_ts.year
                month = first_ts.month
                day = first_ts.day
            except Exception:
                year, month, day = now.year, now.month, now.day
        else:
            year, month, day = now.year, now.month, now.day

        # Loại bỏ cột internal _platform trước khi ghi parquet
        write_df = group_df.drop(columns=["_platform"], errors="ignore")

        # Chuyển DataFrame → PyArrow Table → Parquet bytes
        table = pa.Table.from_pandas(write_df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        parquet_bytes = buf.getvalue()
        buf.seek(0)

        # Object path trên MinIO
        object_name = (
            f"raw/orders/"
            f"platform={platform}/"
            f"year={year:04d}/month={month:02d}/day={day:02d}/"
            f"part-{ts_str}-{len(group_df)}.parquet"
        )

        # Upload
        try:
            minio_client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=buf,
                length=len(parquet_bytes),
                content_type="application/octet-stream",
            )
            files_uploaded += 1
            logger.info(
                "✓ Upload: %s (%d rows, %.1f KB)",
                object_name, len(group_df), len(parquet_bytes) / 1024,
            )
        except S3Error as e:
            logger.error("✗ Upload thất bại %s: %s", object_name, e)

    return files_uploaded


# ─── Main Loop ───

def main():
    global _running

    logger.info("═" * 60)
    logger.info("  Kafka Consumer → MinIO (Parquet)")
    logger.info("  Topic: %s", TOPIC_ORDERS)
    logger.info("  Bucket: %s", MINIO_BUCKET)
    logger.info("  Batch: %d messages hoặc %d giây", BATCH_SIZE, BATCH_TIMEOUT_SEC)
    logger.info("═" * 60)

    consumer = build_consumer()
    minio_client = build_minio_client()
    ensure_bucket(minio_client)

    buffer: list[dict] = []
    last_flush_time = time.time()
    total_messages = 0
    total_files = 0

    try:
        while _running:
            # Poll messages (timeout 1s được set trong consumer_timeout_ms)
            try:
                for message in consumer:
                    if not _running:
                        break

                    buffer.append(message.value)
                    total_messages += 1

                    # Check flush conditions
                    elapsed = time.time() - last_flush_time
                    if len(buffer) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT_SEC:
                        n_files = flush_batch_to_minio(buffer, minio_client)
                        total_files += n_files
                        logger.info(
                            "Batch flushed: %d messages → %d files | "
                            "Tổng: %d messages, %d files",
                            len(buffer), n_files, total_messages, total_files,
                        )
                        buffer.clear()
                        last_flush_time = time.time()

            except StopIteration:
                # consumer_timeout_ms hết → kiểm tra flush theo thời gian
                pass

            # Flush theo timeout ngay cả khi chưa đủ BATCH_SIZE
            elapsed = time.time() - last_flush_time
            if buffer and elapsed >= BATCH_TIMEOUT_SEC:
                n_files = flush_batch_to_minio(buffer, minio_client)
                total_files += n_files
                logger.info(
                    "Timeout flush: %d messages → %d files | "
                    "Tổng: %d messages, %d files",
                    len(buffer), n_files, total_messages, total_files,
                )
                buffer.clear()
                last_flush_time = time.time()

    except KeyboardInterrupt:
        logger.info("Nhận Ctrl+C, đang dừng...")
    finally:
        # Flush buffer còn lại
        if buffer:
            n_files = flush_batch_to_minio(buffer, minio_client)
            total_files += n_files
            logger.info("Final flush: %d messages → %d files", len(buffer), n_files)

        consumer.close()
        logger.info(
            "Consumer đã dừng. Tổng: %d messages → %d Parquet files",
            total_messages, total_files,
        )


if __name__ == "__main__":
    main()
