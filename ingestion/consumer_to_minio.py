"""
consumer_to_minio.py
---------------------
Kafka Consumer: đọc message từ topic `ecom.orders.raw`, gom theo batch
(mỗi 60 giây hoặc mỗi 500 message, cái nào đến trước) và ghi xuống MinIO
Data Lake dưới định dạng Parquet, phân vùng theo ngày + sàn TMĐT.

Path trên MinIO:
  raw/orders/platform={shopee|tiktok}/year=YYYY/month=MM/day=DD/part-<timestamp>-<batch_id>-<rows>.parquet
  (year/month/day theo múi giờ nghiệp vụ BUSINESS_TZ, mặc định Asia/Ho_Chi_Minh)

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
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error

# Keep this executable directly as well as through the package entry point.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from warehouse.etl.quarantine import json_value

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

# Phân vùng year/month/day theo múi giờ nghiệp vụ (giống warehouse/etl/transform.py),
# tránh việc đơn 0h-7h sáng giờ VN bị xếp vào partition ngày hôm trước (UTC).
BUSINESS_TZ = os.getenv("BUSINESS_TZ", "Asia/Ho_Chi_Minh")

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

def decode_event(payload):
    """Keep malformed Kafka payloads auditable instead of blocking a partition."""
    try:
        value = json.loads(payload.decode("utf-8"))
        if isinstance(value, dict):
            return value
    except (ValueError, UnicodeError):
        pass
    return {"_ingest_error": "INVALID_JSON_OBJECT", "_raw_message": payload.decode("utf-8", errors="replace")}

def build_consumer(retries: int = 5, retry_delay: float = 3.0) -> KafkaConsumer:
    """Tạo KafkaConsumer có retry khi Kafka chưa sẵn sàng."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC_ORDERS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=decode_event,
                auto_offset_reset="earliest",
                group_id="minio-writer-group",
                enable_auto_commit=False,  # Tắt auto commit để tránh mất dữ liệu khi crash
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

    Phân vùng chuẩn xác theo platform + ngày của từng bản ghi:
      raw/orders/platform=shopee/year=YYYY/month=MM/day=DD/part-<ts>.parquet
      raw/orders/platform=tiktok/year=YYYY/month=MM/day=DD/part-<ts>.parquet

    Returns:
        Số file đã upload.
    """
    if not batch:
        return 0

    df = pd.DataFrame(batch)

    # Xác định platform từ trường _platform (shopee | tiktok)
    if "_platform" not in df.columns:
        df["_platform"] = "unknown"
    # Never let groupby silently discard nulls or use untrusted values as paths.
    df["_source_platform"] = df["_platform"].map(lambda value: None if pd.isna(value) else str(value))
    df["_platform"] = df["_platform"].astype("string").str.strip().str.lower()
    df["_platform"] = df["_platform"].where(df["_platform"].isin(["shopee", "tiktok"]), "unknown")

    # Lấy timestamp ghi nhận (theo giờ nghiệp vụ)
    now = pd.Timestamp.now(tz=BUSINESS_TZ)
    ts_str = now.strftime("%Y%m%dT%H%M%S")
    # Hậu tố ngẫu nhiên: 2 lần flush cùng giây (hoặc nhiều consumer song song) không ghi đè file nhau
    batch_id = uuid.uuid4().hex[:8]

    # Batch có thể trộn Shopee (create_time) và TikTok (created_time).
    # Parse từng cột rồi kết hợp theo dòng; không chọn một cột cho cả batch.
    # format="mixed" hỗ trợ ISO timestamps có/không có phần mili giây và offset.
    # Timestamp không có offset vẫn được hiểu là UTC, giống tầng ETL.
    parsed_dates = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")
    for time_col in ("create_time", "created_time"):
        if time_col in df.columns:
            parsed_dates = parsed_dates.combine_first(
                pd.to_datetime(df[time_col], format="mixed", errors="coerce", utc=True)
            )
    # Đổi sang giờ Việt Nam trước khi lấy ngày, kể cả đơn đến muộn/replay.
    # Chỉ dùng ngày nhận khi cả hai timestamp đều thiếu hoặc không hợp lệ.
    parsed_dates = parsed_dates.dt.tz_convert(BUSINESS_TZ).fillna(now)
    df["_year"] = parsed_dates.dt.year.astype(int)
    df["_month"] = parsed_dates.dt.month.astype(int)
    df["_day"] = parsed_dates.dt.day.astype(int)

    files_uploaded = 0

    # Group by platform và ngày chính xác của bản ghi
    for (platform, yr, mo, dy), group_df in df.groupby(["_platform", "_year", "_month", "_day"]):
        write_df = group_df.drop(columns=["_platform", "_year", "_month", "_day"], errors="ignore")

        # Chuyển DataFrame → PyArrow Table → Parquet bytes
        try:
            table = pa.Table.from_pandas(write_df, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Mixed types (e.g. quantity=2 alongside quantity="bad") must reach
            # validation, not permanently poison the Kafka partition.
            table = pa.Table.from_pydict({"_raw_payload_json": [
                json.dumps(json_value(row), ensure_ascii=False, allow_nan=False)
                for row in write_df.to_dict("records")]})
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        parquet_bytes = buf.getvalue()
        buf.seek(0)

        # Object path trên MinIO
        object_name = (
            f"raw/orders/"
            f"platform={platform}/"
            f"year={yr:04d}/month={mo:02d}/day={dy:02d}/"
            f"part-{ts_str}-{batch_id}-{len(group_df)}.parquet"
        )

        # Upload - Không nuốt lỗi để tránh mất dữ liệu khi commit offset
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
            raise

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
                        consumer.commit()  # Chỉ commit offset sau khi ghi MinIO thành công
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
                consumer.commit()
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
            try:
                n_files = flush_batch_to_minio(buffer, minio_client)
                consumer.commit()
                total_files += n_files
                logger.info("Final flush: %d messages → %d files", len(buffer), n_files)
            except Exception as e:
                logger.error("Lỗi flush cuối: %s", e)

        consumer.close()
        logger.info(
            "Consumer đã dừng. Tổng: %d messages → %d Parquet files",
            total_messages, total_files,
        )


if __name__ == "__main__":
    main()
