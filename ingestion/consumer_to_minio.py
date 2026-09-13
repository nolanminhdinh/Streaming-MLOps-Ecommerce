"""
consumer_to_minio.py
---------------------
Kafka Consumer: đọc message từ topic `ecom.orders.raw`, gom theo batch (ví dụ
mỗi phút hoặc mỗi N message) và ghi xuống MinIO Data Lake dưới định dạng
Parquet, phân vùng theo ngày (partition by date).

Cấu hình được đọc từ file `.env` ở thư mục gốc dự án (xem `.env.example`).
Nếu không có `.env`, sẽ dùng giá trị mặc định phù hợp với docker-compose.yml.

Trạng thái: skeleton — hoàn thiện logic batch + ghi Parquet ở Tuần 3.
"""

import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer
from minio import Minio

load_dotenv()  # đọc file .env nếu có, không lỗi nếu file không tồn tại

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_ORDERS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="minio-writer-group",
    )


def build_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket(client: Minio, bucket: str = MINIO_BUCKET):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def main():
    consumer = build_consumer()
    minio_client = build_minio_client()
    ensure_bucket(minio_client)

    buffer = []
    # TODO (Tuần 3):
    # 1. Gom `buffer` theo batch (thời gian hoặc số lượng message).
    # 2. Convert buffer -> pandas.DataFrame -> pyarrow Table -> Parquet.
    # 3. Upload file Parquet lên MinIO theo path dạng:
    #    raw/orders/year=YYYY/month=MM/day=DD/part-<timestamp>.parquet
    for message in consumer:
        buffer.append(message.value)
        print(f"Nhận message: {message.value}")


if __name__ == "__main__":
    main()
