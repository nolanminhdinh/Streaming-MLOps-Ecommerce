"""
consumer_to_minio.py
---------------------
Kafka Consumer: đọc message từ topic `ecom.orders.raw`, gom theo batch (ví dụ
mỗi phút hoặc mỗi N message) và ghi xuống MinIO Data Lake dưới định dạng
Parquet, phân vùng theo ngày (partition by date).

Trạng thái: skeleton — hoàn thiện logic batch + ghi Parquet ở Tuần 3.
"""

import json

from kafka import KafkaConsumer
from minio import Minio

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_ORDERS = "ecom.orders.raw"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "ecom-raw-lake"


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
        secure=False,
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
