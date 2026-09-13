"""
producer.py
-----------
Kafka Producer: nhận sự kiện từ ECommerceSimulator và bắn liên tục vào topic
`ecom.orders.raw`.

Cấu hình được đọc từ file `.env` ở thư mục gốc dự án (xem `.env.example`).
Nếu không có `.env`, sẽ dùng giá trị mặc định phù hợp với docker-compose.yml.

Trạng thái: skeleton — hoàn thiện kết nối Kafka thực tế ở Tuần 2.
"""

import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()  # đọc file .env nếu có, không lỗi nếu file không tồn tại

# Dùng KAFKA_BOOTSTRAP_SERVERS_HOST vì producer chạy trên máy host (ngoài Docker),
# không phải trong cùng network với container Kafka.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )


def send_event(producer: KafkaProducer, event: dict, topic: str = TOPIC_ORDERS):
    producer.send(topic, value=event)


if __name__ == "__main__":
    # TODO: import ECommerceSimulator và gửi event thật vào Kafka thay vì print()
    print("TODO: kết nối data_simulator -> Kafka Producer")
