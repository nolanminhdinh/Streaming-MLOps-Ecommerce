"""
producer.py
-----------
Kafka Producer: nhận sự kiện từ ECommerceSimulator và bắn liên tục vào topic
`ecom.orders.raw`.

Trạng thái: skeleton — hoàn thiện kết nối Kafka thực tế ở Tuần 2.
"""

import json

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_ORDERS = "ecom.orders.raw"


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
