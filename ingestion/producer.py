"""
producer.py
-----------
Kafka Producer: nhận sự kiện từ ECommerceSimulator và bắn liên tục vào topic
`ecom.orders.raw`.

Cấu hình được đọc từ file `.env` ở thư mục gốc dự án (xem `.env.example`).
Nếu không có `.env`, sẽ dùng giá trị mặc định phù hợp với docker-compose.yml.

Hoàn thiện Tuần 2-3: tích hợp thực tế data_simulator → Kafka.
"""

import json
import logging
import os
import signal
import sys
import time

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Cho phép import data_simulator từ thư mục gốc dự án
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data_simulator.data_simulator import ECommerceSimulator

load_dotenv()  # đọc file .env nếu có, không lỗi nếu file không tồn tại

# ─── Cấu hình ───
# Dùng KAFKA_BOOTSTRAP_SERVERS_HOST vì producer chạy trên máy host (ngoài Docker),
# không phải trong cùng network với container Kafka.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
TOPIC_ORDERS = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw")
TOPIC_INVENTORY = os.getenv("KAFKA_TOPIC_INVENTORY", "inventory.logs")
# Chu kỳ (giây) gửi ảnh chụp tồn kho của toàn bộ SKU lên topic inventory.logs.
# Simulator giữ trạng thái tồn kho trong bộ nhớ của chính tiến trình producer, nên chỉ
# producer mới phát được tồn kho "thật" khớp với luồng đơn hàng đã bắn đi.
INVENTORY_SNAPSHOT_INTERVAL_SEC = float(os.getenv("INVENTORY_SNAPSHOT_INTERVAL", "60"))

# ─── Logging ───
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka-producer")

# ─── Graceful shutdown ───
_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("Nhận tín hiệu dừng (%s), đang flush và đóng producer...", sig)
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ─── Kafka Producer ───

def build_producer(retries: int = 5, retry_delay: float = 3.0) -> KafkaProducer:
    """Tạo KafkaProducer có retry khi Kafka chưa sẵn sàng."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",                  # đảm bảo message được ghi vào tất cả replica
                retries=3,                   # tự retry khi gửi lỗi tạm thời
                linger_ms=50,                # gom batch nhỏ để tăng throughput
                batch_size=32 * 1024,        # 32KB batch
                compression_type="lz4",      # nén payload giảm network I/O
            )
            logger.info("Kết nối Kafka thành công: %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaError as e:
            logger.warning(
                "Kafka chưa sẵn sàng (lần %d/%d): %s — thử lại sau %.0fs...",
                attempt, retries, e, retry_delay,
            )
            time.sleep(retry_delay)

    raise ConnectionError(
        f"Không thể kết nối Kafka sau {retries} lần thử: {KAFKA_BOOTSTRAP_SERVERS}"
    )


def send_event(producer: KafkaProducer, event: dict, topic: str = TOPIC_ORDERS):
    """Gửi 1 event vào Kafka topic, có callback log lỗi và routing theo key."""

    def on_success(metadata):
        logger.debug(
            "Gửi OK: topic=%s partition=%d offset=%d",
            metadata.topic, metadata.partition, metadata.offset,
        )

    def on_error(exc):
        logger.error("Gửi THẤT BẠI: %s", exc)

    # Sử dụng order_id / sku làm message key để đảm bảo tính thứ tự trên partition
    key_str = str(
        event.get("order_sn")
        or event.get("order_id")
        or event.get("item_sku")
        or event.get("seller_sku")
        or event.get("sku")
        or ""
    )
    key_bytes = key_str.encode("utf-8") if key_str else None

    future = producer.send(topic, key=key_bytes, value=event)
    future.add_callback(on_success)
    future.add_errback(on_error)


def send_inventory_snapshot(producer: KafkaProducer, simulator: ECommerceSimulator) -> int:
    """Gửi ảnh chụp tồn kho (1 message / SKU, key = sku) lên topic inventory.logs."""
    rows = simulator.get_inventory_snapshot()
    for row in rows:
        send_event(producer, row, topic=TOPIC_INVENTORY)
    logger.info("Đã gửi snapshot tồn kho: %d SKU → %s", len(rows), TOPIC_INVENTORY)
    return len(rows)


def run_producer(
    base_events_per_second: float = 3.0,
    shopee_ratio: float = 0.55,
    cancel_rate: float = 0.12,
):
    """Vòng lặp chính: sinh event từ Simulator → gửi vào Kafka liên tục."""
    global _running

    logger.info("═" * 60)
    logger.info("  Kafka Producer — E-Commerce Order Streaming")
    logger.info("  Topic: %s (+ snapshot tồn kho → %s mỗi %.0fs)", TOPIC_ORDERS, TOPIC_INVENTORY, INVENTORY_SNAPSHOT_INTERVAL_SEC)
    logger.info("  Bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Base rate: %.1f events/s (trước hệ số mùa vụ)", base_events_per_second)
    logger.info("═" * 60)

    producer = build_producer()
    simulator = ECommerceSimulator(
        shopee_ratio=shopee_ratio,
        cancel_rate=cancel_rate,
    )

    total_sent = 0
    start_time = time.time()
    # Gửi snapshot ngay khi khởi động để Fact_Inventory_Daily có dữ liệu sớm
    send_inventory_snapshot(producer, simulator)
    last_snapshot = time.time()

    try:
        for event in simulator.stream(base_events_per_second=base_events_per_second):
            if not _running:
                break

            send_event(producer, event)
            total_sent += 1

            if time.time() - last_snapshot >= INVENTORY_SNAPSHOT_INTERVAL_SEC:
                send_inventory_snapshot(producer, simulator)
                last_snapshot = time.time()

            # Log tiến độ mỗi 100 events
            if total_sent % 100 == 0:
                elapsed = time.time() - start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                platform = event.get("_platform", "?")
                status = event.get("order_status", "?")
                logger.info(
                    "Đã gửi %d events (%.1f evt/s) | Cuối: [%s] %s",
                    total_sent, rate, platform.upper(), status,
                )
    except KeyboardInterrupt:
        logger.info("Nhận Ctrl+C, đang dừng...")
    finally:
        logger.info("Flushing %d message còn trong buffer...", total_sent)
        producer.flush(timeout=10)
        producer.close(timeout=5)
        elapsed = time.time() - start_time
        logger.info(
            "Producer đã dừng. Tổng: %d events trong %.1f giây (%.1f evt/s)",
            total_sent, elapsed, total_sent / elapsed if elapsed > 0 else 0,
        )


if __name__ == "__main__":
    run_producer(
        base_events_per_second=3.0,
        shopee_ratio=0.55,
        cancel_rate=0.12,
    )
