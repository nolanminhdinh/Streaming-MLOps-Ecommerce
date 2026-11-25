"""Bộ phát tải cao điểm (YC7): dùng lại nguyên producer.py và simulator, chỉ giữ đúng N sự kiện mỗi giây.

producer.py nghỉ 1 giây sau mỗi đợt và chờ Kafka xác nhận từng bản tin, nên tốc độ thực tế thấp hơn số đặt.
Script này canh nhịp theo mốc giây (đợt nào chậm thì đợt sau không nghỉ) để tốc độ trung bình bằng N.
Cấu hình Kafka producer, nội dung sự kiện, mã lượt chạy và snapshot tồn kho giữ nguyên như producer.py.

    python scripts/peak_load_producer.py --rate 12
"""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "ingestion"), str(ROOT)]
import producer  # noqa: E402
from data_simulator.data_simulator import ECommerceSimulator  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rate", type=int, default=12, help="số sự kiện đơn hàng mỗi giây")
    rate = parser.parse_args().rate

    def paced_stream(self, base_events_per_second=3.0, duration_seconds=None):
        next_tick = time.monotonic()
        while True:
            now = datetime.now(timezone.utc)
            for _ in range(rate):
                yield self.generate_event(now)
            next_tick += 1.0
            time.sleep(max(0.0, next_tick - time.monotonic()))

    ECommerceSimulator.stream = paced_stream
    producer.logger.info("Thử tải cao điểm: %d sự kiện/giây cố định", rate)
    producer.run_producer()


if __name__ == "__main__":
    main()
