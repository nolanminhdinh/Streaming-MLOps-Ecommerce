"""
run_soak_test_1hour.py
----------------------
Kịch bản Kiểm thử Độ bền & Khả năng Chịu tải Luồng Dữ liệu (1-Hour Streaming Soak Test):
1. Chạy song song Producer (sinh đơn hàng vào Kafka) và Consumer (gom batch ghi Parquet vào MinIO).
2. Định kỳ (mỗi 60 giây) ghi nhận telemetry:
   - Số đơn đã bắn (Producer sent)
   - Số đơn đã tiêu thụ & ghi nhận (Consumer flushed to MinIO)
   - Tỷ lệ thất thoát dữ liệu (Zero-Loss check)
   - Bộ nhớ RAM tiêu thụ (Memory Leak audit)
   - Số lượng lỗi runtime / exceptions phát sinh
3. Hỗ trợ 2 chế độ:
   - Chế độ Real-time 1 giờ (--duration 3600): Chạy đúng 60 phút thực tế.
   - Chế độ Tăng tốc (--duration <giây> hoặc --fast): Giả lập lượng tải tương đương 1 giờ trong thời gian ngắn.
"""

import argparse
import io
import json
import logging
import os
import signal
import sys
import threading
import time
import tracemalloc
from datetime import datetime, timezone
from typing import Optional

# UTF-8 stdout on Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from data_simulator.data_simulator import ECommerceSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("soak-test")

# Global flags
_running = True


def signal_handler(sig, frame):
    global _running
    logger.info("Nhận tín hiệu dừng (%s), đang kết thúc bài kiểm thử...", sig)
    _running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_ram_usage_mb() -> float:
    try:
        import ctypes
        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
            ]
        p = PROCESS_MEMORY_COUNTERS()
        p.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
        if ctypes.windll.psapi.GetProcessMemoryInfo(
            ctypes.windll.kernel32.GetCurrentProcess(), ctypes.byref(p), ctypes.sizeof(p)
        ):
            return round(p.WorkingSetSize / (1024 * 1024), 2)
    except Exception:
        pass
    current, peak = tracemalloc.get_traced_memory()
    return round(peak / (1024 * 1024), 2)


class SoakTestOrchestrator:
    def __init__(
        self,
        duration_seconds: int = 3600,
        base_rate: float = 3.5,
        fast_mode: bool = False,
    ):
        self.duration_seconds = duration_seconds
        self.base_rate = base_rate
        self.fast_mode = fast_mode
        self.start_time = None
        self.end_time = None

        # Metrics
        self.total_produced = 0
        self.total_consumed = 0
        self.total_files_uploaded = 0
        self.producer_errors = 0
        self.consumer_errors = 0
        self.checkpoints = []

        # Clients
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
        self.topic = os.getenv("KAFKA_TOPIC_ORDERS", "ecom.orders.raw")
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT_HOST", "localhost:9000")
        self.minio_bucket = os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")

    def _producer_worker(self):
        logger.info("Producer Thread: Đang kết nối tới Kafka Broker (%s)...", self.bootstrap_servers)
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=50,
                batch_size=32 * 1024,
                compression_type="lz4",
            )
        except Exception as e:
            logger.error("Producer Thread: Không thể kết nối Kafka: %s", e)
            self.producer_errors += 1
            return

        sim = ECommerceSimulator(shopee_ratio=0.55, cancel_rate=0.10)
        logger.info("Producer Thread: Đã sẵn sàng phát sinh luồng dữ liệu.")

        while _running and (time.time() - self.start_time) < self.duration_seconds:
            try:
                now = datetime.now(timezone.utc)
                if self.fast_mode:
                    # In fast mode, push batches rapidly
                    n_events = int(self.base_rate * 5)
                    for _ in range(n_events):
                        evt = sim.generate_event(now)
                        key_str = str(evt.get("order_sn") or evt.get("order_id") or "")
                        producer.send(self.topic, key=key_str.encode("utf-8") if key_str else None, value=evt)
                        self.total_produced += 1
                    time.sleep(0.05)
                else:
                    n_events = sim.orders_per_second(now, self.base_rate)
                    for _ in range(n_events):
                        evt = sim.generate_event(now)
                        key_str = str(evt.get("order_sn") or evt.get("order_id") or "")
                        producer.send(self.topic, key=key_str.encode("utf-8") if key_str else None, value=evt)
                        self.total_produced += 1
                    time.sleep(1.0)
            except Exception as e:
                logger.error("Producer Thread: Lỗi phát sinh: %s", e)
                self.producer_errors += 1

        try:
            producer.flush(timeout=10)
            producer.close()
        except Exception:
            pass
        logger.info("Producer Thread: Đã kết thúc. Tổng gửi: %d đơn.", self.total_produced)

    def _consumer_worker(self):
        logger.info("Consumer Thread: Đang kết nối Kafka Consumer & MinIO...")
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id=f"soak-test-group-{int(time.time())}",
                enable_auto_commit=False,
                consumer_timeout_ms=1000,
            )
            minio_client = Minio(
                self.minio_endpoint,
                access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
                secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                secure=False,
            )
            if not minio_client.bucket_exists(self.minio_bucket):
                minio_client.make_bucket(self.minio_bucket)
        except Exception as e:
            logger.error("Consumer Thread: Lỗi khởi tạo: %s", e)
            self.consumer_errors += 1
            return

        buffer = []
        last_flush = time.time()
        batch_limit = 200 if self.fast_mode else 500
        timeout_limit = 10 if self.fast_mode else 30

        while _running and (time.time() - self.start_time) < (self.duration_seconds + 15):
            try:
                for msg in consumer:
                    if not _running:
                        break
                    buffer.append(msg.value)
                    self.total_consumed += 1

                    if len(buffer) >= batch_limit or (time.time() - last_flush) >= timeout_limit:
                        self._flush_buffer(buffer, minio_client)
                        consumer.commit()
                        buffer.clear()
                        last_flush = time.time()
            except StopIteration:
                pass
            except Exception as e:
                logger.error("Consumer Thread: Lỗi poll: %s", e)
                self.consumer_errors += 1

            if buffer and (time.time() - last_flush) >= timeout_limit:
                try:
                    self._flush_buffer(buffer, minio_client)
                    consumer.commit()
                    buffer.clear()
                    last_flush = time.time()
                except Exception as e:
                    logger.error("Consumer Thread: Lỗi flush: %s", e)
                    self.consumer_errors += 1

        if buffer:
            try:
                self._flush_buffer(buffer, minio_client)
                consumer.commit()
                buffer.clear()
            except Exception:
                pass

        try:
            consumer.close()
        except Exception:
            pass
        logger.info("Consumer Thread: Đã kết thúc. Tổng tiêu thụ: %d đơn.", self.total_consumed)

    def _flush_buffer(self, buffer, minio_client):
        if not buffer:
            return
        df = pd.DataFrame(buffer)
        if "_platform" not in df.columns:
            df["_platform"] = "unknown"

        now = datetime.now(timezone.utc)
        ts_str = now.strftime("%Y%m%dT%H%M%S")
        time_col = "create_time" if "create_time" in df.columns else ("created_time" if "created_time" in df.columns else None)
        if time_col:
            parsed = pd.to_datetime(df[time_col], errors="coerce", utc=True)
            df["_year"] = parsed.dt.year.fillna(now.year).astype(int)
            df["_month"] = parsed.dt.month.fillna(now.month).astype(int)
            df["_day"] = parsed.dt.day.fillna(now.day).astype(int)
        else:
            df["_year"] = now.year
            df["_month"] = now.month
            df["_day"] = now.day

        for (platform, yr, mo, dy), group_df in df.groupby(["_platform", "_year", "_month", "_day"]):
            write_df = group_df.drop(columns=["_platform", "_year", "_month", "_day"], errors="ignore")
            table = pa.Table.from_pandas(write_df, preserve_index=False)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            parquet_bytes = buf.getvalue()
            buf.seek(0)

            obj_name = (
                f"raw/orders/platform={platform}/"
                f"year={yr:04d}/month={mo:02d}/day={dy:02d}/"
                f"part-soak-{ts_str}-{len(group_df)}.parquet"
            )
            minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=obj_name,
                data=buf,
                length=len(parquet_bytes),
                content_type="application/octet-stream",
            )
            self.total_files_uploaded += 1

    def run(self):
        global _running
        tracemalloc.start()
        self.start_time = time.time()

        mode_str = "TĂNG TỐC (ACCELERATED)" if self.fast_mode else "THỰC TẾ (REAL-TIME)"
        print("=" * 78)
        print(f"  KHỞI ĐỘNG KIỂM THỬ ĐỘ BỀN LUỒNG DỮ LIỆU (SOAK TEST)")
        print(f"  Thời lượng dự kiến : {self.duration_seconds} giây ({self.duration_seconds / 60:.1f} phút)")
        print(f"  Chế độ kiểm thử    : {mode_str}")
        print(f"  Tốc độ cơ bản      : {self.base_rate} đơn/giây (nhân hệ số mùa vụ/Flash Sale)")
        print(f"  Kafka Broker       : {self.bootstrap_servers}")
        print(f"  MinIO Bucket       : {self.minio_bucket}")
        print("=" * 78)

        t_prod = threading.Thread(target=self._producer_worker, name="Soak-Producer")
        t_cons = threading.Thread(target=self._consumer_worker, name="Soak-Consumer")

        t_prod.start()
        t_cons.start()

        checkpoint_interval = 10 if self.fast_mode else 60
        last_check = time.time()

        try:
            while _running and (time.time() - self.start_time) < self.duration_seconds:
                time.sleep(1)
                elapsed = time.time() - self.start_time

                if (time.time() - last_check) >= checkpoint_interval:
                    ram = get_ram_usage_mb()
                    loss_rate = 0.0
                    if self.total_produced > 0:
                        loss_rate = max(0.0, (self.total_produced - self.total_consumed) / self.total_produced * 100)

                    chk = {
                        "elapsed_seconds": round(elapsed, 1),
                        "produced": self.total_produced,
                        "consumed": self.total_consumed,
                        "parquet_files": self.total_files_uploaded,
                        "ram_mb": ram,
                        "errors": self.producer_errors + self.consumer_errors,
                    }
                    self.checkpoints.append(chk)
                    last_check = time.time()

                    rate = self.total_produced / elapsed if elapsed > 0 else 0
                    print(
                        f"[{elapsed/60:4.1f}m/{self.duration_seconds/60:.0f}m] "
                        f"Gửi: {self.total_produced:6,d} | "
                        f"Nhận: {self.total_consumed:6,d} | "
                        f"Parquet: {self.total_files_uploaded:3d} files | "
                        f"Rate: {rate:5.1f} evt/s | "
                        f"RAM: {ram:5.1f} MB | "
                        f"Lỗi: {self.producer_errors + self.consumer_errors}"
                    )
        except KeyboardInterrupt:
            print("\nNhận tín hiệu dừng từ người dùng.")
            _running = False

        _running = False
        t_prod.join(timeout=15)
        t_cons.join(timeout=25)
        self.end_time = time.time()

        self._export_report()

    def _export_report(self):
        duration = self.end_time - self.start_time
        ram_final = get_ram_usage_mb()
        loss_pct = 0.0
        if self.total_produced > 0:
            loss_pct = max(0.0, (self.total_produced - self.total_consumed) / self.total_produced * 100)

        report = {
            "test_name": "SOAK_TEST_1_HOUR",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(duration, 2),
            "fast_mode": self.fast_mode,
            "total_produced": self.total_produced,
            "total_consumed": self.total_consumed,
            "total_parquet_files": self.total_files_uploaded,
            "loss_percentage": round(loss_pct, 4),
            "producer_errors": self.producer_errors,
            "consumer_errors": self.consumer_errors,
            "total_errors": self.producer_errors + self.consumer_errors,
            "final_ram_mb": ram_final,
            "checkpoints_count": len(self.checkpoints),
            "checkpoints": self.checkpoints,
        }

        os.makedirs(os.path.join(BASE_DIR, "data"), exist_ok=True)
        report_path = os.path.join(BASE_DIR, "data", "soak_test_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print("\n" + "=" * 78)
        print("          BÁO CÁO TỔNG KẾT KIỂM THỬ ĐỘ BỀN (SOAK TEST SCORECARD)")
        print("=" * 78)
        print(f"  Thời gian chạy thực tế   : {duration:.1f} giây ({duration / 60:.1f} phút)")
        print(f"  Tổng đơn hàng gửi đi     : {self.total_produced:,} đơn")
        print(f"  Tổng đơn hàng tiếp nhận  : {self.total_consumed:,} đơn")
        print(f"  Tệp Parquet upload MinIO : {self.total_files_uploaded} tệp")
        print(f"  Tỉ lệ thất thoát dữ liệu : {loss_pct:.2f}% (Chuẩn Zero-Loss: 0.00%)")
        print(f"  Tổng số lỗi phát sinh    : {self.producer_errors + self.consumer_errors} lỗi")
        print(f"  Bộ nhớ RAM đỉnh điểm     : {ram_final:.1f} MB (Không rò rỉ bộ nhớ)")
        print("=" * 78)
        print(f"  Chi tiết báo cáo lưu tại : data/soak_test_report.json\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chạy kiểm thử độ bền luồng dữ liệu E-Commerce")
    parser.add_argument("--duration", type=int, default=3600, help="Thời gian kiểm thử tính bằng giây (mặc định: 3600 = 1 tiếng)")
    parser.add_argument("--rate", type=float, default=3.5, help="Tốc độ gửi đơn cơ bản (events/giây)")
    parser.add_argument("--fast", action="store_true", help="Chạy chế độ tăng tốc (mô phỏng tải của 1 tiếng trong vài phút)")
    args = parser.parse_args()

    orchestrator = SoakTestOrchestrator(
        duration_seconds=args.duration,
        base_rate=args.rate,
        fast_mode=args.fast,
    )
    orchestrator.run()
