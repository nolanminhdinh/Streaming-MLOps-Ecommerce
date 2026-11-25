"""Tự xác nhận độ đủ dữ liệu theo ngày nghiệp vụ cho nguồn streaming (quyết định của sinh viên 16/11/2026).

Một ngày (giờ Việt Nam) được ghi vào Warehouse_Daily_Coverage với data_origin='warehouse_live' khi:
  1. ngày đó đã kết thúc (nhỏ hơn hôm nay theo BUSINESS_TZ);
  2. worker không còn object raw chờ ETL;
  3. lag của mọi consumer group Kafka bằng 0 (mọi bản tin đã nhận đều đã ghi xuống lake/kho).
  4. khối lượng ngày không bất thường thấp: số dòng đơn >= MIN_VOLUME_RATIO × trung vị số dòng của
     các ngày có đơn trong REFERENCE_DAYS ngày trước (khi đã có ít nhất MIN_REFERENCE_DAYS ngày tham chiếu).
Điều kiện 1–3 chỉ chứng minh pipeline không còn tồn đọng; chúng không phân biệt "nguồn ngừng gửi"
với "không có đơn". Điều kiện 4 (bổ sung 16/11/2026 sau khi 14 ngày producer tắt bị xác nhận là đủ)
giữ lại ngày nghi thiếu dữ liệu để người vận hành kiểm tra và xác nhận tay bằng confirm_daily_coverage.py.
Bản tin đến muộn sau khi xác nhận vẫn được nạp; ngày đó không bị bỏ xác nhận.
Không xác nhận ngày nào nếu không kiểm tra được lag.
"""
from datetime import date, timedelta
import logging
import statistics

from sqlalchemy import text

ORIGIN = "warehouse_live"
CONSUMER_GROUPS = ("minio-writer-group", "inventory-postgres-writer-group")
MIN_VOLUME_RATIO = 0.2
REFERENCE_DAYS = 28
MIN_REFERENCE_DAYS = 7
logger = logging.getLogger("etl.coverage")


def daily_volumes(conn, start: date, end: date) -> dict:
    return {row[0]: int(row[1]) for row in conn.execute(text("""
        SELECT create_time::date, COUNT(*) FROM Fact_Orders
        WHERE create_time >= :start AND create_time < CAST(:end AS DATE) + 1 GROUP BY 1"""),
        {"start": start, "end": end})}


def volume_plausible(day: date, volumes: dict) -> bool:
    """Ngày đủ khối lượng so với trung vị các ngày có đơn trước đó; chưa đủ tham chiếu thì không chặn."""
    reference = [volumes[d] for d in (day - timedelta(days=i) for i in range(1, REFERENCE_DAYS + 1)) if volumes.get(d, 0) > 0]
    if len(reference) < MIN_REFERENCE_DAYS:
        return True
    return volumes.get(day, 0) >= MIN_VOLUME_RATIO * statistics.median(reference)


def unconfirmed_days(conn, today: date, origin: str = ORIGIN) -> list[date]:
    """Các ngày từ ngày có đơn đầu tiên tới hôm qua chưa được xác nhận."""
    return list(conn.execute(text("""
        SELECT d::date FROM generate_series(
            (SELECT MIN(create_time)::date FROM Fact_Orders), CAST(:today AS DATE) - 1, INTERVAL '1 day') d
        WHERE NOT EXISTS (SELECT 1 FROM Warehouse_Daily_Coverage c
                          WHERE c.data_origin = :origin AND c.source_dataset = '' AND c.full_date = d::date)
        ORDER BY 1"""), {"today": today, "origin": origin}).scalars())


def kafka_lag(bootstrap: str, groups=CONSUMER_GROUPS) -> int:
    """Tổng lag của các consumer group; ném lỗi nếu không đọc được để không xác nhận nhầm."""
    from kafka import KafkaAdminClient, KafkaConsumer
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=10000)
    try:
        total = 0
        for group in groups:
            committed = {tp: meta.offset for tp, meta in admin.list_consumer_group_offsets(group).items()}
            if not committed:
                raise RuntimeError(f"Consumer group {group} chưa có offset đã commit")
            ends = consumer.end_offsets(list(committed))
            total += sum(max(0, ends[tp] - offset) for tp, offset in committed.items())
        return total
    finally:
        admin.close()
        consumer.close()


def confirm_completed_days(conn, today: date, pending_objects, lag_reader, origin: str = ORIGIN) -> list[date]:
    """Ghi coverage cho các ngày đủ điều kiện; trả về các ngày vừa xác nhận."""
    if pending_objects:
        return []
    days = unconfirmed_days(conn, today, origin)
    if not days:
        return []
    lag = lag_reader()
    if lag != 0:
        logger.info("Chưa xác nhận coverage: lag Kafka còn %s bản tin", lag)
        return []
    volumes = daily_volumes(conn, days[0] - timedelta(days=REFERENCE_DAYS), days[-1])
    held = [day for day in days if not volume_plausible(day, volumes)]
    if held:
        logger.warning("Giữ lại %d ngày có khối lượng thấp bất thường, cần kiểm tra trước khi xác nhận tay: %s",
                       len(held), ", ".join(str(d) for d in held[:10]))
    days = [day for day in days if day not in held]
    if not days:
        return []
    conn.execute(text("""INSERT INTO Warehouse_Daily_Coverage (data_origin, source_dataset, full_date)
        VALUES (:origin, '', :day) ON CONFLICT (data_origin, source_dataset, full_date) DO NOTHING"""),
        [{"origin": origin, "day": day} for day in days])
    logger.info("Đã xác nhận coverage %s cho %d ngày (%s → %s)", origin, len(days), days[0], days[-1])
    return days
