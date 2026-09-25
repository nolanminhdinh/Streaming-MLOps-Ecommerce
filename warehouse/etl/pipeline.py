"""
pipeline.py
-----------
Orchestrator điều phối toàn bộ chu trình ETL:
  MinIO (Data Lake) → Transform & Clean → Data Quality Validation → PostgreSQL (Star Schema).

Quy trình:
  1. [EXTRACT]    Đọc các file Parquet phân vùng theo ngày/platform từ MinIO.
  2. [TRANSFORM]  Chuẩn hóa schema Shopee + TikTok, làm sạch dữ liệu, xử lý missing & outliers.
  3. [VALIDATE]   Kiểm định tính toàn vẹn (Null checks, Range checks, Logic thời gian).
  4. [LOAD]       Upsert các bảng Dimension và nạp Fact_Orders vào PostgreSQL.

Sử dụng:
  python warehouse/etl/pipeline.py --date 2026-09-24
  python warehouse/etl/pipeline.py --dry-run
  python warehouse/etl/pipeline.py --all-dates
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

# Cho phép import các module trong cùng package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from warehouse.etl.extract import build_minio_client, extract_orders
from warehouse.etl.transform import unify_schema, clean_data, generate_quality_report
from warehouse.etl.data_validation import DataValidator
from warehouse.etl.load import get_engine, load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl.pipeline")


def run_etl_pipeline(
    target_date: Optional[datetime] = None,
    dry_run: bool = False,
    batch_size: int = 1000,
) -> dict:
    """
    Thực thi chu trình ETL hoàn chỉnh.

    Args:
        target_date: Ngày cụ thể cần xử lý (None = xử lý toàn bộ partition tìm thấy).
        dry_run: Nếu True, dừng lại ở bước Validate và không ghi vào PostgreSQL.
        batch_size: Kích thước batch nạp vào PostgreSQL.

    Returns:
        dict thống kê kết quả thực thi.
    """
    start_time = time.time()
    logger.info("╔" + "═" * 58 + "╗")
    logger.info("║         BẮT ĐẦU CHU TRÌNH ETL: DATA LAKE → STAR SCHEMA    ║")
    logger.info("╚" + "═" * 58 + "╝")
    date_str = target_date.strftime("%Y-%m-%d") if target_date else "ALL AVAILABLE"
    logger.info("Target Date: %s | Dry Run: %s | Batch Size: %d", date_str, dry_run, batch_size)

    stats = {
        "status": "FAILED",
        "target_date": date_str,
        "raw_records": 0,
        "unified_records": 0,
        "clean_records": 0,
        "quarantined_records": 0,
        "loaded_records": 0,
        "elapsed_seconds": 0.0,
    }

    # ─────────────────────────────────────────────────────────
    # BƯỚC 1: EXTRACT TỪ MINIO
    # ─────────────────────────────────────────────────────────
    logger.info("─── BƯỚC 1/4: EXTRACT ───")
    try:
        minio_client = build_minio_client()
        raw_df = extract_orders(minio_client, target_date=target_date)
        stats["raw_records"] = len(raw_df)
    except Exception as e:
        logger.error("Lỗi khi kết nối hoặc trích xuất từ MinIO: %s", e)
        raise

    if raw_df.empty:
        logger.warning("Không tìm thấy dữ liệu Parquet nào trên MinIO cho ngày: %s", date_str)
        stats["status"] = "NO_DATA"
        stats["elapsed_seconds"] = round(time.time() - start_time, 2)
        return stats

    logger.info("✓ Extract thành công: %d records thô", len(raw_df))

    # ─────────────────────────────────────────────────────────
    # BƯỚC 2: TRANSFORM & CLEAN
    # ─────────────────────────────────────────────────────────
    logger.info("─── BƯỚC 2/4: TRANSFORM ───")
    unified_df = unify_schema(raw_df)
    stats["unified_records"] = len(unified_df)
    logger.info("✓ Unify schema (Shopee + TikTok): %d records", len(unified_df))

    transformed_df = clean_data(unified_df)
    quality_report = generate_quality_report(unified_df, transformed_df)
    logger.info(
        "✓ Clean data hoàn tất: %d records giữ lại (%d duplicate bị loại)",
        len(transformed_df), quality_report.get("duplicates_removed", 0),
    )

    # ─────────────────────────────────────────────────────────
    # BƯỚC 3: DATA VALIDATION & QUALITY ASSURANCE
    # ─────────────────────────────────────────────────────────
    logger.info("─── BƯỚC 3/4: VALIDATE ───")
    validator = DataValidator()
    clean_df, quarantine_df, val_report = validator.validate(transformed_df)
    val_report.print_summary()

    stats["clean_records"] = len(clean_df)
    stats["quarantined_records"] = len(quarantine_df)

    if not quarantine_df.empty:
        logger.warning(
            "CẢNH BÁO: Đã cách ly %d bản ghi không đạt chuẩn chất lượng dữ liệu.",
            len(quarantine_df),
        )

    # ─────────────────────────────────────────────────────────
    # BƯỚC 4: LOAD VÀO POSTGRESQL (STAR SCHEMA)
    # ─────────────────────────────────────────────────────────
    logger.info("─── BƯỚC 4/4: LOAD ───")
    if dry_run:
        logger.info("DRY-RUN: Bỏ qua nạp vào PostgreSQL. Dữ liệu sạch: %d records.", len(clean_df))
        stats["loaded_records"] = 0
        stats["status"] = "DRY_RUN_SUCCESS"
    else:
        if clean_df.empty:
            logger.warning("Không có bản ghi hợp lệ nào để nạp vào PostgreSQL.")
            stats["status"] = "EMPTY_CLEAN_DATA"
        else:
            try:
                load_result = load(clean_df)
                loaded_count = load_result.get("rows_loaded", 0)
                stats["loaded_records"] = loaded_count
                stats["status"] = "SUCCESS"
                logger.info("✓ Nạp thành công %d Fact_Orders vào PostgreSQL", loaded_count)
            except Exception as e:
                logger.error("Lỗi nạp dữ liệu vào PostgreSQL: %s", e)
                stats["status"] = "LOAD_ERROR"
                raise

    stats["elapsed_seconds"] = round(time.time() - start_time, 2)
    logger.info("═" * 60)
    logger.info("  KẾT THÚC CHU TRÌNH ETL — Trạng thái: %s", stats["status"])
    logger.info("  Thời gian thực thi: %.2f giây", stats["elapsed_seconds"])
    logger.info("  Thống kê: Raw=%d → Clean=%d → Loaded=%d | Quarantined=%d",
                stats["raw_records"], stats["clean_records"],
                stats["loaded_records"], stats["quarantined_records"])
    logger.info("═" * 60)

    return stats


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline: MinIO → PostgreSQL")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Ngày cần xử lý định dạng YYYY-MM-DD (Mặc định: hôm nay)",
    )
    parser.add_argument(
        "--all-dates",
        action="store_true",
        help="Xử lý toàn bộ các ngày có trong Data Lake",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chạy Extract, Transform, Validate mà không nạp vào PostgreSQL",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Kích thước batch khi insert vào Fact_Orders (mặc định 1000)",
    )

    args = parser.parse_args()

    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Định dạng ngày không hợp lệ. Vui lòng dùng YYYY-MM-DD")
            sys.exit(1)
    elif not args.all_dates:
        # Mặc định lấy ngày hôm nay (UTC)
        target_date = datetime.now(timezone.utc)

    try:
        stats = run_etl_pipeline(
            target_date=target_date,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
        )
        if stats["status"] in ("SUCCESS", "DRY_RUN_SUCCESS"):
            sys.exit(0)
        else:
            sys.exit(0 if stats["status"] == "NO_DATA" else 1)
    except Exception as exc:
        logger.exception("Pipeline thất bại với ngoại lệ: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
