"""
extract.py
----------
Đọc file Parquet từ MinIO Data Lake theo partition ngày.

Input:  MinIO bucket `ecom-raw-lake` → `raw/orders/platform=*/year=*/month=*/day=*/*.parquet`
Output: pandas DataFrame chứa dữ liệu thô đã merge từ tất cả partitions.

Tuần 3: Tầng Extract của pipeline ETL.
"""

import io
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

# ─── Cấu hình ───
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_RAW", "ecom-raw-lake")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

logger = logging.getLogger("etl.extract")


def build_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def list_parquet_files(
    client: Minio,
    bucket: str = MINIO_BUCKET,
    prefix: str = "raw/orders/",
    target_date: Optional[datetime] = None,
) -> list[str]:
    """
    Liệt kê tất cả file Parquet trong MinIO theo prefix.

    Nếu target_date được chỉ định, chỉ lấy partition ngày đó.
    """
    if target_date:
        # Duyệt cả 2 platform
        all_files = []
        for platform in ["shopee", "tiktok"]:
            day_prefix = (
                f"{prefix}platform={platform}/"
                f"year={target_date.year:04d}/"
                f"month={target_date.month:02d}/"
                f"day={target_date.day:02d}/"
            )
            objects = client.list_objects(bucket, prefix=day_prefix, recursive=True)
            all_files.extend([
                obj.object_name for obj in objects
                if obj.object_name.endswith(".parquet")
            ])
        return all_files
    else:
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        return [
            obj.object_name for obj in objects
            if obj.object_name.endswith(".parquet")
        ]


def read_parquet_from_minio(client: Minio, bucket: str, object_name: str) -> pd.DataFrame:
    """Đọc 1 file Parquet từ MinIO, trả về DataFrame."""
    try:
        response = client.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()

        buf = io.BytesIO(data)
        table = pq.read_table(buf)
        df = table.to_pandas()

        # Trích xuất platform từ object path
        if "platform=" in object_name:
            platform = object_name.split("platform=")[1].split("/")[0]
            df["_platform"] = platform

        logger.debug("Đọc %s: %d rows", object_name, len(df))
        return df

    except S3Error as e:
        logger.error("Lỗi đọc %s: %s", object_name, e)
        return pd.DataFrame()


from concurrent.futures import ThreadPoolExecutor


def extract(
    target_date: Optional[datetime] = None,
    bucket: str = MINIO_BUCKET,
    client: Optional[Minio] = None,
) -> pd.DataFrame:
    """
    Hàm extract chính: đọc song song tất cả Parquet files từ MinIO và merge.

    Args:
        target_date: Ngày cần extract (None = tất cả dữ liệu).
        bucket: Tên MinIO bucket.
        client: Minio client tái sử dụng nếu có.

    Returns:
        DataFrame chứa dữ liệu thô merge từ tất cả files.
    """
    client = client or build_minio_client()

    files = list_parquet_files(client, bucket, target_date=target_date)
    if not files:
        logger.warning("Không tìm thấy file Parquet nào cho ngày %s", target_date)
        return pd.DataFrame()

    logger.info("Tìm thấy %d file Parquet để extract", len(files))

    dfs = []
    # Đọc song song qua ThreadPoolExecutor để tối ưu I/O mạng
    max_workers = min(16, max(2, len(files)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(read_parquet_from_minio, client, bucket, f) for f in files]
        for fut in futures:
            df = fut.result()
            if not df.empty:
                dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    logger.info(
        "Extract hoàn tất: %d rows từ %d files (ngày: %s)",
        len(result), len(dfs),
        target_date.strftime("%Y-%m-%d") if target_date else "tất cả",
    )
    return result


def extract_orders(
    client: Optional[Minio] = None,
    target_date: Optional[datetime] = None,
    bucket: str = MINIO_BUCKET,
) -> pd.DataFrame:
    """
    Hàm trích xuất đơn hàng (alias tương thích cho pipeline và các caller khác).
    """
    return extract(client=client, target_date=target_date, bucket=bucket)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Extract dữ liệu hôm nay
    today = datetime.now(timezone.utc)
    df = extract(target_date=today)
    print(f"Extracted {len(df)} rows")
    if not df.empty:
        print(df.head())
        print(df.dtypes)
