"""Regression tests for MinIO event-date partitions in Vietnam business time."""

import io
from unittest import mock

import pandas as pd
import pyarrow.parquet as pq
import pytest

from ingestion import consumer_to_minio


class MemoryMinio:
    def __init__(self):
        self.objects = {}

    def put_object(self, *, bucket_name, object_name, data, length, content_type):
        payload = data.read()
        assert len(payload) == length
        self.objects[object_name] = pq.read_table(io.BytesIO(payload)).to_pandas()


def partitions_by_id(records):
    client = MemoryMinio()
    with mock.patch.object(consumer_to_minio, "BUSINESS_TZ", "Asia/Ho_Chi_Minh"):
        uploaded = consumer_to_minio.flush_batch_to_minio(records, client)
    assert uploaded == len(client.objects)
    partitions = {}
    for path, frame in client.objects.items():
        assert not {"_platform", "_year", "_month", "_day"}.intersection(frame.columns)
        prefix = path.rsplit("/", 1)[0]
        for record_id in frame["id"]:
            assert record_id not in partitions
            partitions[record_id] = prefix
    assert len(partitions) == len(records)
    return partitions


def test_mixed_platform_batch_uses_each_events_historical_date():
    records = [
        {"id": "shopee", "_platform": "shopee", "create_time": "2026-07-01T10:00:00+07:00"},
        {"id": "tiktok", "_platform": "tiktok", "created_time": "2026-07-02T10:00:00+07:00"},
    ]
    expected = {
        "shopee": "raw/orders/platform=shopee/year=2026/month=07/day=01",
        "tiktok": "raw/orders/platform=tiktok/year=2026/month=07/day=02",
    }
    assert partitions_by_id(records) == expected
    assert partitions_by_id(list(reversed(records))) == expected


@pytest.mark.parametrize("platform,time_column", [("shopee", "create_time"), ("tiktok", "created_time")])
def test_vietnam_midnight_year_boundary_and_mixed_iso_formats(platform, time_column):
    records = [
        {"id": "before", "_platform": platform, time_column: "2026-12-31T16:59:59Z"},
        {"id": "midnight", "_platform": platform, time_column: "2026-12-31T17:00:00.000Z"},
        {"id": "local", "_platform": platform, time_column: "2027-01-01T00:00:00+07:00"},
        {"id": "other_offset", "_platform": platform, time_column: "2026-12-31T12:00:00-05:00"},
    ]
    result = partitions_by_id(records)
    prefix = f"raw/orders/platform={platform}"
    assert result["before"] == prefix + "/year=2026/month=12/day=31"
    for record_id in ("midnight", "local", "other_offset"):
        assert result[record_id] == prefix + "/year=2027/month=01/day=01"


def test_invalid_primary_timestamp_uses_valid_alternative():
    result = partitions_by_id([
        {"id": "alternative", "_platform": "tiktok", "create_time": "invalid", "created_time": "2026-07-02T20:00:00Z"},
    ])
    assert result["alternative"] == "raw/orders/platform=tiktok/year=2026/month=07/day=03"


def test_missing_or_invalid_timestamps_fall_back_to_vietnam_receipt_date():
    now = pd.Timestamp("2026-12-31T17:30:00Z").tz_convert("Asia/Ho_Chi_Minh")
    records = [
        {"id": "missing", "_platform": "shopee"},
        {"id": "invalid", "_platform": "tiktok", "created_time": "invalid"},
    ]
    with mock.patch.object(consumer_to_minio.pd.Timestamp, "now", return_value=now):
        result = partitions_by_id(records)
    assert result["missing"] == "raw/orders/platform=shopee/year=2027/month=01/day=01"
    assert result["invalid"] == "raw/orders/platform=tiktok/year=2027/month=01/day=01"


def test_offset_free_timestamps_keep_utc_contract_shared_with_etl():
    result = partitions_by_id([
        {"id": "utc", "_platform": "shopee", "create_time": "2026-07-02T20:30:00"},
    ])
    assert result["utc"] == "raw/orders/platform=shopee/year=2026/month=07/day=03"
