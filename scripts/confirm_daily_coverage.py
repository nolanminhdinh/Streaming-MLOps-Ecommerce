"""Confirm a fully ingested Vietnam business day for forecast scoring.

Run only after reconciling all expected input records for that day. No inference
of completeness is made from the mere presence of orders or passing ETL batches.
Historical CSVs require one confirmation per source fingerprint and day.
"""

import argparse
import os
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from warehouse.etl.load import get_engine


def confirm_daily_coverage(data_origin: str, full_date: date, source_dataset: str = "") -> None:
    if full_date >= datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date():
        raise ValueError("Only completed Vietnam business days can be confirmed.")
    if data_origin == "public_anonymized_historical_csv":
        if not source_dataset:
            raise ValueError("Historical coverage requires a dataset fingerprint.")
    elif data_origin not in {"unverified_warehouse", "warehouse_live"} or source_dataset:
        raise ValueError("Streaming coverage requires a supported origin and an empty dataset.")
    engine = get_engine()
    try:
        with engine.begin() as conn:
            if source_dataset and not conn.execute(text("""
                SELECT 1 FROM Fact_Historical_Order_Lines
                WHERE data_origin = :origin AND source_dataset = :dataset LIMIT 1
            """), {"origin": data_origin, "dataset": source_dataset}).scalar():
                raise ValueError("Dataset does not exist in the selected warehouse origin.")
            conn.execute(text("""
                INSERT INTO Warehouse_Daily_Coverage (data_origin, source_dataset, full_date)
                VALUES (:origin, :dataset, :day)
                ON CONFLICT (data_origin, source_dataset, full_date)
                DO UPDATE SET confirmed_at = NOW()
            """), {"origin": data_origin, "dataset": source_dataset, "day": full_date})
    finally:
        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-origin", required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--source-dataset", default="")
    args = parser.parse_args()
    confirm_daily_coverage(args.data_origin, args.date, args.source_dataset)
    print("Confirmed daily coverage.")
