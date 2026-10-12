# Bài 1 — Toàn vẹn dữ liệu: FAIL

Lượt thăm dò ban đầu. Tiêu chí `dimension_keys_complete` ở lượt này yêu cầu carrier_key dù nguồn không có carrier; đây là tiêu chí thử nghiệm quá chặt, không phải lỗi ánh xạ. Lượt kiểm tra chính thức sau điều chỉnh: [DI20261013ba6be6](data_integrity_DI20261013ba6be6.md). Bằng chứng mất dòng SKU và thiếu quarantine của lượt này vẫn có giá trị.

Run ID: `DI20261013f7b3a3`
Thời gian Việt Nam: 2026-10-13T01:29:10.334345+07:00

Kafka → consumer thật → MinIO Parquet → ETL thật → PostgreSQL. Chỉ chọn các file của lượt thử này khi chạy ETL.
18 sự kiện tổng hợp: 16 dòng hợp lệ và 2 dòng lỗi. Đây là kiểm tra chức năng toàn vẹn, chưa phải kiểm tra tải production.

| Kiểm tra | Kết quả |
|---|---|
| kafka_to_minio_exactly_preserved | PASS |
| vietnam_partition | PASS |
| all_valid_order_lines_preserved | FAIL |
| retained_line_fields_and_vietnam_dates | PASS |
| cancelled_orders_preserved | PASS |
| same_order_id_across_platforms | PASS |
| warehouse_totals | FAIL |
| dimension_keys_complete | FAIL |
| no_unexpected_warehouse_lines | PASS |
| invalid_records_reach_quarantine_with_reason | FAIL |

Chi tiết bằng chứng:

```json
{
  "run_id": "DI20261013f7b3a3",
  "status": "FAIL",
  "timezone": "Asia/Ho_Chi_Minh",
  "scope": "18 synthetic events; real services; isolated Parquet cohort; no model accuracy test",
  "checks": [
    {
      "name": "kafka_to_minio_exactly_preserved",
      "status": "PASS",
      "evidence": {
        "acked": 18,
        "raw_rows": 18,
        "objects": [
          "raw/orders/platform=shopee/year=2026/month=10/day=12/part-20261013T012904-428a1211-1.parquet",
          "raw/orders/platform=shopee/year=2026/month=10/day=13/part-20261013T012904-428a1211-5.parquet",
          "raw/orders/platform=shopee/year=2026/month=10/day=13/part-20261013T012904-878b2c81-3.parquet",
          "raw/orders/platform=tiktok/year=2026/month=10/day=13/part-20261013T012904-878b2c81-3.parquet",
          "raw/orders/platform=tiktok/year=2026/month=10/day=12/part-20261013T012905-a25fdf03-1.parquet",
          "raw/orders/platform=tiktok/year=2026/month=10/day=13/part-20261013T012905-a25fdf03-5.parquet"
        ],
        "missing": [],
        "extra": []
      }
    },
    {
      "name": "vietnam_partition",
      "status": "PASS",
      "evidence": []
    },
    {
      "name": "all_valid_order_lines_preserved",
      "status": "FAIL",
      "evidence": {
        "expected": 16,
        "actual": 12,
        "missing_lines": [
          {
            "order_id": "DI20261013f7b3a3-multi-s",
            "platform": "shopee",
            "sku": "DI20261013f7b3a3-s-multi-0",
            "quantity": 2,
            "original_price": 10000,
            "subtotal": 20000,
            "buyer_total_amount": 20000,
            "order_status": "COMPLETED",
            "is_cancelled": false,
            "create_time": "2026-10-13T00:01:00",
            "day": "2026-10-13",
            "case": "multi"
          },
          {
            "order_id": "DI20261013f7b3a3-multi-s",
            "platform": "shopee",
            "sku": "DI20261013f7b3a3-s-multi-1",
            "quantity": 3,
            "original_price": 11000,
            "subtotal": 33000,
            "buyer_total_amount": 33000,
            "order_status": "COMPLETED",
            "is_cancelled": false,
            "create_time": "2026-10-13T00:01:00",
            "day": "2026-10-13",
            "case": "multi"
          },
          {
            "order_id": "DI20261013f7b3a3-multi-t",
            "platform": "tiktok",
            "sku": "DI20261013f7b3a3-t-multi-0",
            "quantity": 2,
            "original_price": 10000,
            "subtotal": 20000,
            "buyer_total_amount": 20000,
            "order_status": "COMPLETED",
            "is_cancelled": false,
            "create_time": "2026-10-13T00:01:00",
            "day": "2026-10-13",
            "case": "multi"
          },
          {
            "order_id": "DI20261013f7b3a3-multi-t",
            "platform": "tiktok",
            "sku": "DI20261013f7b3a3-t-multi-1",
            "quantity": 3,
            "original_price": 11000,
            "subtotal": 33000,
            "buyer_total_amount": 33000,
            "order_status": "COMPLETED",
            "is_cancelled": false,
            "create_time": "2026-10-13T00:01:00",
            "day": "2026-10-13",
            "case": "multi"
          }
        ]
      }
    },
    {
      "name": "retained_line_fields_and_vietnam_dates",
      "status": "PASS",
      "evidence": []
    },
    {
      "name": "cancelled_orders_preserved",
      "status": "PASS",
      "evidence": [
        {
          "order_id": "DI20261013f7b3a3-cancelled-s",
          "platform": "shopee",
          "sku": "DI20261013f7b3a3-s-cancelled-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "CANCELLED",
          "is_cancelled": true,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 212,
          "shop_key": 22038,
          "geo_key": 11,
          "date_key": 651,
          "payment_key": 5,
          "carrier_key": null
        },
        {
          "order_id": "DI20261013f7b3a3-cancelled-t",
          "platform": "tiktok",
          "sku": "DI20261013f7b3a3-t-cancelled-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "CANCELLED",
          "is_cancelled": true,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 217,
          "shop_key": 22039,
          "geo_key": 9,
          "date_key": 651,
          "payment_key": 1,
          "carrier_key": 7
        }
      ]
    },
    {
      "name": "same_order_id_across_platforms",
      "status": "PASS",
      "evidence": [
        {
          "order_id": "DI20261013f7b3a3-shared",
          "platform": "shopee",
          "sku": "DI20261013f7b3a3-s-shared-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "COMPLETED",
          "is_cancelled": false,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 213,
          "shop_key": 22038,
          "geo_key": 15,
          "date_key": 651,
          "payment_key": 6,
          "carrier_key": 1
        },
        {
          "order_id": "DI20261013f7b3a3-shared",
          "platform": "tiktok",
          "sku": "DI20261013f7b3a3-t-shared-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "COMPLETED",
          "is_cancelled": false,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 218,
          "shop_key": 22039,
          "geo_key": 13,
          "date_key": 651,
          "payment_key": 1,
          "carrier_key": 6
        }
      ]
    },
    {
      "name": "warehouse_totals",
      "status": "FAIL",
      "evidence": {
        "expected_units": 38,
        "actual_units": 28,
        "expected_subtotal_vnd": 402000,
        "actual_subtotal_vnd": "296000.00"
      }
    },
    {
      "name": "dimension_keys_complete",
      "status": "FAIL",
      "evidence": [
        {
          "key": [
            "shopee",
            "DI20261013f7b3a3-after_midnight-s",
            "DI20261013f7b3a3-s-after_midnight-0"
          ],
          "field": "carrier_key"
        },
        {
          "key": [
            "shopee",
            "DI20261013f7b3a3-cancelled-s",
            "DI20261013f7b3a3-s-cancelled-0"
          ],
          "field": "carrier_key"
        }
      ]
    },
    {
      "name": "no_unexpected_warehouse_lines",
      "status": "PASS",
      "evidence": []
    },
    {
      "name": "invalid_records_reach_quarantine_with_reason",
      "status": "FAIL",
      "evidence": {
        "invalid_inputs": [
          {
            "order_id": "DI20261013f7b3a3-invalid-s",
            "platform": "shopee",
            "reason": "missing SKU"
          },
          {
            "order_id": "DI20261013f7b3a3-invalid-t",
            "platform": "tiktok",
            "reason": "quantity=0"
          }
        ],
        "quarantine": [],
        "note": "Raw records remain in MinIO. Pre-validation cleaning drops these inputs; pipeline currently does not persist quarantine."
      }
    }
  ],
  "expected_lines": [
    {
      "order_id": "DI20261013f7b3a3-normal-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-normal-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "normal"
    },
    {
      "order_id": "DI20261013f7b3a3-cancelled-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-cancelled-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "CANCELLED",
      "is_cancelled": true,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "cancelled"
    },
    {
      "order_id": "DI20261013f7b3a3-shared",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-shared-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "shared"
    },
    {
      "order_id": "DI20261013f7b3a3-before_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-before_midnight-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-12T23:59:00",
      "day": "2026-10-12",
      "case": "before_midnight"
    },
    {
      "order_id": "DI20261013f7b3a3-after_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-after_midnight-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "after_midnight"
    },
    {
      "order_id": "DI20261013f7b3a3-multi-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-multi-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "multi"
    },
    {
      "order_id": "DI20261013f7b3a3-multi-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-multi-1",
      "quantity": 3,
      "original_price": 11000,
      "subtotal": 33000,
      "buyer_total_amount": 33000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "multi"
    },
    {
      "order_id": "DI20261013f7b3a3-multi-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-multi-2",
      "quantity": 4,
      "original_price": 12000,
      "subtotal": 48000,
      "buyer_total_amount": 48000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "multi"
    },
    {
      "order_id": "DI20261013f7b3a3-normal-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-normal-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "normal"
    },
    {
      "order_id": "DI20261013f7b3a3-cancelled-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-cancelled-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "CANCELLED",
      "is_cancelled": true,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "cancelled"
    },
    {
      "order_id": "DI20261013f7b3a3-shared",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-shared-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "shared"
    },
    {
      "order_id": "DI20261013f7b3a3-before_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-before_midnight-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-12T23:59:00",
      "day": "2026-10-12",
      "case": "before_midnight"
    },
    {
      "order_id": "DI20261013f7b3a3-after_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-after_midnight-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "after_midnight"
    },
    {
      "order_id": "DI20261013f7b3a3-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-multi-0",
      "quantity": 2,
      "original_price": 10000,
      "subtotal": 20000,
      "buyer_total_amount": 20000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "multi"
    },
    {
      "order_id": "DI20261013f7b3a3-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-multi-1",
      "quantity": 3,
      "original_price": 11000,
      "subtotal": 33000,
      "buyer_total_amount": 33000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "multi"
    },
    {
      "order_id": "DI20261013f7b3a3-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-multi-2",
      "quantity": 4,
      "original_price": 12000,
      "subtotal": 48000,
      "buyer_total_amount": 48000,
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13T00:01:00",
      "day": "2026-10-13",
      "case": "multi"
    }
  ],
  "invalid_inputs": [
    {
      "order_id": "DI20261013f7b3a3-invalid-s",
      "platform": "shopee",
      "reason": "missing SKU"
    },
    {
      "order_id": "DI20261013f7b3a3-invalid-t",
      "platform": "tiktok",
      "reason": "quantity=0"
    }
  ],
  "kafka_acks": [
    {
      "event_id": "DI20261013f7b3a3-00",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3139
    },
    {
      "event_id": "DI20261013f7b3a3-01",
      "topic": "ecom.orders.raw",
      "partition": 0,
      "offset": 2991
    },
    {
      "event_id": "DI20261013f7b3a3-02",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3140
    },
    {
      "event_id": "DI20261013f7b3a3-03",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3141
    },
    {
      "event_id": "DI20261013f7b3a3-04",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3142
    },
    {
      "event_id": "DI20261013f7b3a3-05",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3084
    },
    {
      "event_id": "DI20261013f7b3a3-06",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3085
    },
    {
      "event_id": "DI20261013f7b3a3-07",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3086
    },
    {
      "event_id": "DI20261013f7b3a3-08",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3143
    },
    {
      "event_id": "DI20261013f7b3a3-09",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3087
    },
    {
      "event_id": "DI20261013f7b3a3-10",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3144
    },
    {
      "event_id": "DI20261013f7b3a3-11",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3145
    },
    {
      "event_id": "DI20261013f7b3a3-12",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3146
    },
    {
      "event_id": "DI20261013f7b3a3-13",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3088
    },
    {
      "event_id": "DI20261013f7b3a3-14",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3089
    },
    {
      "event_id": "DI20261013f7b3a3-15",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3090
    },
    {
      "event_id": "DI20261013f7b3a3-16",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3091
    },
    {
      "event_id": "DI20261013f7b3a3-17",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3092
    }
  ],
  "stages": {
    "raw": 18,
    "unified": 18,
    "after_cleaning": 12,
    "valid": 12,
    "quarantined": 0,
    "cleaning": {
      "original_rows": 18,
      "duplicates_removed": 4,
      "missing_key_removed": 1,
      "quantity_outliers_removed": 1,
      "clean_rows": 12,
      "clean_pct": 66.7
    },
    "validation": {
      "total_records": 12,
      "valid_records": 12,
      "invalid_records": 0,
      "pass_rate": 1.0,
      "error_counts": {},
      "warnings": []
    }
  },
  "etl": [
    {
      "status": "SUCCESS",
      "target_date": "2026-10-12",
      "raw_records": 2,
      "unified_records": 2,
      "clean_records": 2,
      "quarantined_records": 0,
      "loaded_records": 2,
      "elapsed_seconds": 0.3
    },
    {
      "status": "SUCCESS",
      "target_date": "2026-10-13",
      "raw_records": 16,
      "unified_records": 16,
      "clean_records": 10,
      "quarantined_records": 0,
      "loaded_records": 10,
      "elapsed_seconds": 0.29
    }
  ],
  "warehouse_rows": [
    {
      "order_id": "DI20261013f7b3a3-after_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-after_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 214,
      "shop_key": 22038,
      "geo_key": 2,
      "date_key": 651,
      "payment_key": 3,
      "carrier_key": null
    },
    {
      "order_id": "DI20261013f7b3a3-before_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-before_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-12 23:59:00",
      "day": "2026-10-12",
      "product_key": 209,
      "shop_key": 22038,
      "geo_key": 14,
      "date_key": 650,
      "payment_key": 2,
      "carrier_key": 7
    },
    {
      "order_id": "DI20261013f7b3a3-cancelled-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-cancelled-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "CANCELLED",
      "is_cancelled": true,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 212,
      "shop_key": 22038,
      "geo_key": 11,
      "date_key": 651,
      "payment_key": 5,
      "carrier_key": null
    },
    {
      "order_id": "DI20261013f7b3a3-multi-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-multi-2",
      "quantity": 4,
      "original_price": "12000.00",
      "subtotal": "48000.00",
      "buyer_total_amount": "48000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 215,
      "shop_key": 22038,
      "geo_key": 20,
      "date_key": 651,
      "payment_key": 1,
      "carrier_key": 5
    },
    {
      "order_id": "DI20261013f7b3a3-normal-s",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-normal-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 211,
      "shop_key": 22038,
      "geo_key": 20,
      "date_key": 651,
      "payment_key": 4,
      "carrier_key": 1
    },
    {
      "order_id": "DI20261013f7b3a3-shared",
      "platform": "shopee",
      "sku": "DI20261013f7b3a3-s-shared-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 213,
      "shop_key": 22038,
      "geo_key": 15,
      "date_key": 651,
      "payment_key": 6,
      "carrier_key": 1
    },
    {
      "order_id": "DI20261013f7b3a3-after_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-after_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 219,
      "shop_key": 22039,
      "geo_key": 18,
      "date_key": 651,
      "payment_key": 9,
      "carrier_key": 7
    },
    {
      "order_id": "DI20261013f7b3a3-before_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-before_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-12 23:59:00",
      "day": "2026-10-12",
      "product_key": 210,
      "shop_key": 22039,
      "geo_key": 19,
      "date_key": 650,
      "payment_key": 8,
      "carrier_key": 7
    },
    {
      "order_id": "DI20261013f7b3a3-cancelled-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-cancelled-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "CANCELLED",
      "is_cancelled": true,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 217,
      "shop_key": 22039,
      "geo_key": 9,
      "date_key": 651,
      "payment_key": 1,
      "carrier_key": 7
    },
    {
      "order_id": "DI20261013f7b3a3-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-multi-2",
      "quantity": 4,
      "original_price": "12000.00",
      "subtotal": "48000.00",
      "buyer_total_amount": "48000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 220,
      "shop_key": 22039,
      "geo_key": 7,
      "date_key": 651,
      "payment_key": 4,
      "carrier_key": 7
    },
    {
      "order_id": "DI20261013f7b3a3-normal-t",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-normal-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 216,
      "shop_key": 22039,
      "geo_key": 12,
      "date_key": 651,
      "payment_key": 9,
      "carrier_key": 1
    },
    {
      "order_id": "DI20261013f7b3a3-shared",
      "platform": "tiktok",
      "sku": "DI20261013f7b3a3-t-shared-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 218,
      "shop_key": 22039,
      "geo_key": 13,
      "date_key": 651,
      "payment_key": 1,
      "carrier_key": 6
    }
  ],
  "completed_at_vietnam": "2026-10-13T01:29:10.334345+07:00"
}
```
