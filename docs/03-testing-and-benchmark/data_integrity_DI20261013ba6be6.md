# Bài 1 — Toàn vẹn dữ liệu: FAIL

Run ID: `DI20261013ba6be6`
Thời gian Việt Nam: 2026-10-13T01:30:46.761448+07:00

Kafka → consumer thật → MinIO Parquet → ETL thật → PostgreSQL. Chỉ chọn các file của lượt thử này khi chạy ETL.
18 sự kiện tổng hợp: 16 dòng hợp lệ và 2 dòng lỗi. Đây là kiểm tra chức năng toàn vẹn, chưa phải kiểm tra tải production.

Kết quả: **7/10 tiêu chí đạt, bài 1 FAIL**. Hai lần chạy hoàn chỉnh xác nhận cùng lỗi mất dòng SKU và không đưa các bản ghi lỗi thử nghiệm vào quarantine.

| Chỉ số đối soát | Kỳ vọng | Thực tế |
|---|---:|---:|
| Sự kiện Kafka đã ack / dòng MinIO | 18 | 18 |
| Dòng hàng hợp lệ trong warehouse | 16 | 12 |
| Tổng số lượng, gồm cả đơn hủy | 38 | 28 |
| Tổng subtotal, gồm cả đơn hủy (VND) | 402.000 | 296.000 |
| Bản ghi lỗi đưa vào quarantine | 2 | 0 |

Các số tiền là tổng measure của fixture để đối soát, không phải doanh thu thuần sau loại đơn hủy.

Nguyên nhân và hướng sửa trước khi nghiệm thu production:

1. **P1 — Mất dòng hàng của đơn nhiều SKU.** `clean_data()` khử trùng theo `(order_id, platform)`; ràng buộc UNIQUE và upsert của `Fact_Orders` cũng dùng khóa này. Hai đơn, mỗi đơn ba SKU, chỉ giữ một SKU mỗi đơn: thiếu bốn dòng, mười sản phẩm và 106.000 VND subtotal. Cần thống nhất grain dòng hàng, có khóa dòng ổn định từ nguồn, rồi sửa dedupe, schema/upsert và các truy vấn đếm đơn tương ứng. Chỉ thêm SKU vào dedupe không giải quyết ràng buộc database hoặc trường hợp hai dòng cùng SKU.
2. **P1 — Không truy vết được việc từ chối dữ liệu qua quarantine.** Dòng thiếu SKU và dòng quantity=0 bị cleaning loại trước validator. Validator báo pass 100% trên các dòng còn lại, quarantine=0. Pipeline cũng chưa ghi quarantine ra kho bền vững. Cần lưu payload/định danh nguồn, lý do, lần chạy và trạng thái xử lý lại cho mọi dòng bị từ chối.
3. **P2 — Báo cáo duplicate sai phân loại.** `generate_quality_report()` tính toàn bộ chênh lệch trước/sau clean là duplicate: log ETL ngày 13 báo sáu duplicate dù thực tế bốn dòng SKU bị nhận nhầm là trùng và hai dòng lỗi bị loại vì lý do khác. Cần tách thống kê theo từng lý do loại và đối soát tổng đầu vào/đầu ra.

Dimension được đối soát theo dữ liệu nguồn: carrier/payment thiếu từ nguồn là trường tùy chọn; các khóa nguồn có giá trị đều được ánh xạ trong lượt chạy cuối.

NiFi, tồn kho, serving và độ chính xác mô hình không thuộc phạm vi bài này. Dữ liệu thô vẫn còn trong MinIO; đây là mất dòng ở tầng xử lý/warehouse. Test facts dùng tiền tố riêng được giữ để kiểm tra, không xóa dữ liệu.

Chạy lại từ thư mục dự án: `.venv\Scripts\python.exe scripts/run_data_integrity_check.py`. Exit code 1 thể hiện không đạt integrity gate. Lượt chạy dùng chuyển hướng log đã lỗi `Invalid file descriptor: -1` trước khi gửi sự kiện; chạy trực tiếp sau đó hoàn thành. Chưa sửa logic nghiệp vụ hoặc commit/push trong bài kiểm tra này.

| Kiểm tra | Kết quả |
|---|---|
| kafka_to_minio_exactly_preserved | PASS |
| vietnam_partition | PASS |
| all_valid_order_lines_preserved | FAIL |
| retained_line_fields_and_vietnam_dates | PASS |
| cancelled_orders_preserved | PASS |
| same_order_id_across_platforms | PASS |
| warehouse_totals | FAIL |
| dimension_keys_when_source_present | PASS |
| no_unexpected_warehouse_lines | PASS |
| invalid_records_reach_quarantine_with_reason | FAIL |

Chi tiết bằng chứng:

```json
{
  "run_id": "DI20261013ba6be6",
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
          "raw/orders/platform=shopee/year=2026/month=10/day=12/part-20261013T013040-51602dbd-1.parquet",
          "raw/orders/platform=shopee/year=2026/month=10/day=13/part-20261013T013040-51602dbd-5.parquet",
          "raw/orders/platform=shopee/year=2026/month=10/day=13/part-20261013T013041-c424cc21-3.parquet",
          "raw/orders/platform=tiktok/year=2026/month=10/day=13/part-20261013T013041-c424cc21-3.parquet",
          "raw/orders/platform=tiktok/year=2026/month=10/day=12/part-20261013T013041-5b843611-1.parquet",
          "raw/orders/platform=tiktok/year=2026/month=10/day=13/part-20261013T013041-5b843611-5.parquet"
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
            "order_id": "DI20261013ba6be6-multi-s",
            "platform": "shopee",
            "sku": "DI20261013ba6be6-s-multi-0",
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
            "order_id": "DI20261013ba6be6-multi-s",
            "platform": "shopee",
            "sku": "DI20261013ba6be6-s-multi-1",
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
            "order_id": "DI20261013ba6be6-multi-t",
            "platform": "tiktok",
            "sku": "DI20261013ba6be6-t-multi-0",
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
            "order_id": "DI20261013ba6be6-multi-t",
            "platform": "tiktok",
            "sku": "DI20261013ba6be6-t-multi-1",
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
          "order_id": "DI20261013ba6be6-cancelled-s",
          "platform": "shopee",
          "sku": "DI20261013ba6be6-s-cancelled-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "CANCELLED",
          "is_cancelled": true,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 224,
          "shop_key": 22042,
          "geo_key": 16,
          "date_key": 651,
          "payment_key": 7,
          "carrier_key": 1
        },
        {
          "order_id": "DI20261013ba6be6-cancelled-t",
          "platform": "tiktok",
          "sku": "DI20261013ba6be6-t-cancelled-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "CANCELLED",
          "is_cancelled": true,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 231,
          "shop_key": 22043,
          "geo_key": 20,
          "date_key": 651,
          "payment_key": 8,
          "carrier_key": 1
        }
      ]
    },
    {
      "name": "same_order_id_across_platforms",
      "status": "PASS",
      "evidence": [
        {
          "order_id": "DI20261013ba6be6-shared",
          "platform": "shopee",
          "sku": "DI20261013ba6be6-s-shared-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "COMPLETED",
          "is_cancelled": false,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 225,
          "shop_key": 22042,
          "geo_key": 19,
          "date_key": 651,
          "payment_key": 2,
          "carrier_key": 6
        },
        {
          "order_id": "DI20261013ba6be6-shared",
          "platform": "tiktok",
          "sku": "DI20261013ba6be6-t-shared-0",
          "quantity": 2,
          "original_price": "10000.00",
          "subtotal": "20000.00",
          "buyer_total_amount": "20000.00",
          "order_status": "COMPLETED",
          "is_cancelled": false,
          "create_time": "2026-10-13 00:01:00",
          "day": "2026-10-13",
          "product_key": 232,
          "shop_key": 22043,
          "geo_key": 7,
          "date_key": 651,
          "payment_key": 7,
          "carrier_key": 3
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
      "name": "dimension_keys_when_source_present",
      "status": "PASS",
      "evidence": {
        "unresolved_keys": [],
        "optional_source_absences": [
          {
            "key": [
              "shopee",
              "DI20261013ba6be6-after_midnight-s",
              "DI20261013ba6be6-s-after_midnight-0"
            ],
            "field": "carrier_key",
            "reason": "source value absent"
          },
          {
            "key": [
              "shopee",
              "DI20261013ba6be6-before_midnight-s",
              "DI20261013ba6be6-s-before_midnight-0"
            ],
            "field": "carrier_key",
            "reason": "source value absent"
          },
          {
            "key": [
              "tiktok",
              "DI20261013ba6be6-normal-t",
              "DI20261013ba6be6-t-normal-0"
            ],
            "field": "carrier_key",
            "reason": "source value absent"
          }
        ]
      }
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
            "order_id": "DI20261013ba6be6-invalid-s",
            "platform": "shopee",
            "reason": "missing SKU"
          },
          {
            "order_id": "DI20261013ba6be6-invalid-t",
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
      "order_id": "DI20261013ba6be6-normal-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-normal-0",
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
      "order_id": "DI20261013ba6be6-cancelled-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-cancelled-0",
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
      "order_id": "DI20261013ba6be6-shared",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-shared-0",
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
      "order_id": "DI20261013ba6be6-before_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-before_midnight-0",
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
      "order_id": "DI20261013ba6be6-after_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-after_midnight-0",
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
      "order_id": "DI20261013ba6be6-multi-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-multi-0",
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
      "order_id": "DI20261013ba6be6-multi-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-multi-1",
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
      "order_id": "DI20261013ba6be6-multi-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-multi-2",
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
      "order_id": "DI20261013ba6be6-normal-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-normal-0",
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
      "order_id": "DI20261013ba6be6-cancelled-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-cancelled-0",
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
      "order_id": "DI20261013ba6be6-shared",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-shared-0",
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
      "order_id": "DI20261013ba6be6-before_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-before_midnight-0",
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
      "order_id": "DI20261013ba6be6-after_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-after_midnight-0",
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
      "order_id": "DI20261013ba6be6-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-multi-0",
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
      "order_id": "DI20261013ba6be6-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-multi-1",
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
      "order_id": "DI20261013ba6be6-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-multi-2",
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
      "order_id": "DI20261013ba6be6-invalid-s",
      "platform": "shopee",
      "reason": "missing SKU"
    },
    {
      "order_id": "DI20261013ba6be6-invalid-t",
      "platform": "tiktok",
      "reason": "quantity=0"
    }
  ],
  "kafka_acks": [
    {
      "event_id": "DI20261013ba6be6-00",
      "topic": "ecom.orders.raw",
      "partition": 0,
      "offset": 2992
    },
    {
      "event_id": "DI20261013ba6be6-01",
      "topic": "ecom.orders.raw",
      "partition": 0,
      "offset": 2993
    },
    {
      "event_id": "DI20261013ba6be6-02",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3093
    },
    {
      "event_id": "DI20261013ba6be6-03",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3094
    },
    {
      "event_id": "DI20261013ba6be6-04",
      "topic": "ecom.orders.raw",
      "partition": 0,
      "offset": 2994
    },
    {
      "event_id": "DI20261013ba6be6-05",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3095
    },
    {
      "event_id": "DI20261013ba6be6-06",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3096
    },
    {
      "event_id": "DI20261013ba6be6-07",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3097
    },
    {
      "event_id": "DI20261013ba6be6-08",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3098
    },
    {
      "event_id": "DI20261013ba6be6-09",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3147
    },
    {
      "event_id": "DI20261013ba6be6-10",
      "topic": "ecom.orders.raw",
      "partition": 0,
      "offset": 2995
    },
    {
      "event_id": "DI20261013ba6be6-11",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3099
    },
    {
      "event_id": "DI20261013ba6be6-12",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3148
    },
    {
      "event_id": "DI20261013ba6be6-13",
      "topic": "ecom.orders.raw",
      "partition": 2,
      "offset": 3100
    },
    {
      "event_id": "DI20261013ba6be6-14",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3149
    },
    {
      "event_id": "DI20261013ba6be6-15",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3150
    },
    {
      "event_id": "DI20261013ba6be6-16",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3151
    },
    {
      "event_id": "DI20261013ba6be6-17",
      "topic": "ecom.orders.raw",
      "partition": 1,
      "offset": 3152
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
      "elapsed_seconds": 0.28
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
      "order_id": "DI20261013ba6be6-after_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-after_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 226,
      "shop_key": 22042,
      "geo_key": 17,
      "date_key": 651,
      "payment_key": 7,
      "carrier_key": null
    },
    {
      "order_id": "DI20261013ba6be6-before_midnight-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-before_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-12 23:59:00",
      "day": "2026-10-12",
      "product_key": 221,
      "shop_key": 22042,
      "geo_key": 15,
      "date_key": 650,
      "payment_key": 2,
      "carrier_key": null
    },
    {
      "order_id": "DI20261013ba6be6-cancelled-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-cancelled-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "CANCELLED",
      "is_cancelled": true,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 224,
      "shop_key": 22042,
      "geo_key": 16,
      "date_key": 651,
      "payment_key": 7,
      "carrier_key": 1
    },
    {
      "order_id": "DI20261013ba6be6-multi-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-multi-2",
      "quantity": 4,
      "original_price": "12000.00",
      "subtotal": "48000.00",
      "buyer_total_amount": "48000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 227,
      "shop_key": 22042,
      "geo_key": 15,
      "date_key": 651,
      "payment_key": 5,
      "carrier_key": 6
    },
    {
      "order_id": "DI20261013ba6be6-normal-s",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-normal-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 223,
      "shop_key": 22042,
      "geo_key": 10,
      "date_key": 651,
      "payment_key": 5,
      "carrier_key": 2
    },
    {
      "order_id": "DI20261013ba6be6-shared",
      "platform": "shopee",
      "sku": "DI20261013ba6be6-s-shared-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 225,
      "shop_key": 22042,
      "geo_key": 19,
      "date_key": 651,
      "payment_key": 2,
      "carrier_key": 6
    },
    {
      "order_id": "DI20261013ba6be6-after_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-after_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 228,
      "shop_key": 22043,
      "geo_key": 15,
      "date_key": 651,
      "payment_key": 8,
      "carrier_key": 5
    },
    {
      "order_id": "DI20261013ba6be6-before_midnight-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-before_midnight-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-12 23:59:00",
      "day": "2026-10-12",
      "product_key": 222,
      "shop_key": 22043,
      "geo_key": 14,
      "date_key": 650,
      "payment_key": 3,
      "carrier_key": 5
    },
    {
      "order_id": "DI20261013ba6be6-cancelled-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-cancelled-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "CANCELLED",
      "is_cancelled": true,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 231,
      "shop_key": 22043,
      "geo_key": 20,
      "date_key": 651,
      "payment_key": 8,
      "carrier_key": 1
    },
    {
      "order_id": "DI20261013ba6be6-multi-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-multi-2",
      "quantity": 4,
      "original_price": "12000.00",
      "subtotal": "48000.00",
      "buyer_total_amount": "48000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 229,
      "shop_key": 22043,
      "geo_key": 6,
      "date_key": 651,
      "payment_key": 1,
      "carrier_key": 3
    },
    {
      "order_id": "DI20261013ba6be6-normal-t",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-normal-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 230,
      "shop_key": 22043,
      "geo_key": 17,
      "date_key": 651,
      "payment_key": 9,
      "carrier_key": null
    },
    {
      "order_id": "DI20261013ba6be6-shared",
      "platform": "tiktok",
      "sku": "DI20261013ba6be6-t-shared-0",
      "quantity": 2,
      "original_price": "10000.00",
      "subtotal": "20000.00",
      "buyer_total_amount": "20000.00",
      "order_status": "COMPLETED",
      "is_cancelled": false,
      "create_time": "2026-10-13 00:01:00",
      "day": "2026-10-13",
      "product_key": 232,
      "shop_key": 22043,
      "geo_key": 7,
      "date_key": 651,
      "payment_key": 7,
      "carrier_key": 3
    }
  ],
  "completed_at_vietnam": "2026-10-13T01:30:46.761448+07:00"
}
```
