# Bài 1 — Toàn vẹn dữ liệu: ERROR

Run ID: `DI2026101327a98d`
Thời gian Việt Nam: 2026-10-13T01:30:25.892402+07:00

Kafka → consumer thật → MinIO Parquet → ETL thật → PostgreSQL. Chỉ chọn các file của lượt thử này khi chạy ETL.
18 sự kiện tổng hợp: 16 dòng hợp lệ và 2 dòng lỗi. Đây là kiểm tra chức năng toàn vẹn, chưa phải kiểm tra tải production.

| Kiểm tra | Kết quả |
|---|---|

Chi tiết bằng chứng:

```json
{
  "run_id": "DI2026101327a98d",
  "status": "ERROR",
  "timezone": "Asia/Ho_Chi_Minh",
  "scope": "18 synthetic events; real services; isolated Parquet cohort; no model accuracy test",
  "checks": [],
  "expected_lines": [
    {
      "order_id": "DI2026101327a98d-normal-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-normal-0",
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
      "order_id": "DI2026101327a98d-cancelled-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-cancelled-0",
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
      "order_id": "DI2026101327a98d-shared",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-shared-0",
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
      "order_id": "DI2026101327a98d-before_midnight-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-before_midnight-0",
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
      "order_id": "DI2026101327a98d-after_midnight-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-after_midnight-0",
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
      "order_id": "DI2026101327a98d-multi-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-multi-0",
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
      "order_id": "DI2026101327a98d-multi-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-multi-1",
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
      "order_id": "DI2026101327a98d-multi-s",
      "platform": "shopee",
      "sku": "DI2026101327a98d-s-multi-2",
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
      "order_id": "DI2026101327a98d-normal-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-normal-0",
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
      "order_id": "DI2026101327a98d-cancelled-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-cancelled-0",
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
      "order_id": "DI2026101327a98d-shared",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-shared-0",
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
      "order_id": "DI2026101327a98d-before_midnight-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-before_midnight-0",
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
      "order_id": "DI2026101327a98d-after_midnight-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-after_midnight-0",
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
      "order_id": "DI2026101327a98d-multi-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-multi-0",
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
      "order_id": "DI2026101327a98d-multi-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-multi-1",
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
      "order_id": "DI2026101327a98d-multi-t",
      "platform": "tiktok",
      "sku": "DI2026101327a98d-t-multi-2",
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
      "order_id": "DI2026101327a98d-invalid-s",
      "platform": "shopee",
      "reason": "missing SKU"
    },
    {
      "order_id": "DI2026101327a98d-invalid-t",
      "platform": "tiktok",
      "reason": "quantity=0"
    }
  ],
  "kafka_acks": [],
  "error": "TimeoutError: Consumer not ready: ['ValueError: Invalid file descriptor: -1']",
  "completed_at_vietnam": "2026-10-13T01:30:25.892402+07:00"
}
```
