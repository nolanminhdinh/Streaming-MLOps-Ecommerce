"""
Data_Simulator.py
------------------
Mô phỏng luồng phát sinh đơn hàng E-Commerce và Quản trị Tồn kho thời gian thực
cho 1 DOANH NGHIỆP BÁN LẺ ĐA KÊNH (Single Enterprise Multi-Channel Retailer):
  - Kênh bán hàng: Shopee Mall & TikTok Official Shop.
  - Kho hàng trung tâm (Central Warehouse): Quản lý tồn kho thực tế (Stock on Hand).
  - Schema tuân thủ bộ dữ liệu thực tế ``vietnam_ecommerce`` trên XomData:
      + shopee_orders (84 cột)
      + tiktok_orders (71 cột)

Hai luồng dữ liệu song hành phục vụ AI MLOps & Quản trị:
  1. Luồng Đơn đặt hàng (Sales Orders): Khách hàng đặt mua SKU trên sàn.
  2. Luồng Biến động Tồn kho (Inventory Movements & Snapshot):
     - Khấu trừ tồn kho khi phát sinh đơn xuất bán (Outbound Sale).
     - Huỷ đơn nếu kho hết hàng (Out of Stock).
     - Tự động kích hoạt đề xuất nhập hàng (Reorder Point - ROP) & nhập kho sau Lead Time.
     - Cảnh báo tồn kho 3 cấp độ (CRITICAL, WARNING, NORMAL) cho báo cáo quản trị.
     - Phục vụ nạp bảng Fact_Orders và Fact_Inventory_Daily trong Data Warehouse.

Các quy luật nghiệp vụ đã triển khai:
  1. Vòng đời đơn hàng (Order Lifecycle / State Machine):
     - Shopee:  UNPAID → READY_TO_SHIP → SHIPPED → COMPLETED  (hoặc → CANCELLED)
     - TikTok:  UNPAID → AWAITING_SHIPMENT → SHIPPED → DELIVERED → COMPLETED
                (hoặc → CANCELLED ở bất kỳ bước nào trước COMPLETED)

  2. Quy luật thời gian (Timeline Rules):
     - create_time: thời điểm tạo đơn.
     - pay_time = create_time + 0-30 phút (nếu không COD).
     - shipped_time = pay_time + 1-3 ngày.
     - completed_time = shipped_time + 2-7 ngày.
     - cancel_time: chỉ gán nếu order bị huỷ; nằm ở bất kỳ giai đoạn nào.

  3. Xu hướng & mùa vụ (Trend & Seasonality):
     - Tăng trưởng dài hạn: +2% mỗi tuần (trend_multiplier).
     - Cuối tuần (T7-CN): × 1.3 đơn.
     - Flash sale giờ vàng (12h-13h, 20h-22h): × 2.5 đơn.
     - Mega-sale ngày đôi (1/1, 2/2, …, 12/12): × 4.0 đơn.

  4. Cấu trúc phí sàn (Platform Fee Structure):
     - Shopee: commission_fee, service_fee, transaction_fee, phí vận chuyển.
     - TikTok: shipping_fee, platform_discount, seller_discount, tax.

  5. Dữ liệu Việt Nam thực tế:
     - Danh mục SKU chuẩn, giá VND, trọng lượng, địa chỉ 63 tỉnh thành.
     - Phương thức thanh toán: COD, Ví Shopee, VNPay, MoMo, ZaloPay, Thẻ tín dụng.
"""

import json
import math
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum

# Đảm bảo stdout hỗ trợ UTF-8 trên Windows
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
from typing import Optional


# ─────────────────────────────────────────────────────────────
# 1. ENUMS & CONSTANTS
# ─────────────────────────────────────────────────────────────

class Platform(str, Enum):
    SHOPEE = "shopee"
    TIKTOK = "tiktok"


class ShopeeOrderStatus(str, Enum):
    UNPAID = "UNPAID"
    READY_TO_SHIP = "READY_TO_SHIP"
    SHIPPED = "SHIPPED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    IN_CANCEL = "IN_CANCEL"
    TO_RETURN = "TO_RETURN"


class TikTokOrderStatus(str, Enum):
    UNPAID = "UNPAID"
    AWAITING_SHIPMENT = "AWAITING_SHIPMENT"
    AWAITING_COLLECTION = "AWAITING_COLLECTION"
    SHIPPED = "SHIPPED"  # IN_TRANSIT
    DELIVERED = "DELIVERED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


# ── Vòng đời hợp lệ (State Machine Transitions) ──
SHOPEE_LIFECYCLE = [
    ShopeeOrderStatus.UNPAID,
    ShopeeOrderStatus.READY_TO_SHIP,
    ShopeeOrderStatus.SHIPPED,
    ShopeeOrderStatus.COMPLETED,
]

TIKTOK_LIFECYCLE = [
    TikTokOrderStatus.UNPAID,
    TikTokOrderStatus.AWAITING_SHIPMENT,
    TikTokOrderStatus.AWAITING_COLLECTION,
    TikTokOrderStatus.SHIPPED,
    TikTokOrderStatus.DELIVERED,
    TikTokOrderStatus.COMPLETED,
]

# ── Mega-sale dates (ngày đôi) ──
MEGA_SALE_DAYS = [(m, m) for m in range(1, 13)]  # 1/1, 2/2, ..., 12/12

# ── Phương thức thanh toán ──
SHOPEE_PAYMENT_METHODS = [
    "COD", "Shopee Wallet", "VNPay", "Credit Card",
    "Debit Card", "SPayLater", "Bank Transfer",
]
TIKTOK_PAYMENT_METHODS = [
    "COD", "VNPay", "MoMo", "ZaloPay",
    "Credit Card", "Bank Transfer",
]

# ── Sản phẩm mẫu & Thiết lập Tồn kho Doanh nghiệp ──
PRODUCT_CATALOG = [
    {"name": "Ốp lưng iPhone 15 Pro Max silicon",          "sku": "OL-IP15PM",   "price": 59_000,   "weight": 0.05, "category": "Phụ kiện điện thoại", "initial_stock": 350, "safety_stock": 50, "reorder_point": 100, "reorder_qty": 250, "lead_time_days": 2, "popularity_weight": 12.0},
    {"name": "Áo thun nam cotton Premium Basic",            "sku": "ATN-CB-001",  "price": 129_000,  "weight": 0.2,  "category": "Thời trang nam",       "initial_stock": 250, "safety_stock": 40, "reorder_point": 80,  "reorder_qty": 180, "lead_time_days": 3, "popularity_weight": 6.5},
    {"name": "Sạc nhanh 65W GaN Type-C PD",                "sku": "SN-65W-GAN",  "price": 249_000,  "weight": 0.12, "category": "Phụ kiện điện tử",    "initial_stock": 280, "safety_stock": 40, "reorder_point": 85,  "reorder_qty": 200, "lead_time_days": 2, "popularity_weight": 8.5},
    {"name": "Bột giặt OMO Matic 6kg",                     "sku": "OMO-6KG",     "price": 175_000,  "weight": 6.0,  "category": "Gia dụng",             "initial_stock": 300, "safety_stock": 45, "reorder_point": 90,  "reorder_qty": 200, "lead_time_days": 3, "popularity_weight": 10.0},
    {"name": "Tai nghe Bluetooth TWS AirBuds Pro",          "sku": "TN-TWS-PRO",  "price": 450_000,  "weight": 0.08, "category": "Điện tử",             "initial_stock": 220, "safety_stock": 35, "reorder_point": 70,  "reorder_qty": 150, "lead_time_days": 3, "popularity_weight": 6.0},
    {"name": "Kem chống nắng Anessa SPF50+",               "sku": "KCN-ANESSA",  "price": 399_000,  "weight": 0.06, "category": "Mỹ phẩm",             "initial_stock": 260, "safety_stock": 45, "reorder_point": 90,  "reorder_qty": 200, "lead_time_days": 3, "popularity_weight": 9.0},
    {"name": "Bàn phím cơ Gaming RGB 104 phím",             "sku": "BP-GM-104",   "price": 690_000,  "weight": 0.95, "category": "Gaming",              "initial_stock": 160, "safety_stock": 25, "reorder_point": 50,  "reorder_qty": 120, "lead_time_days": 4, "popularity_weight": 3.0},
    {"name": "Nồi chiên không dầu 6L Air Fryer",           "sku": "AF-6L-001",   "price": 1_290_000,"weight": 4.5,  "category": "Gia dụng",             "initial_stock": 120, "safety_stock": 20, "reorder_point": 40,  "reorder_qty": 80,  "lead_time_days": 4, "popularity_weight": 2.2},
    {"name": "Sữa rửa mặt CeraVe 236ml",                  "sku": "SRM-CERAVE",  "price": 289_000,  "weight": 0.3,  "category": "Mỹ phẩm",             "initial_stock": 240, "safety_stock": 35, "reorder_point": 75,  "reorder_qty": 160, "lead_time_days": 3, "popularity_weight": 6.5},
    {"name": "Giày thể thao nam Jogger Runner",             "sku": "GTT-JR-001",  "price": 550_000,  "weight": 0.7,  "category": "Giày dép",            "initial_stock": 140, "safety_stock": 20, "reorder_point": 45,  "reorder_qty": 100, "lead_time_days": 4, "popularity_weight": 2.5},
    {"name": "Balo laptop chống sốc 15.6 inch",            "sku": "BL-15-CS",    "price": 350_000,  "weight": 0.65, "category": "Túi & Balo",          "initial_stock": 130, "safety_stock": 20, "reorder_point": 40,  "reorder_qty": 90,  "lead_time_days": 3, "popularity_weight": 2.0},
    {"name": "Nước hoa hồng Klairs 180ml",                 "sku": "NHH-KLA180",  "price": 320_000,  "weight": 0.25, "category": "Mỹ phẩm",             "initial_stock": 190, "safety_stock": 30, "reorder_point": 60,  "reorder_qty": 140, "lead_time_days": 3, "popularity_weight": 4.5},
    {"name": "Chuột gaming Logitech G304",                  "sku": "CG-G304",     "price": 690_000,  "weight": 0.1,  "category": "Gaming",              "initial_stock": 200, "safety_stock": 30, "reorder_point": 65,  "reorder_qty": 150, "lead_time_days": 3, "popularity_weight": 4.5},
    {"name": "Bộ drap giường cotton 1m8",                   "sku": "DG-CT-1M8",   "price": 399_000,  "weight": 1.2,  "category": "Gia dụng",             "initial_stock": 100, "safety_stock": 15, "reorder_point": 30,  "reorder_qty": 70,  "lead_time_days": 4, "popularity_weight": 1.5},
    {"name": "Đèn LED dây trang trí 10m",                   "sku": "DEN-LED-10",  "price": 89_000,   "weight": 0.15, "category": "Decor",               "initial_stock": 210, "safety_stock": 35, "reorder_point": 70,  "reorder_qty": 150, "lead_time_days": 3, "popularity_weight": 5.0},
    {"name": "Serum Vitamin C The Ordinary 30ml",           "sku": "SR-VC-TO30",  "price": 230_000,  "weight": 0.05, "category": "Mỹ phẩm",             "initial_stock": 220, "safety_stock": 35, "reorder_point": 70,  "reorder_qty": 150, "lead_time_days": 3, "popularity_weight": 5.5},
    {"name": "Combo 10 khẩu trang 3D KF94",                "sku": "KT-KF94-10",  "price": 45_000,   "weight": 0.08, "category": "Sức khoẻ",            "initial_stock": 500, "safety_stock": 80, "reorder_point": 160, "reorder_qty": 400, "lead_time_days": 2, "popularity_weight": 15.0},
    {"name": "Quạt mini cầm tay sạc USB",                  "sku": "QM-USB-01",   "price": 69_000,   "weight": 0.12, "category": "Gia dụng",             "initial_stock": 130, "safety_stock": 20, "reorder_point": 40,  "reorder_qty": 90,  "lead_time_days": 3, "popularity_weight": 2.0},
    {"name": "Túi xách nữ da PU thời trang",               "sku": "TX-PU-001",   "price": 259_000,  "weight": 0.35, "category": "Thời trang nữ",       "initial_stock": 90,  "safety_stock": 15, "reorder_point": 30,  "reorder_qty": 60,  "lead_time_days": 4, "popularity_weight": 1.5},
    {"name": "Miếng dán cường lực Samsung Galaxy S24",      "sku": "CL-SS-S24",   "price": 35_000,   "weight": 0.02, "category": "Phụ kiện điện thoại", "initial_stock": 300, "safety_stock": 45, "reorder_point": 90,  "reorder_qty": 220, "lead_time_days": 2, "popularity_weight": 8.0},
]

# ── Cấu hình Doanh nghiệp Giả lập & Gian hàng đa kênh (Mock Enterprise Configuration) ──
ENTERPRISE_CONFIG = {
    "enterprise_id": "MOCK-CORP-VN",
    "enterprise_name": "Mock Retail Enterprise",
    "tax_code": "0319999999",
    "central_warehouse": {
        "warehouse_id": "WH-MOCK-CENTRAL",
        "warehouse_name": "Kho Tổng Mock Logistics",
        "state": "Bình Dương",
        "city": "Thủ Dầu Một",
        "district": "Dĩ An",
    },
    "channels": {
        Platform.SHOPEE: {
            "shop_id": "10000001",
            "shop_name": "MockStore Official Mall",
            "connection_id": "CONN-SPE-MOCK",
        },
        Platform.TIKTOK: {
            "shop_id": "MockStore Official Shop",
            "shop_name": "MockStore Official Shop",
            "connection_id": "CONN-TT-MOCK",
        },
    },
}

# ── Địa chỉ giao hàng (Tỉnh → Thành phố → Quận/Huyện) ──
VIETNAM_ADDRESSES = [
    {"state": "TP.HCM",     "city": "TP.HCM",     "district": "Quận 1",        "town": "Phường Bến Nghé"},
    {"state": "TP.HCM",     "city": "TP.HCM",     "district": "Quận 7",        "town": "Phường Tân Phú"},
    {"state": "TP.HCM",     "city": "TP.HCM",     "district": "Thủ Đức",       "town": "Phường Linh Trung"},
    {"state": "TP.HCM",     "city": "TP.HCM",     "district": "Bình Thạnh",    "town": "Phường 25"},
    {"state": "TP.HCM",     "city": "TP.HCM",     "district": "Gò Vấp",        "town": "Phường 10"},
    {"state": "Hà Nội",     "city": "Hà Nội",      "district": "Cầu Giấy",      "town": "Phường Dịch Vọng"},
    {"state": "Hà Nội",     "city": "Hà Nội",      "district": "Đống Đa",       "town": "Phường Khâm Thiên"},
    {"state": "Hà Nội",     "city": "Hà Nội",      "district": "Thanh Xuân",    "town": "Phường Nhân Chính"},
    {"state": "Hà Nội",     "city": "Hà Nội",      "district": "Hoàng Mai",     "town": "Phường Hoàng Văn Thụ"},
    {"state": "Đà Nẵng",    "city": "Đà Nẵng",     "district": "Hải Châu",      "town": "Phường Thạch Thang"},
    {"state": "Đà Nẵng",    "city": "Đà Nẵng",     "district": "Thanh Khê",     "town": "Phường Xuân Hà"},
    {"state": "Bình Dương",  "city": "Thủ Dầu Một", "district": "Dĩ An",         "town": "Phường Đông Hoà"},
    {"state": "Đồng Nai",   "city": "Biên Hoà",    "district": "Long Bình",     "town": "Phường An Bình"},
    {"state": "Cần Thơ",    "city": "Cần Thơ",     "district": "Ninh Kiều",     "town": "Phường Xuân Khánh"},
    {"state": "Hải Phòng",  "city": "Hải Phòng",   "district": "Hồng Bàng",     "town": "Phường Quán Toan"},
    {"state": "Khánh Hoà",  "city": "Nha Trang",   "district": "Lộc Thọ",       "town": "Phường Tân Lập"},
    {"state": "Nghệ An",    "city": "Vinh",         "district": "TP. Vinh",      "town": "Phường Cửa Nam"},
    {"state": "Thanh Hoá",  "city": "Thanh Hoá",    "district": "TP Thanh Hoá",  "town": "Phường Đông Vệ"},
    {"state": "Lâm Đồng",  "city": "Đà Lạt",      "district": "TP Đà Lạt",     "town": "Phường 1"},
    {"state": "Long An",    "city": "Tân An",       "district": "TP Tân An",     "town": "Phường 1"},
]

# ── Đơn vị vận chuyển ──
SHOPEE_CARRIERS = [
    "SPX Express", "Giao Hàng Tiết Kiệm", "Giao Hàng Nhanh",
    "J&T Express", "Viettel Post", "BEST Express",
]
TIKTOK_CARRIERS = [
    "TikTok Logistics", "J&T Express", "Giao Hàng Nhanh",
    "BEST Express", "Viettel Post",
]

# ── Tên shop mẫu (Bảo toàn tương thích seed_dim_tables) ──
SHOP_NAMES = [
    "MockStore Official Mall", "MockStore Official Shop", "BeautyHouseHCM", "TechZone Official",
    "FashionKorea_VN", "HomeDecorHN", "SportsGear365",
    "GadgetWorldVN", "CosmeBoutique", "SneakerHub_SG",
    "FreshMartOnline",
]

# ── Lý do huỷ đơn & Hoàn hàng ──
SHOPEE_CANCEL_REASONS = [
    "Khách hàng yêu cầu huỷ", "Hết hàng", "Không liên lạc được khách (COD giao không thành công)",
    "Đổi ý không mua", "Trùng đơn", "Sản phẩm lỗi trước khi gửi",
    "Buyer requested to cancel", "Out of stock",
]
TIKTOK_CANCEL_REASONS = [
    "Buyer Cancel", "Seller Cancel - Out of Stock",
    "System Cancel - Payment Timeout", "Buyer changed mind",
    "Logistics delivery failed (COD)", "Wrong item",
]


# ─────────────────────────────────────────────────────────────
# 2. DATA CLASSES — INVENTORY & ORDER EVENTS
# ─────────────────────────────────────────────────────────────

@dataclass
class InventoryItem:
    """Theo dõi trạng thái tồn kho thực tế của từng SKU trong kho Doanh nghiệp."""
    sku: str
    product_name: str
    category: str
    price: float
    weight: float
    stock_on_hand: int
    safety_stock: int
    reorder_point: int
    reorder_qty: int
    lead_time_days: int
    incoming_stock: int = 0
    reserved_stock: int = 0
    pending_return_stock: int = 0
    restock_eta: Optional[str] = None
    total_sold: int = 0
    total_restocked: int = 0
    total_returned: int = 0
    out_of_stock_count: int = 0
    last_updated: Optional[str] = None


@dataclass
class InventoryMovementEvent:
    """Ghi nhận sự kiện biến động xuất nhập tồn kho (Inventory Transaction Audit)."""
    movement_id: str
    sku: str
    warehouse_id: str
    movement_type: str  # "OUTBOUND_SALE" | "INBOUND_RESTOCK" | "OUT_OF_STOCK_REJECT" | "RETURN_RESTOCK" | "CANCEL_RELEASE"
    quantity: int
    stock_before: int
    stock_after: int
    reference_order_id: Optional[str] = None
    timestamp: Optional[str] = None

@dataclass
class ShopeeOrderEvent:
    """Sự kiện đơn hàng Shopee — tuân theo schema shopee_orders (84 cột).
    Chọn lọc các cột quan trọng nhất cho mục tiêu demand forecasting."""

    # ─── Định danh ───
    pkId: str
    user_id: str
    shop_id: str
    order_sn: str
    shop_name: str
    connection_id: str

    # ─── Trạng thái & vòng đời ───
    order_status: str
    order_status_raw: str
    create_time: str
    pay_time: Optional[str]
    shipped_time: Optional[str]
    completed_time: Optional[str]
    cancel_time: Optional[str]
    cancel_reason: Optional[str]
    cancel_by: Optional[str]
    fulfillment_flag: str

    # ─── Sản phẩm & SKU ───
    item_name: str
    item_sku: str
    model_name: Optional[str]
    model_sku: str
    item_id: str
    model_id: str
    quantity: int
    original_price: float
    model_discounted_price: Optional[float]
    item_weight: float

    # ─── Tài chính (VND) ───
    total_order_value: float
    buyer_total_amount: float
    seller_discount: float
    shopee_discount: float
    voucher_from_seller: float
    voucher_from_shopee: float
    commission_fee: float
    service_fee: float
    transaction_fee: float
    escrow_amount: float
    actual_shipping_fee: float
    estimated_shipping_fee: float

    # ─── Thanh toán ───
    payment_method: str
    cod: Optional[str]
    currency: str

    # ─── Người mua & Giao hàng ───
    buyer_user_id: str
    buyer_username: Optional[str]
    recipient_name: str
    phone: str
    full_address: str
    state: str
    city: str
    district: str
    country: str
    region: Optional[str]

    # ─── Vận chuyển ───
    shipping_carrier: Optional[str]
    shipping_method: Optional[str]
    ship_by_date: Optional[str]
    package_number: Optional[str]

    # ─── Metadata ───
    synced_at: str
    update_time: Optional[str]
    is_primary_row: str

    # ─── Trường phụ (khuyến mãi, …) ───
    promotion_type: Optional[str] = None
    promotion_id: Optional[str] = None

    # ─── Streaming metadata ───
    _platform: str = field(default="shopee", repr=False)


@dataclass
class TikTokOrderEvent:
    """Sự kiện đơn hàng TikTok Shop — tuân theo schema tiktok_orders (71 cột).
    Chọn lọc các cột quan trọng nhất cho mục tiêu demand forecasting."""

    # ─── Định danh ───
    pkId: str
    user_id: str
    order_id: str
    order_type: str
    shop_name: str

    # ─── Trạng thái & vòng đời ───
    order_status: str
    created_time: str
    paid_time: Optional[str]
    rts_time: Optional[str]          # Ready to Ship
    shipped_time: Optional[str]
    delivered_time: Optional[str]
    completed_time: Optional[str]
    cancelled_time: Optional[str]
    updated_time: str

    # ─── SLA timestamps ───
    rts_sla_time: Optional[str]
    tts_sla_time: Optional[str]      # Time to Ship SLA
    delivery_sla_time: Optional[str]
    cancel_sla_time: Optional[str]

    # ─── Tài chính (VND) ───
    currency: str
    total_amount: float
    sub_total: float
    order_original_price: float
    shipping_fee: float
    original_shipping_fee: float
    tax_amount: float
    product_tax: float
    shipping_tax: float
    small_order_fee: float
    retail_delivery_fee: float
    insurance_fee: float
    seller_discount: float
    platform_discount: float
    shipping_seller_discount: float
    shipping_platform_discount: float

    # ─── Thanh toán ───
    payment_method: Optional[str]
    is_cod: Optional[str]

    # ─── Vận chuyển & Kho ───
    fulfillment_type: Optional[str]
    delivery_type: Optional[str]
    delivery_option: Optional[str]
    shipping_provider: Optional[str]
    tracking_number: Optional[str]
    warehouse_id: Optional[str]

    # ─── Người mua & Giao hàng ───
    buyer_uid: Optional[str]
    buyer_name: Optional[str]
    buyer_message: Optional[str]
    recipient_name: Optional[str]
    recipient_phone: Optional[str]
    full_address: Optional[str]
    postal_code: Optional[str]
    region_state: Optional[str]
    city_town: Optional[str]
    district: Optional[str]

    # ─── Sản phẩm & SKU ───
    product_name: str
    sku_name: str
    seller_sku: Optional[str]
    quantity: int
    item_status: str
    item_sale_price: float
    item_original_price: float
    item_platform_disc: float
    item_seller_disc: float
    product_id: str
    sku_id: str

    # ─── Package ───
    package_id: Optional[str]
    package_status: Optional[str]
    item_cancel_reason: Optional[str]

    # ─── Metadata ───
    createdAt: str
    updatedAt: str
    row_idx: int

    # ─── Streaming metadata ───
    _platform: str = field(default="tiktok", repr=False)


# ─────────────────────────────────────────────────────────────
# 3. CORE SIMULATOR
# ─────────────────────────────────────────────────────────────

class ECommerceSimulator:
    """Sinh sự kiện đơn hàng giả lập và quản trị tồn kho thời gian thực cho 1 Doanh nghiệp:
    - Mô hình 1 Doanh nghiệp bán đa kênh (Shopee Mall & TikTok Official Shop).
    - Quản lý trạng thái tồn kho (Current Stock / Stock on Hand) theo từng SKU.
    - Khấu trừ tồn kho khi xuất bán; từ chối/hủy đơn khi hết hàng (Out of Stock).
    - Cơ chế đề xuất nhập hàng (Reorder Point - ROP) và nhập kho (Restock) sau Lead Time.
    - Cảnh báo tồn kho 3 cấp độ: CRITICAL, WARNING, NORMAL.
    - Vòng đời đơn hàng, phí sàn và xu hướng mùa vụ Việt Nam.
    """

    def __init__(
        self,
        shopee_ratio: float = 0.55,     # 55% đơn Shopee, 45% TikTok
        cancel_rate: float = 0.12,       # 12% tỉ lệ huỷ đơn thông thường
        trend_weekly_pct: float = 0.02,  # +2% mỗi tuần
        start_date: Optional[datetime] = None,
        enterprise_config: Optional[dict] = None,
        initial_stock_override: Optional[dict] = None,
    ):
        self.shopee_ratio = shopee_ratio
        self.cancel_rate = cancel_rate
        self.trend_weekly_pct = trend_weekly_pct
        self.start_date = start_date or datetime.now(timezone.utc)
        self.enterprise = enterprise_config or ENTERPRISE_CONFIG
        self._order_counter = 0
        self._row_idx = 0
        self._movement_counter = 0

        # Khởi tạo trạng thái tồn kho nội bộ cho Doanh nghiệp
        self.inventory: dict[str, InventoryItem] = {}
        self.movement_history: list[InventoryMovementEvent] = []
        self.pending_returns: list[dict] = []
        self._init_inventory(initial_stock_override)

    # ──────────── Inventory Engine ────────────

    def _init_inventory(self, stock_override: Optional[dict] = None):
        """Khởi tạo kho hàng ban đầu cho toàn bộ danh mục sản phẩm của Doanh nghiệp."""
        override = stock_override or {}
        for p in PRODUCT_CATALOG:
            sku = p["sku"]
            initial_stock = override.get(sku, p.get("initial_stock", 200))
            self.inventory[sku] = InventoryItem(
                sku=sku,
                product_name=p["name"],
                category=p.get("category", "Chung"),
                price=float(p["price"]),
                weight=float(p.get("weight", 0.1)),
                stock_on_hand=initial_stock,
                safety_stock=p.get("safety_stock", 30),
                reorder_point=p.get("reorder_point", 60),
                reorder_qty=p.get("reorder_qty", 150),
                lead_time_days=p.get("lead_time_days", 3),
                incoming_stock=0,
                reserved_stock=0,
                pending_return_stock=0,
                restock_eta=None,
                total_sold=0,
                total_restocked=0,
                total_returned=0,
                out_of_stock_count=0,
                last_updated=self._fmt(self.start_date),
            )

    def _process_incoming_restocks(self, now: datetime):
        """Kiểm tra và nhập kho các lô hàng đã đến thời điểm giao (sau Lead Time)."""
        dt_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        for sku, inv in self.inventory.items():
            if inv.incoming_stock > 0 and inv.restock_eta is not None:
                try:
                    eta_dt = datetime.fromisoformat(inv.restock_eta)
                    if eta_dt.tzinfo is None:
                        eta_dt = eta_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue

                if dt_now >= eta_dt:
                    stock_before = inv.stock_on_hand
                    inv.stock_on_hand += inv.incoming_stock
                    inv.total_restocked += inv.incoming_stock
                    stock_after = inv.stock_on_hand
                    restocked_qty = inv.incoming_stock

                    self._movement_counter += 1
                    mov = InventoryMovementEvent(
                        movement_id=f"MOV-IN-{self._movement_counter:06d}",
                        sku=sku,
                        warehouse_id=self.enterprise["central_warehouse"]["warehouse_id"],
                        movement_type="INBOUND_RESTOCK",
                        quantity=restocked_qty,
                        stock_before=stock_before,
                        stock_after=stock_after,
                        reference_order_id=None,
                        timestamp=self._fmt(dt_now),
                    )
                    self.movement_history.append(mov)

                    inv.incoming_stock = 0
                    inv.restock_eta = None
                    inv.last_updated = self._fmt(dt_now)

    def _process_pending_returns(self, now: datetime):
        """Kiểm tra và tái nhập kho (Restock Return) cho các kiện hàng hoàn về đến kho tổng (sau 3-5 ngày)."""
        dt_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        still_pending = []
        for ret in self.pending_returns:
            eta = ret["return_eta"]
            dt_eta = eta if eta.tzinfo is not None else eta.replace(tzinfo=timezone.utc)
            if dt_now >= dt_eta:
                sku = ret["sku"]
                qty = ret["quantity"]
                if sku in self.inventory:
                    inv = self.inventory[sku]
                    stock_before = inv.stock_on_hand
                    inv.stock_on_hand += qty
                    inv.total_returned += qty
                    inv.pending_return_stock = max(0, inv.pending_return_stock - qty)
                    inv.last_updated = self._fmt(dt_now)

                    self._movement_counter += 1
                    mov = InventoryMovementEvent(
                        movement_id=f"MOV-RET-{self._movement_counter:06d}",
                        sku=sku,
                        warehouse_id=self.enterprise["central_warehouse"]["warehouse_id"],
                        movement_type="RETURN_RESTOCK",
                        quantity=qty,
                        stock_before=stock_before,
                        stock_after=inv.stock_on_hand,
                        reference_order_id=ret["order_id"],
                        timestamp=self._fmt(dt_now),
                    )
                    self.movement_history.append(mov)
            else:
                still_pending.append(ret)
        self.pending_returns = still_pending

    def _queue_return(self, sku: str, qty: int, order_id: str, cancel_time: Optional[datetime] = None):
        """Đưa sản phẩm vào luồng logistics hoàn hàng (Reverse Logistics) về kho tổng sau 3-5 ngày."""
        base_time = cancel_time or datetime.now(timezone.utc)
        dt_cancel = base_time if base_time.tzinfo is not None else base_time.replace(tzinfo=timezone.utc)
        return_lead_days = random.randint(3, 5)
        eta = dt_cancel + timedelta(days=return_lead_days)
        self.pending_returns.append({
            "sku": sku,
            "quantity": qty,
            "order_id": order_id,
            "return_eta": eta,
            "enqueued_at": dt_cancel,
        })
        if sku in self.inventory:
            self.inventory[sku].pending_return_stock += qty

    def _release_cancelled_stock(self, sku: str, qty: int, order_id: str, now: datetime):
        """Hoàn lại tồn kho tức thì khi đơn hàng bị huỷ trước khi xuất kho giao hàng."""
        if sku not in self.inventory:
            return
        inv = self.inventory[sku]
        dt_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        stock_before = inv.stock_on_hand
        inv.stock_on_hand += qty
        inv.total_sold = max(0, inv.total_sold - qty)
        inv.last_updated = self._fmt(dt_now)

        self._movement_counter += 1
        mov = InventoryMovementEvent(
            movement_id=f"MOV-CAN-{self._movement_counter:06d}",
            sku=sku,
            warehouse_id=self.enterprise["central_warehouse"]["warehouse_id"],
            movement_type="CANCEL_RELEASE",
            quantity=qty,
            stock_before=stock_before,
            stock_after=inv.stock_on_hand,
            reference_order_id=order_id,
            timestamp=self._fmt(dt_now),
        )
        self.movement_history.append(mov)

    def _deduct_inventory(self, sku: str, qty: int, order_id: str, now: datetime) -> tuple[bool, int]:
        """
        Khấu trừ tồn kho khi phát sinh đơn hàng:
          - Trả về (True, stock_left) nếu đủ hàng để xuất bán.
          - Trả về (False, stock_left) nếu không đủ hàng (Out of Stock).
          - Tự động kích hoạt đề xuất nhập hàng (Reorder) nếu tồn kho giảm <= Reorder Point.
        """
        self._process_incoming_restocks(now)
        self._process_pending_returns(now)
        dt_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)

        if sku not in self.inventory:
            return True, 999  # Fallback nếu SKU không tồn tại trong kho

        inv = self.inventory[sku]

        # Kiểm tra đủ tồn kho hay không
        if inv.stock_on_hand < qty:
            inv.out_of_stock_count += 1
            inv.last_updated = self._fmt(dt_now)

            self._movement_counter += 1
            mov = InventoryMovementEvent(
                movement_id=f"MOV-REJ-{self._movement_counter:06d}",
                sku=sku,
                warehouse_id=self.enterprise["central_warehouse"]["warehouse_id"],
                movement_type="OUT_OF_STOCK_REJECT",
                quantity=qty,
                stock_before=inv.stock_on_hand,
                stock_after=inv.stock_on_hand,
                reference_order_id=order_id,
                timestamp=self._fmt(dt_now),
            )
            self.movement_history.append(mov)

            # Kích hoạt nhập hàng bổ sung khẩn cấp nếu chưa có lệnh nhập đang trên đường về
            if inv.incoming_stock == 0:
                inv.incoming_stock = inv.reorder_qty
                inv.restock_eta = self._fmt(dt_now + timedelta(days=inv.lead_time_days))

            return False, inv.stock_on_hand

        # Đủ hàng xuất bán
        stock_before = inv.stock_on_hand
        inv.stock_on_hand -= qty
        inv.total_sold += qty
        stock_after = inv.stock_on_hand
        inv.last_updated = self._fmt(dt_now)

        self._movement_counter += 1
        mov = InventoryMovementEvent(
            movement_id=f"MOV-OUT-{self._movement_counter:06d}",
            sku=sku,
            warehouse_id=self.enterprise["central_warehouse"]["warehouse_id"],
            movement_type="OUTBOUND_SALE",
            quantity=qty,
            stock_before=stock_before,
            stock_after=stock_after,
            reference_order_id=order_id,
            timestamp=self._fmt(dt_now),
        )
        self.movement_history.append(mov)

        # Kiểm tra ngưỡng đặt hàng lại (Reorder Point)
        if inv.stock_on_hand <= inv.reorder_point and inv.incoming_stock == 0:
            inv.incoming_stock = inv.reorder_qty
            inv.restock_eta = self._fmt(dt_now + timedelta(days=inv.lead_time_days))

        return True, inv.stock_on_hand

    # ──────────── Seasonality & Trend ────────────

    def _trend_multiplier(self, now: datetime) -> float:
        """Xu hướng tăng trưởng dài hạn: +trend_weekly_pct mỗi tuần kể từ start_date."""
        dt_now = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
        dt_start = self.start_date if self.start_date.tzinfo is not None else self.start_date.replace(tzinfo=timezone.utc)
        weeks_elapsed = (dt_now - dt_start).days / 7.0
        return 1.0 + self.trend_weekly_pct * weeks_elapsed

    def _is_flash_sale(self, now: datetime) -> bool:
        """Flash sale vào khung giờ vàng: 12h-13h (lunch) và 20h-22h (tối)."""
        return now.hour in (12, 20, 21)

    def _is_mega_sale(self, now: datetime) -> bool:
        """Mega-sale ngày đôi: 1/1, 2/2, …, 12/12."""
        return (now.month, now.day) in MEGA_SALE_DAYS

    def _is_payday(self, now: datetime) -> bool:
        """Ngày lĩnh lương định kỳ tại VN: ngày 15 và các ngày 25-28 hàng tháng."""
        return now.day in (15, 25, 26, 27, 28)

    def _payday_multiplier(self, now: datetime) -> float:
        """Hệ số tăng trưởng đơn hàng vào ngày lĩnh lương (Payday Sale)."""
        if now.day == 15:
            return 1.8  # Đợt lương giữa tháng
        elif now.day in (25, 26, 27, 28):
            return 1.6  # Đợt lương cuối tháng
        return 1.0

    def _seasonal_multiplier(self, now: datetime) -> float:
        """Hệ số tổng hợp: tuần × flash × mega × payday × ngày trong tuần × giờ trong ngày."""
        # Hiệu ứng ngày trong tuần: T2-T6=1.0, T7=1.3, CN=1.2
        dow = now.weekday()
        dow_factor = {5: 1.3, 6: 1.2}.get(dow, 1.0)

        # Hiệu ứng giờ trong ngày (bell curve hành vi mua sắm thương mại điện tử thực tế)
        hour = now.hour
        hour_factor = 0.25 + 0.75 * (
            0.8 * math.exp(-0.5 * ((hour - 10.5) / 2.5) ** 2) +
            1.2 * math.exp(-0.5 * ((hour - 20.5) / 2.0) ** 2)
        )

        flash = 2.5 if self._is_flash_sale(now) else 1.0
        mega = 4.0 if self._is_mega_sale(now) else 1.0
        payday = self._payday_multiplier(now)

        return dow_factor * hour_factor * flash * mega * payday * self._trend_multiplier(now)

    def orders_per_second(self, now: datetime, base_rate: float = 3.0) -> int:
        """Số đơn/giây thực tế sau khi áp dụng tất cả hệ số mùa vụ."""
        raw = base_rate * self._seasonal_multiplier(now)
        return max(1, int(round(raw)))

    # ──────────── Helpers ────────────

    @staticmethod
    def _gen_phone() -> str:
        prefixes = ["032", "033", "034", "035", "036", "037", "038", "039",
                     "056", "058", "070", "076", "077", "078", "079",
                     "081", "082", "083", "084", "085", "086", "088", "089"]
        return random.choice(prefixes) + "".join([str(random.randint(0, 9)) for _ in range(7)])

    @staticmethod
    def _gen_buyer_username() -> str:
        adjectives = ["happy", "cool", "lucky", "sweet", "smart", "fast"]
        nouns = ["buyer", "shopper", "user", "fan", "star", "cat", "panda"]
        return f"{random.choice(adjectives)}_{random.choice(nouns)}_{random.randint(100,9999)}"

    @staticmethod
    def _gen_recipient_name() -> str:
        ho = random.choice(["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh",
                             "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ"])
        ten_dem = random.choice(["Văn", "Thị", "Hoàng", "Minh", "Thanh", "Đức", "Quốc"])
        ten = random.choice(["An", "Bình", "Châu", "Dung", "Em", "Giang",
                              "Hà", "Khoa", "Linh", "Mai", "Nam", "Phúc",
                              "Quân", "Sơn", "Tâm", "Uyên", "Vy", "Xuân"])
        return f"{ho} {ten_dem} {ten}"

    def _pick_product(self) -> dict:
        """Chọn SKU theo trọng số phổ biến (Pareto 80/20 sales distribution)."""
        weights = [p.get("popularity_weight", 1.0) for p in PRODUCT_CATALOG]
        return random.choices(PRODUCT_CATALOG, weights=weights)[0]

    def _pick_address(self) -> dict:
        return random.choice(VIETNAM_ADDRESSES)

    def _pick_shop(self, platform: Platform = Platform.SHOPEE) -> tuple[str, str, str]:
        """Trả về (shop_name, shop_id, connection_id) của gian hàng chính hãng Doanh nghiệp."""
        channels = self.enterprise.get("channels", {})
        channel = channels.get(platform) or channels.get(Platform.SHOPEE, {})
        default_name = "MockStore Official Mall" if platform == Platform.SHOPEE else "MockStore Official Shop"
        default_conn = "CONN-SPE-MOCK" if platform == Platform.SHOPEE else "CONN-TT-MOCK"
        return (
            channel.get("shop_name", default_name),
            channel.get("shop_id", "10000001"),
            channel.get("connection_id", default_conn),
        )

    # ──────────── Order Lifecycle Logic ────────────

    def _resolve_shopee_status(self, is_cancelled: bool) -> tuple[str, int]:
        """Xác định trạng thái cuối và vị trí trong lifecycle.
        Trả về (final_status, stage_index)."""
        if is_cancelled:
            # Huỷ có thể xảy ra ở bất kỳ giai đoạn nào trước COMPLETED
            cancel_at = random.randint(0, 2)  # 0=UNPAID, 1=READY_TO_SHIP, 2=SHIPPED
            return ShopeeOrderStatus.CANCELLED.value, cancel_at
        # Đơn hoàn thành — chọn random đang ở stage nào (mô phỏng real-time)
        stage = random.choices(range(4), weights=[5, 15, 30, 50])[0]
        return SHOPEE_LIFECYCLE[stage].value, stage

    def _resolve_tiktok_status(self, is_cancelled: bool) -> tuple[str, int]:
        if is_cancelled:
            cancel_at = random.randint(0, 3)
            return TikTokOrderStatus.CANCELLED.value, cancel_at
        stage = random.choices(range(6), weights=[3, 8, 5, 15, 19, 50])[0]
        return TIKTOK_LIFECYCLE[stage].value, stage

    def _generate_timestamps(self, create_time: datetime, stage: int, is_cancelled: bool):
        """Sinh chuỗi timestamp theo vòng đời đơn hàng.
        Quy tắc:
          - pay_time   = create + 0–30 phút
          - ship_time  = pay    + 1–3 ngày
          - deliver    = ship   + 2–5 ngày
          - complete   = deliver + 0–2 ngày
          - cancel     = ở stage bị huỷ + 0–2h
        """
        ts = {"pay": None, "ship": None, "deliver": None, "complete": None, "cancel": None}

        if stage >= 1:  # Đã thanh toán
            ts["pay"] = create_time + timedelta(minutes=random.randint(0, 30))
        if stage >= 2:  # Đã lấy hàng / ready
            ts["ship"] = ts["pay"] + timedelta(
                days=random.randint(1, 3), hours=random.randint(0, 12)
            )
        if stage >= 3:  # Đang vận chuyển / đã giao
            ts["deliver"] = ts["ship"] + timedelta(
                days=random.randint(2, 5), hours=random.randint(0, 23)
            )
        if stage >= 4:  # Hoàn thành (TikTok có thêm bước DELIVERED)
            ts["complete"] = ts["deliver"] + timedelta(
                days=random.randint(0, 2), hours=random.randint(0, 12)
            )
        if stage >= 5:  # TikTok full lifecycle
            ts["complete"] = ts["deliver"] + timedelta(days=random.randint(1, 3))

        if is_cancelled:
            # Thời gian huỷ: tại stage hiện tại + 0-2 giờ
            base = ts["pay"] or create_time
            if stage >= 2 and ts["ship"]:
                base = ts["ship"]
            ts["cancel"] = base + timedelta(hours=random.randint(0, 2))

        return ts

    @staticmethod
    def _fmt(dt: Optional[datetime]) -> Optional[str]:
        return dt.isoformat() if dt else None

    # ──────────── FEE CALCULATION ────────────

    @staticmethod
    def _calc_shopee_fees(original_price: float, quantity: int):
        """Tính phí sàn Shopee theo quy tắc thực tế VN (ước lượng)."""
        subtotal = original_price * quantity

        # Giảm giá từ seller: 0-15% subtotal
        seller_disc_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15])
        seller_discount = round(subtotal * seller_disc_pct)

        # Giảm giá từ Shopee: 0-10% subtotal (thường ít hơn seller)
        shopee_disc_pct = random.choice([0, 0, 0, 0, 0.03, 0.05, 0.10])
        shopee_discount = round(subtotal * shopee_disc_pct)

        # Voucher
        voucher_seller = random.choice([0, 0, 0, 10_000, 20_000, 30_000])
        voucher_shopee = random.choice([0, 0, 0, 0, 15_000, 25_000])

        buyer_amount = max(0, subtotal - seller_discount - shopee_discount
                          - voucher_seller - voucher_shopee)

        # Phí sàn
        commission_fee = round(subtotal * random.uniform(0.02, 0.06))  # 2-6%
        service_fee = round(subtotal * random.uniform(0.02, 0.04))     # 2-4%
        transaction_fee = round(subtotal * random.uniform(0.01, 0.02)) # 1-2%

        # Phí vận chuyển
        actual_shipping = random.choice([0, 15_000, 18_000, 25_000, 30_000, 38_000])
        estimated_shipping = actual_shipping + random.randint(-5000, 5000)
        estimated_shipping = max(0, estimated_shipping)

        escrow = round(buyer_amount - commission_fee - service_fee
                       - transaction_fee + actual_shipping * 0.5)
        escrow = max(0, escrow)

        return {
            "seller_discount": seller_discount,
            "shopee_discount": shopee_discount,
            "voucher_from_seller": voucher_seller,
            "voucher_from_shopee": voucher_shopee,
            "buyer_total_amount": buyer_amount,
            "commission_fee": commission_fee,
            "service_fee": service_fee,
            "transaction_fee": transaction_fee,
            "actual_shipping_fee": actual_shipping,
            "estimated_shipping_fee": estimated_shipping,
            "escrow_amount": escrow,
            "total_order_value": subtotal,
        }

    @staticmethod
    def _calc_tiktok_fees(item_price: float, quantity: int):
        """Tính phí sàn TikTok Shop theo quy tắc ước lượng VN."""
        subtotal = item_price * quantity

        seller_disc_pct = random.choice([0, 0, 0.05, 0.08, 0.10, 0.15])
        seller_discount = round(subtotal * seller_disc_pct)

        platform_disc_pct = random.choice([0, 0, 0, 0.03, 0.05, 0.08])
        platform_discount = round(subtotal * platform_disc_pct)

        sale_price = max(0, item_price * (1 - seller_disc_pct - platform_disc_pct))

        # Shipping
        original_shipping = random.choice([15_000, 20_000, 25_000, 30_000, 35_000])
        ship_seller_disc = random.choice([0, 0, original_shipping, original_shipping // 2])
        ship_platform_disc = random.choice([0, 0, 0, min(15_000, original_shipping)])
        shipping_fee = max(0, original_shipping - ship_seller_disc - ship_platform_disc)

        # Tax (VAT 10%)
        product_tax = round(subtotal * 0.10)
        shipping_tax = round(shipping_fee * 0.10)
        tax_amount = product_tax + shipping_tax

        # Other fees
        small_order_fee = 5_000 if subtotal < 50_000 else 0
        insurance_fee = random.choice([0, 0, 0, 1_000, 2_000])
        retail_delivery_fee = 0

        total = subtotal - seller_discount - platform_discount + shipping_fee + tax_amount + small_order_fee

        return {
            "total_amount": round(total),
            "sub_total": subtotal,
            "order_original_price": subtotal,
            "item_sale_price": round(sale_price),
            "item_platform_disc": platform_discount,
            "item_seller_disc": seller_discount,
            "seller_discount": seller_discount,
            "platform_discount": platform_discount,
            "shipping_fee": shipping_fee,
            "original_shipping_fee": original_shipping,
            "shipping_seller_discount": ship_seller_disc,
            "shipping_platform_discount": ship_platform_disc,
            "tax_amount": tax_amount,
            "product_tax": product_tax,
            "shipping_tax": shipping_tax,
            "small_order_fee": small_order_fee,
            "retail_delivery_fee": retail_delivery_fee,
            "insurance_fee": insurance_fee,
        }

    # ──────────── EVENT GENERATORS ────────────

    def _generate_shopee_event(self, now: datetime) -> ShopeeOrderEvent:
        self._order_counter += 1
        product = self._pick_product()
        addr = self._pick_address()
        shop_name, shop_id, conn_id = self._pick_shop(Platform.SHOPEE)

        order_sn = f"SPE{now.strftime('%y%m%d')}{self._order_counter:06d}"

        qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]
        # Flash sale → tăng quantity
        if self._is_flash_sale(now):
            qty = min(10, qty + random.randint(1, 3))

        # Khấu trừ kho Doanh nghiệp
        in_stock, stock_left = self._deduct_inventory(product["sku"], qty, order_sn, now)

        if not in_stock:
            # Hết hàng -> bắt buộc hủy đơn
            is_cancelled = True
            status = ShopeeOrderStatus.CANCELLED.value
            stage = 0
            cancel_reason = "Hết hàng"
            cancel_by = "seller"
        else:
            is_cancelled = random.random() < self.cancel_rate
            status, stage = self._resolve_shopee_status(is_cancelled)
            cancel_reason = random.choice(SHOPEE_CANCEL_REASONS) if is_cancelled else None
            cancel_by = random.choice(["buyer", "seller", "system"]) if is_cancelled else None

            if is_cancelled:
                if stage < 2:
                    self._release_cancelled_stock(product["sku"], qty, order_sn, now)
                else:
                    self._queue_return(product["sku"], qty, order_sn, now)

        ts = self._generate_timestamps(now, stage, is_cancelled)

        # Discounted price
        disc_price = round(product["price"] * random.uniform(0.80, 1.0))

        fees = self._calc_shopee_fees(product["price"], qty)

        payment = random.choice(SHOPEE_PAYMENT_METHODS)
        is_cod = "True" if payment == "COD" else "False"

        # Dùng timezone-aware datetime cho synced_at / update_time
        _utcnow = datetime.now(timezone.utc)
        pk_id = f"SPE-{shop_id}-{order_sn}"
        user_id = f"USR-{random.randint(100000, 999999)}"
        buyer_uid = f"BUY-{random.randint(1000000, 9999999)}"

        return ShopeeOrderEvent(
            pkId=pk_id,
            user_id=user_id,
            shop_id=shop_id,
            order_sn=order_sn,
            shop_name=shop_name,
            connection_id=conn_id,
            order_status=status,
            order_status_raw=status,
            create_time=now.isoformat(),
            pay_time=self._fmt(ts["pay"]),
            shipped_time=self._fmt(ts["ship"]),
            completed_time=self._fmt(ts["complete"]),
            cancel_time=self._fmt(ts["cancel"]),
            cancel_reason=cancel_reason,
            cancel_by=cancel_by,
            fulfillment_flag="fulfilled_by_seller",  # Kho Doanh nghiệp tự vận hành
            item_name=product["name"],
            item_sku=product["sku"],
            model_name=product.get("category"),
            model_sku=f"{product['sku']}-V{random.randint(1,5)}",
            item_id=f"ITEM-{random.randint(10000000, 99999999)}",
            model_id=f"MODEL-{random.randint(10000000, 99999999)}",
            quantity=qty,
            original_price=product["price"],
            model_discounted_price=disc_price,
            item_weight=product["weight"],
            total_order_value=fees["total_order_value"],
            buyer_total_amount=fees["buyer_total_amount"],
            seller_discount=fees["seller_discount"],
            shopee_discount=fees["shopee_discount"],
            voucher_from_seller=fees["voucher_from_seller"],
            voucher_from_shopee=fees["voucher_from_shopee"],
            commission_fee=fees["commission_fee"],
            service_fee=fees["service_fee"],
            transaction_fee=fees["transaction_fee"],
            escrow_amount=fees["escrow_amount"],
            actual_shipping_fee=fees["actual_shipping_fee"],
            estimated_shipping_fee=fees["estimated_shipping_fee"],
            payment_method=payment,
            cod=is_cod,
            currency="VND",
            buyer_user_id=buyer_uid,
            buyer_username=self._gen_buyer_username(),
            recipient_name=self._gen_recipient_name(),
            phone=self._gen_phone(),
            full_address="***",  # masked for privacy
            state=addr["state"],
            city=addr["city"],
            district=addr["district"],
            country="VN",
            region="VN",
            shipping_carrier=random.choice(SHOPEE_CARRIERS) if stage >= 2 else None,
            shipping_method="Standard" if random.random() < 0.7 else "Express",
            ship_by_date=self._fmt(now + timedelta(days=random.randint(2, 5))) if stage >= 1 else None,
            package_number=f"PKG{random.randint(100000000, 999999999)}" if stage >= 2 else None,
            synced_at=_utcnow.isoformat(),
            update_time=self._fmt(ts.get("deliver") or ts.get("ship") or ts.get("pay") or now),
            is_primary_row="True",
            promotion_type="flash_sale" if self._is_flash_sale(now) else (
                "mega_sale" if self._is_mega_sale(now) else None
            ),
            promotion_id=f"PROMO-{now.strftime('%m%d')}" if self._is_flash_sale(now) or self._is_mega_sale(now) else None,
        )

    def _generate_tiktok_event(self, now: datetime) -> TikTokOrderEvent:
        self._order_counter += 1
        self._row_idx += 1
        product = self._pick_product()
        addr = self._pick_address()
        shop_name, shop_id, conn_id = self._pick_shop(Platform.TIKTOK)
        warehouse_id = self.enterprise["central_warehouse"]["warehouse_id"]

        order_id = f"TT{now.strftime('%y%m%d')}{self._order_counter:06d}"

        qty = random.choices([1, 2, 3, 4, 5], weights=[55, 22, 12, 7, 4])[0]
        if self._is_flash_sale(now):
            qty = min(10, qty + random.randint(1, 3))

        # Livestream shopping: cao điểm 12h trưa và 19h-22h tối
        is_live_hour = now.hour in (12, 19, 20, 21, 22)
        if is_live_hour and random.random() < 0.65:
            order_type = "livestream"
        else:
            order_type = "normal" if random.random() < 0.95 else "wholesale"

        # Khấu trừ kho Doanh nghiệp
        in_stock, stock_left = self._deduct_inventory(product["sku"], qty, order_id, now)

        if not in_stock:
            # Hết hàng -> bắt buộc hủy đơn
            is_cancelled = True
            status = TikTokOrderStatus.CANCELLED.value
            stage = 0
            item_cancel_reason = "Seller Cancel - Out of Stock"
        else:
            is_cancelled = random.random() < self.cancel_rate
            status, stage = self._resolve_tiktok_status(is_cancelled)
            item_cancel_reason = random.choice(TIKTOK_CANCEL_REASONS) if is_cancelled else None

            if is_cancelled:
                if stage < 2:
                    self._release_cancelled_stock(product["sku"], qty, order_id, now)
                else:
                    self._queue_return(product["sku"], qty, order_id, now)

        ts = self._generate_timestamps(now, stage, is_cancelled)

        fees = self._calc_tiktok_fees(product["price"], qty)

        payment = random.choice(TIKTOK_PAYMENT_METHODS)
        is_cod = "True" if payment == "COD" else "False"

        pk_id = str(uuid.uuid4())
        user_id = f"USR-{random.randint(100000, 999999)}"
        buyer_uid = f"TTBUY-{random.randint(1000000, 9999999)}"

        item_status_map = {
            TikTokOrderStatus.UNPAID.value: "UNPAID",
            TikTokOrderStatus.AWAITING_SHIPMENT.value: "AWAITING_SHIPMENT",
            TikTokOrderStatus.AWAITING_COLLECTION.value: "AWAITING_COLLECTION",
            TikTokOrderStatus.SHIPPED.value: "IN_TRANSIT",
            TikTokOrderStatus.DELIVERED.value: "DELIVERED",
            TikTokOrderStatus.COMPLETED.value: "COMPLETED",
            TikTokOrderStatus.CANCELLED.value: "CANCELLED",
        }

        _utcnow_tt = datetime.now(timezone.utc)

        return TikTokOrderEvent(
            pkId=pk_id,
            user_id=user_id,
            order_id=order_id,
            order_type=order_type,
            shop_name=shop_name,
            order_status=status,
            created_time=now.isoformat(),
            paid_time=self._fmt(ts["pay"]),
            rts_time=self._fmt(ts["pay"] + timedelta(hours=random.randint(1, 24))) if ts["pay"] and stage >= 1 else None,
            shipped_time=self._fmt(ts["ship"]),
            delivered_time=self._fmt(ts["deliver"]),
            completed_time=self._fmt(ts["complete"]),
            cancelled_time=self._fmt(ts["cancel"]),
            updated_time=_utcnow_tt.isoformat(),
            rts_sla_time=self._fmt(now + timedelta(days=2)) if stage >= 1 else None,
            tts_sla_time=self._fmt(now + timedelta(days=3)) if stage >= 1 else None,
            delivery_sla_time=self._fmt(now + timedelta(days=7)) if stage >= 2 else None,
            cancel_sla_time=self._fmt(now + timedelta(hours=1)) if stage == 0 else None,
            currency="VND",
            total_amount=fees["total_amount"],
            sub_total=fees["sub_total"],
            order_original_price=fees["order_original_price"],
            shipping_fee=fees["shipping_fee"],
            original_shipping_fee=fees["original_shipping_fee"],
            tax_amount=fees["tax_amount"],
            product_tax=fees["product_tax"],
            shipping_tax=fees["shipping_tax"],
            small_order_fee=fees["small_order_fee"],
            retail_delivery_fee=fees["retail_delivery_fee"],
            insurance_fee=fees["insurance_fee"],
            seller_discount=fees["seller_discount"],
            platform_discount=fees["platform_discount"],
            shipping_seller_discount=fees["shipping_seller_discount"],
            shipping_platform_discount=fees["shipping_platform_discount"],
            payment_method=payment,
            is_cod=is_cod,
            fulfillment_type="Seller Fulfillment",
            delivery_type=random.choice(["Standard", "Economy"]),
            delivery_option=random.choice(["Standard", "Economy", "Express"]),
            shipping_provider=random.choice(TIKTOK_CARRIERS) if stage >= 2 else None,
            tracking_number=f"VN{random.randint(100000000000, 999999999999)}" if stage >= 2 else None,
            warehouse_id=warehouse_id if stage >= 1 else None,
            buyer_uid=buyer_uid,
            buyer_name=self._gen_recipient_name(),
            buyer_message=random.choice([None, None, "Giao giờ hành chính", "Gọi trước khi giao",
                                          "Để ở bảo vệ", "Ship nhanh giúp em"]),
            recipient_name=self._gen_recipient_name(),
            recipient_phone=self._gen_phone(),
            full_address="***",
            postal_code=f"{random.randint(10000, 99999)}",
            region_state=addr["state"],
            city_town=addr["city"],
            district=addr["district"],
            product_name=product["name"],
            sku_name=f"{product['name']} - {product.get('category', 'Default')}",
            seller_sku=product["sku"],
            quantity=qty,
            item_status=item_status_map.get(status, status),
            item_sale_price=fees["item_sale_price"],
            item_original_price=product["price"],
            item_platform_disc=fees["item_platform_disc"],
            item_seller_disc=fees["item_seller_disc"],
            product_id=f"TTPROD-{random.randint(1000000, 9999999)}",
            sku_id=f"TTSKU-{random.randint(1000000, 9999999)}",
            package_id=f"TTPKG-{random.randint(100000, 999999)}" if stage >= 2 else None,
            package_status="shipped" if stage >= 2 else None,
            item_cancel_reason=item_cancel_reason,
            createdAt=now.isoformat(),
            updatedAt=_utcnow_tt.isoformat(),
            row_idx=self._row_idx,
        )

    # ──────────── PUBLIC API: INVENTORY & ORDERS ────────────

    def get_inventory_snapshot(self, as_of: Optional[datetime] = None) -> list[dict]:
        """
        Trích xuất ảnh chụp trạng thái tồn kho (Inventory Snapshot) cho toàn bộ danh mục sản phẩm,
        phù hợp nạp trực tiếp vào bảng Fact_Inventory_Daily.
        """
        dt_now = as_of or datetime.now(timezone.utc)
        self._process_incoming_restocks(dt_now)
        self._process_pending_returns(dt_now)

        rows = []
        for sku, inv in self.inventory.items():
            if inv.stock_on_hand <= inv.safety_stock:
                alert_level = "CRITICAL"
            elif inv.stock_on_hand <= inv.reorder_point:
                alert_level = "WARNING"
            else:
                alert_level = "NORMAL"

            needs_reorder = (inv.stock_on_hand <= inv.reorder_point)
            rec_reorder_qty = inv.reorder_qty if needs_reorder else 0

            rows.append({
                "enterprise_id": self.enterprise["enterprise_id"],
                "warehouse_id": self.enterprise["central_warehouse"]["warehouse_id"],
                "sku": sku,
                "product_name": inv.product_name,
                "category": inv.category,
                "stock_on_hand": inv.stock_on_hand,
                "safety_stock": inv.safety_stock,
                "reorder_point": inv.reorder_point,
                "incoming_stock": inv.incoming_stock,
                "pending_return_stock": inv.pending_return_stock,
                "restock_eta": inv.restock_eta,
                "lead_time_days": inv.lead_time_days,
                "alert_level": alert_level,
                "needs_reorder": needs_reorder,
                "recommended_reorder_qty": rec_reorder_qty,
                "total_sold": inv.total_sold,
                "total_restocked": inv.total_restocked,
                "total_returned": inv.total_returned,
                "out_of_stock_count": inv.out_of_stock_count,
                "loaded_at": self._fmt(dt_now),
            })
        return rows

    def get_inventory_alerts(self, as_of: Optional[datetime] = None) -> list[dict]:
        """Lọc ra các SKU đang bị thiếu hàng hoặc cần nhập thêm (CRITICAL hoặc WARNING)."""
        snapshot = self.get_inventory_snapshot(as_of)
        return [item for item in snapshot if item["alert_level"] in ("CRITICAL", "WARNING")]

    def restock_sku(self, sku: str, quantity: int, immediate: bool = True, now: Optional[datetime] = None):
        """Chủ động nhập thêm hàng cho 1 SKU (thủ công hoặc theo quyết định quản trị)."""
        if sku not in self.inventory:
            return
        inv = self.inventory[sku]
        dt_now = now or datetime.now(timezone.utc)

        if immediate:
            stock_before = inv.stock_on_hand
            inv.stock_on_hand += quantity
            inv.total_restocked += quantity
            inv.last_updated = self._fmt(dt_now)

            self._movement_counter += 1
            mov = InventoryMovementEvent(
                movement_id=f"MOV-IN-{self._movement_counter:06d}",
                sku=sku,
                warehouse_id=self.enterprise["central_warehouse"]["warehouse_id"],
                movement_type="INBOUND_RESTOCK",
                quantity=quantity,
                stock_before=stock_before,
                stock_after=inv.stock_on_hand,
                reference_order_id=None,
                timestamp=self._fmt(dt_now),
            )
            self.movement_history.append(mov)
        else:
            inv.incoming_stock += quantity
            inv.restock_eta = self._fmt(dt_now + timedelta(days=inv.lead_time_days))

    def reset_inventory(self, stock_override: Optional[dict] = None):
        """Khôi phục tồn kho về trạng thái ban đầu."""
        self.movement_history.clear()
        self.pending_returns.clear()
        self._movement_counter = 0
        self._init_inventory(stock_override)

    def generate_event(self, now: Optional[datetime] = None) -> dict:
        """Sinh 1 sự kiện đơn hàng (Shopee hoặc TikTok) và tự động cập nhật tồn kho."""
        now = now or datetime.now(timezone.utc)
        if random.random() < self.shopee_ratio:
            event = self._generate_shopee_event(now)
        else:
            event = self._generate_tiktok_event(now)

        d = asdict(event)
        # Loại bỏ None values để giảm payload size
        return {k: v for k, v in d.items() if v is not None}

    def run(
        self,
        base_events_per_second: float = 3.0,
        duration_seconds: Optional[int] = None,
        output_mode: str = "print",  # "print" | "return"
    ):
        """
        Vòng lặp sinh sự kiện liên tục.

        Args:
            base_events_per_second: Tốc độ cơ bản (trước khi nhân hệ số mùa vụ).
            duration_seconds: Giới hạn thời gian chạy (None = chạy vô hạn).
            output_mode: "print" = in ra stdout, "return" = yield events.
        """
        start = time.time()

        while duration_seconds is None or (time.time() - start) < duration_seconds:
            now = datetime.now(timezone.utc)
            n_events = self.orders_per_second(now, base_events_per_second)

            for _ in range(n_events):
                event = self.generate_event(now)

                if output_mode == "print":
                    print(json.dumps(event, ensure_ascii=False))

            time.sleep(1)

    def stream(
        self,
        base_events_per_second: float = 3.0,
        duration_seconds: Optional[int] = None,
    ):
        """Generator version: yield events liên tục, dùng khi tích hợp Kafka."""
        start = time.time()

        while duration_seconds is None or (time.time() - start) < duration_seconds:
            now = datetime.now(timezone.utc)
            n_events = self.orders_per_second(now, base_events_per_second)

            for _ in range(n_events):
                yield self.generate_event(now)

            time.sleep(1)


# ─────────────────────────────────────────────────────────────
# 4. DEMO — Chạy thử mô phỏng Doanh nghiệp & Tồn kho
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 85)
    print("  DEMO: Enterprise E-Commerce & Inventory Simulator (Vietnam Multi-Channel)")
    print("  Mô hình: 1 Doanh nghiệp đa kênh (Shopee Mall + TikTok Official Shop)")
    print("  Kho trung tâm: WH-MOCK-CENTRAL (Quản lý tồn kho thực tế, xuất bán & nhập hàng)")
    print("=" * 85)
    print()

    simulator = ECommerceSimulator(
        shopee_ratio=0.55,
        cancel_rate=0.12,
        trend_weekly_pct=0.02,
    )

    print("─── Thông tin Doanh nghiệp & Kênh bán ───")
    ent = simulator.enterprise
    print(f"  Doanh nghiệp: {ent['enterprise_name']} ({ent['enterprise_id']})")
    print(f"  Kho tổng:     {ent['central_warehouse']['warehouse_name']} ({ent['central_warehouse']['warehouse_id']})")
    print(f"  Shopee Store: {ent['channels'][Platform.SHOPEE]['shop_name']}")
    print(f"  TikTok Store: {ent['channels'][Platform.TIKTOK]['shop_name']}")
    print()

    # ── Demo 1: Xem 3 đơn mẫu ──
    print("─── Demo 1: Ba đơn hàng mẫu & trừ kho thực tế ───")
    for i in range(3):
        event = simulator.generate_event()
        platform = event.get("_platform", "unknown")
        status = event.get("order_status", "?")
        sku = event.get("item_sku") or event.get("seller_sku", "?")
        qty = event.get("quantity", 0)
        stock_left = simulator.inventory[sku].stock_on_hand
        amount_key = "buyer_total_amount" if platform == "shopee" else "total_amount"
        amount = event.get(amount_key, 0)

        print(f"  [{platform.upper():7}] {status:20} | SKU: {sku:12} x{qty} | "
              f"Tồn kho còn: {stock_left:>4} | {amount:>12,.0f} VND")
    print()

    # ── Demo 2: Snapshot Tồn kho & Cảnh báo nhập hàng ──
    print("─── Demo 2: Ảnh chụp Tồn kho & Cảnh báo (Snapshot) ───")
    snapshot = simulator.get_inventory_snapshot()
    for row in snapshot[:6]:
        print(f"  SKU: {row['sku']:12} | Tồn: {row['stock_on_hand']:>4} | "
              f"ROP: {row['reorder_point']:>3} | SS: {row['safety_stock']:>3} | "
              f"Cảnh báo: {row['alert_level']:8} | Đề xuất nhập: {row['recommended_reorder_qty']}")
    print(f"  ... tổng cộng {len(snapshot)} mặt hàng được quản lý.")
    print()

    # ── Demo 3: Thống kê seasonal multiplier ──
    print("─── Demo 3: Seasonal Multiplier theo giờ (hôm nay) ───")
    now = datetime.now(timezone.utc)
    for h in [0, 9, 12, 15, 20, 23]:
        test_time = now.replace(hour=h, minute=0, second=0)
        mult = simulator._seasonal_multiplier(test_time)
        ops = simulator.orders_per_second(test_time, base_rate=3.0)
        bar = "█" * int(mult * 5)
        print(f"  {h:02d}:00  mult={mult:5.2f}  ~{ops} events/s  {bar}")
    print()
