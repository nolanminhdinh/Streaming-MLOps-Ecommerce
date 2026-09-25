"""
Data_Simulator.py
------------------
Mô phỏng luồng phát sinh đơn hàng E-Commerce theo thời gian thực, dựa trên
schema thực tế của bộ dữ liệu ``vietnam_ecommerce`` trên XomData:
  - shopee_orders (84 cột)
  - tiktok_orders (71 cột)

Các quy luật order đã triển khai:
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
     - Tên sản phẩm, SKU, kho hàng phân bố theo khu vực.
     - Địa chỉ giao hàng: 63 tỉnh thành Việt Nam.
     - Phương thức thanh toán: COD, Ví Shopee, VNPay, MoMo, Thẻ tín dụng, ...
     - Đơn vị tiền: VND.

Tích hợp:
  - Tương thích với Kafka Producer ở ``ingestion/producer.py``.
  - Output JSON có thể ghi thẳng vào topic ``ecom.orders.raw``.

Nguồn dữ liệu tham khảo:
  https://dataset.xomdata.com/datasets/schema/vietnam_ecommerce
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

# ── Sản phẩm mẫu Việt Nam ──
PRODUCT_CATALOG = [
    {"name": "Ốp lưng iPhone 15 Pro Max silicon",          "sku": "OL-IP15PM",   "price": 59_000,   "weight": 0.05, "category": "Phụ kiện điện thoại"},
    {"name": "Áo thun nam cotton Premium Basic",            "sku": "ATN-CB-001",  "price": 129_000,  "weight": 0.2,  "category": "Thời trang nam"},
    {"name": "Sạc nhanh 65W GaN Type-C PD",                "sku": "SN-65W-GAN",  "price": 249_000,  "weight": 0.12, "category": "Phụ kiện điện tử"},
    {"name": "Bột giặt OMO Matic 6kg",                     "sku": "OMO-6KG",     "price": 175_000,  "weight": 6.0,  "category": "Gia dụng"},
    {"name": "Tai nghe Bluetooth TWS AirBuds Pro",          "sku": "TN-TWS-PRO",  "price": 450_000,  "weight": 0.08, "category": "Điện tử"},
    {"name": "Kem chống nắng Anessa SPF50+",               "sku": "KCN-ANESSA",  "price": 399_000,  "weight": 0.06, "category": "Mỹ phẩm"},
    {"name": "Bàn phím cơ Gaming RGB 104 phím",             "sku": "BP-GM-104",   "price": 690_000,  "weight": 0.95, "category": "Gaming"},
    {"name": "Nồi chiên không dầu 6L Air Fryer",           "sku": "AF-6L-001",   "price": 1_290_000,"weight": 4.5,  "category": "Gia dụng"},
    {"name": "Sữa rửa mặt CeraVe 236ml",                  "sku": "SRM-CERAVE",  "price": 289_000,  "weight": 0.3,  "category": "Mỹ phẩm"},
    {"name": "Giày thể thao nam Jogger Runner",             "sku": "GTT-JR-001",  "price": 550_000,  "weight": 0.7,  "category": "Giày dép"},
    {"name": "Balo laptop chống sốc 15.6 inch",            "sku": "BL-15-CS",    "price": 350_000,  "weight": 0.65, "category": "Túi & Balo"},
    {"name": "Nước hoa hồng Klairs 180ml",                 "sku": "NHH-KLA180",  "price": 320_000,  "weight": 0.25, "category": "Mỹ phẩm"},
    {"name": "Chuột gaming Logitech G304",                  "sku": "CG-G304",     "price": 690_000,  "weight": 0.1,  "category": "Gaming"},
    {"name": "Bộ drap giường cotton 1m8",                   "sku": "DG-CT-1M8",   "price": 399_000,  "weight": 1.2,  "category": "Gia dụng"},
    {"name": "Đèn LED dây trang trí 10m",                   "sku": "DEN-LED-10",  "price": 89_000,   "weight": 0.15, "category": "Decor"},
    {"name": "Serum Vitamin C The Ordinary 30ml",           "sku": "SR-VC-TO30",  "price": 230_000,  "weight": 0.05, "category": "Mỹ phẩm"},
    {"name": "Combo 10 khẩu trang 3D KF94",                "sku": "KT-KF94-10",  "price": 45_000,   "weight": 0.08, "category": "Sức khoẻ"},
    {"name": "Quạt mini cầm tay sạc USB",                  "sku": "QM-USB-01",   "price": 69_000,   "weight": 0.12, "category": "Gia dụng"},
    {"name": "Túi xách nữ da PU thời trang",               "sku": "TX-PU-001",   "price": 259_000,  "weight": 0.35, "category": "Thời trang nữ"},
    {"name": "Miếng dán cường lực Samsung Galaxy S24",      "sku": "CL-SS-S24",   "price": 35_000,   "weight": 0.02, "category": "Phụ kiện điện thoại"},
]

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

# ── Tên shop mẫu ──
SHOP_NAMES = [
    "MinhShopVN", "BeautyHouseHCM", "TechZone Official",
    "FashionKorea_VN", "HomeDecorHN", "SportsGear365",
    "GadgetWorldVN", "CosmeBoutique", "SneakerHub_SG",
    "FreshMartOnline",
]

# ── Lý do huỷ đơn ──
SHOPEE_CANCEL_REASONS = [
    "Khách hàng yêu cầu huỷ", "Hết hàng", "Không liên lạc được khách",
    "Đổi ý không mua", "Trùng đơn", "Sản phẩm lỗi trước khi gửi",
    "Buyer requested to cancel", "Out of stock",
]
TIKTOK_CANCEL_REASONS = [
    "Buyer Cancel", "Seller Cancel - Out of Stock",
    "System Cancel - Payment Timeout", "Buyer changed mind",
    "Logistics issue", "Wrong item",
]


# ─────────────────────────────────────────────────────────────
# 2. DATA CLASSES — ORDER EVENTS
# ─────────────────────────────────────────────────────────────

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
    """Sinh sự kiện đơn hàng giả lập đa kênh (Shopee + TikTok) với các quy luật:
    - Order lifecycle / state machine.
    - Xu hướng tăng trưởng dài hạn + mùa vụ + flash sale + mega-sale.
    - Phân bố giá và phí sàn theo quy tắc thực tế.
    """

    def __init__(
        self,
        shopee_ratio: float = 0.55,     # 55% đơn Shopee, 45% TikTok
        cancel_rate: float = 0.12,       # 12% tỉ lệ huỷ đơn
        trend_weekly_pct: float = 0.02,  # +2% mỗi tuần
        start_date: Optional[datetime] = None,
    ):
        self.shopee_ratio = shopee_ratio
        self.cancel_rate = cancel_rate
        self.trend_weekly_pct = trend_weekly_pct
        self.start_date = start_date or datetime.now(timezone.utc)
        self._order_counter = 0
        self._row_idx = 0

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

    def _seasonal_multiplier(self, now: datetime) -> float:
        """Hệ số tổng hợp: tuần × flash × mega × giờ trong ngày."""
        # Hiệu ứng ngày trong tuần: T2-T6=1.0, T7=1.3, CN=1.2
        dow = now.weekday()
        dow_factor = {5: 1.3, 6: 1.2}.get(dow, 1.0)

        # Hiệu ứng giờ trong ngày (bell curve quanh 10h sáng và 20h tối)
        hour = now.hour
        hour_factor = 0.3 + 0.7 * (
            math.exp(-0.5 * ((hour - 10) / 3) ** 2) +
            math.exp(-0.5 * ((hour - 20) / 2.5) ** 2)
        )

        flash = 2.5 if self._is_flash_sale(now) else 1.0
        mega = 4.0 if self._is_mega_sale(now) else 1.0

        return dow_factor * hour_factor * flash * mega * self._trend_multiplier(now)

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
        return random.choice(PRODUCT_CATALOG)

    def _pick_address(self) -> dict:
        return random.choice(VIETNAM_ADDRESSES)

    def _pick_shop(self) -> tuple[str, str, str]:
        """Trả về (shop_name, shop_id, connection_id)."""
        name = random.choice(SHOP_NAMES)
        shop_id = str(random.randint(10000000, 99999999))
        conn_id = f"CONN-{shop_id[:4]}"
        return name, shop_id, conn_id

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
        shop_name, shop_id, conn_id = self._pick_shop()

        is_cancelled = random.random() < self.cancel_rate
        status, stage = self._resolve_shopee_status(is_cancelled)
        ts = self._generate_timestamps(now, stage, is_cancelled)

        qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]
        # Flash sale → tăng quantity
        if self._is_flash_sale(now):
            qty = min(10, qty + random.randint(1, 3))

        # Discounted price
        disc_price = round(product["price"] * random.uniform(0.80, 1.0))

        fees = self._calc_shopee_fees(product["price"], qty)

        payment = random.choice(SHOPEE_PAYMENT_METHODS)
        is_cod = "True" if payment == "COD" else "False"

        order_sn = f"SPE{now.strftime('%y%m%d')}{self._order_counter:06d}"
        # Fix: dùng timezone-aware datetime cho synced_at / update_time
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
            cancel_reason=random.choice(SHOPEE_CANCEL_REASONS) if is_cancelled else None,
            cancel_by=random.choice(["buyer", "seller", "system"]) if is_cancelled else None,
            fulfillment_flag="fulfilled_by_shopee" if random.random() < 0.3 else "fulfilled_by_seller",
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
        shop_name, _, _ = self._pick_shop()

        is_cancelled = random.random() < self.cancel_rate
        status, stage = self._resolve_tiktok_status(is_cancelled)
        ts = self._generate_timestamps(now, stage, is_cancelled)

        qty = random.choices([1, 2, 3, 4, 5], weights=[55, 22, 12, 7, 4])[0]
        if self._is_flash_sale(now):
            qty = min(10, qty + random.randint(1, 3))

        fees = self._calc_tiktok_fees(product["price"], qty)

        payment = random.choice(TIKTOK_PAYMENT_METHODS)
        is_cod = "True" if payment == "COD" else "False"

        order_id = f"TT{now.strftime('%y%m%d')}{self._order_counter:06d}"
        pk_id = str(uuid.uuid4())
        user_id = f"USR-{random.randint(100000, 999999)}"
        buyer_uid = f"TTBUY-{random.randint(1000000, 9999999)}"

        # TikTok item_status follows order_status
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
            order_type="normal" if random.random() < 0.9 else "wholesale",
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
            fulfillment_type="Seller Fulfillment" if random.random() < 0.7 else "TikTok Fulfillment",
            delivery_type=random.choice(["Standard", "Economy"]),
            delivery_option=random.choice(["Standard", "Economy", "Express"]),
            shipping_provider=random.choice(TIKTOK_CARRIERS) if stage >= 2 else None,
            tracking_number=f"VN{random.randint(100000000000, 999999999999)}" if stage >= 2 else None,
            warehouse_id=f"WH-{random.randint(100, 999)}" if stage >= 1 else None,
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
            item_cancel_reason=random.choice(TIKTOK_CANCEL_REASONS) if is_cancelled else None,
            createdAt=now.isoformat(),
            updatedAt=_utcnow_tt.isoformat(),
            row_idx=self._row_idx,
        )

    # ──────────── PUBLIC API ────────────

    def generate_event(self, now: Optional[datetime] = None) -> dict:
        """Sinh 1 sự kiện đơn hàng (Shopee hoặc TikTok), trả về dict."""
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

        Lưu ý: Khi tích hợp Kafka, thay output_mode="print" bằng cách gọi
        ``ingestion.producer.send_event(producer, event)`` trong vòng lặp.
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
# 4. DEMO — Chạy thử mô phỏng
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 80)
    print("  DEMO: E-Commerce Order Simulator — Vietnam Multi-Channel")
    print("  Schema: https://dataset.xomdata.com/datasets/schema/vietnam_ecommerce")
    print("=" * 80)
    print()

    simulator = ECommerceSimulator(
        shopee_ratio=0.55,     # 55% Shopee, 45% TikTok (phản ánh thị phần VN)
        cancel_rate=0.12,      # ~12% tỉ lệ huỷ đơn
        trend_weekly_pct=0.02, # tăng 2%/tuần
    )

    # ── Demo 1: Xem 3 đơn mẫu ──
    print("─── Demo 1: Ba đơn hàng mẫu ───")
    for i in range(3):
        event = simulator.generate_event()
        platform = event.get("_platform", "unknown")
        status = event.get("order_status", "?")
        amount_key = "buyer_total_amount" if platform == "shopee" else "total_amount"
        amount = event.get(amount_key, 0)
        product = event.get("item_name") or event.get("product_name", "?")
        qty = event.get("quantity", 0)

        print(f"  [{platform.upper():7}] {status:20} | {product[:35]:35} "
              f"x{qty} | {amount:>12,.0f} VND")
    print()

    # ── Demo 2: Thống kê seasonal multiplier ──
    print("─── Demo 2: Seasonal Multiplier theo giờ (hôm nay) ───")
    now = datetime.now(timezone.utc)
    for h in range(24):
        test_time = now.replace(hour=h, minute=0, second=0)
        mult = simulator._seasonal_multiplier(test_time)
        ops = simulator.orders_per_second(test_time, base_rate=3.0)
        bar = "█" * int(mult * 5)
        print(f"  {h:02d}:00  mult={mult:5.2f}  ~{ops} events/s  {bar}")
    print()

    # ── Demo 3: Stream 5 giây ──
    print("─── Demo 3: Stream 5 giây (JSON → stdout) ───")
    print("    (Đây là output tương thích Kafka topic ecom.orders.raw)")
    print()
    simulator.run(base_events_per_second=2, duration_seconds=5)
