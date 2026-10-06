import base64
import json
import urllib.request
import zlib
import os
import sys

DIAGRAMS = {
    "diagram_6_etl_star_schema_pipeline": """flowchart TD
    subgraph S_SRC["1. NGUỒN DỮ LIỆU ĐỆM (DATA LAKE SINK)"]
        MINIO[("MinIO S3 Object Storage<br/>Bucket: ecom-raw-lake<br/>orders/year=YYYY/month=MM/day=DD/*.parquet")]
    end

    subgraph S_EXTRACT["2. TẦNG TRÍCH XUẤT (EXTRACT)"]
        EXT["extract.py: extract_orders()<br/>• Kết nối MinIO SDK (boto3/minio)<br/>• Quét các partition theo target_date hoặc all_dates<br/>• Đọc các tệp Parquet vào Pandas DataFrame"]
    end

    subgraph S_TRANSFORM["3. TẦNG CHUYỂN HÓA & LÀM SẠCH (TRANSFORM)"]
        direction TB
        UNIFY["unify_schema()<br/>• Shopee (84 cột) & TikTok (71 cột) ➔ Schema chung<br/>• Đồng bộ khóa: order_id, sku, product_name, category<br/>• Phân tách phí sàn (Shopee) & thuế (TikTok)"]
        CLEAN["clean_data()<br/>• Khử trùng lặp (order_id, platform)<br/>• Ép kiểu datetime UTC (create, pay, ship, complete)<br/>• Ép kiểu số: quantity, prices, discounts, fees<br/>• Tính toán: subtotal = quantity × original_price<br/>• Tính buyer_total_amount thực trả"]
        UNIFY --> CLEAN
    end

    subgraph S_VALIDATE["4. TẦNG KIỂM ĐỊNH CHẤT LƯỢNG (DATA QUALITY GATE)"]
        direction TB
        VAL["data_validation.py: DataValidator.validate()<br/>• Null Check: order_id, platform, sku, create_time<br/>• Domain Check: platform ∈ {shopee, tiktok}<br/>• Range Check: quantity > 0, price >= 0, amount >= 0<br/>• Chronological Order: create <= pay <= ship <= complete"]
        GATE{"Kiểm định<br/>Hợp lệ?"}
        CLEAN_DF[("clean_df<br/>Bản ghi đạt chuẩn")]
        QUARANTINE[("quarantine_df<br/>Cách ly bản ghi lỗi & gắn lý do")]
        VAL --> GATE
        GATE -- "ĐẠT" --> CLEAN_DF
        GATE -- "LỖI" --> QUARANTINE
    end

    subgraph S_LOAD["5. TẦNG NẠP KHO DỮ LIỆU (LOAD TO STAR SCHEMA)"]
        direction TB
        DIMS["Upsert Dimension Tables (load.py)<br/>• Dim_Products (ON CONFLICT sku DO UPDATE)<br/>• Dim_Shops (ON CONFLICT shop_id, platform DO UPDATE)<br/>• Dim_Geography (Bắc/Trung/Nam mapping)<br/>• Dim_Carriers, Dim_Payment, Dim_Dates"]
        LOOKUP["In-Memory Surrogate Key Lookup<br/>Ánh xạ Natural Keys ➔ Surrogate Keys (*_key)"]
        FACT["Batch Insert Fact_Orders<br/>• Batch Size: 1000 - 5000 records<br/>• Ràng buộc lũy đẳng (Idempotence):<br/>ON CONFLICT (order_id, platform) DO NOTHING"]
        DIMS --> LOOKUP --> FACT
    end

    subgraph S_DWH["6. RELATIONAL DATA WAREHOUSE (POSTGRESQL)"]
        PG[("PostgreSQL 16 Engine<br/>ecom_warehouse Database<br/>Star Schema: Fact_Orders + 5 Dims")]
    end

    MINIO --> EXT --> S_TRANSFORM
    CLEAN --> VAL
    CLEAN_DF --> DIMS
    FACT --> PG

    classDef lakeStyle fill:#1e293b,stroke:#34d399,stroke-width:2px,color:#f8fafc;
    classDef procStyle fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;
    classDef gateStyle fill:#1e293b,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;
    classDef pgStyle fill:#1e293b,stroke:#a855f7,stroke-width:2px,color:#f8fafc;
    classDef badStyle fill:#450a0a,stroke:#ef4444,stroke-width:2px,color:#fca5a5;

    class MINIO lakeStyle;
    class EXT,UNIFY,CLEAN,DIMS,LOOKUP,FACT procStyle;
    class VAL,GATE,CLEAN_DF gateStyle;
    class QUARANTINE badStyle;
    class PG pgStyle;""",

    "diagram_7_star_schema_er_model": """erDiagram
    Dim_Products ||--o{ Fact_Orders : "product_key"
    Dim_Shops ||--o{ Fact_Orders : "shop_key"
    Dim_Geography ||--o{ Fact_Orders : "geo_key"
    Dim_Dates ||--o{ Fact_Orders : "date_key"
    Dim_Payment ||--o{ Fact_Orders : "payment_key"
    Dim_Carriers ||--o{ Fact_Orders : "carrier_key"

    Fact_Orders {
        bigint order_fact_id PK
        int product_key FK
        int shop_key FK
        int geo_key FK
        int date_key FK
        int payment_key FK
        int carrier_key FK
        varchar order_id
        varchar platform
        varchar order_status
        boolean is_cancelled
        int quantity
        numeric original_price
        numeric discounted_price
        numeric subtotal
        numeric buyer_total_amount
        numeric seller_discount
        numeric platform_discount
        numeric voucher_total
        numeric commission_fee
        numeric service_fee
        numeric shipping_fee
        timestamp create_time
        timestamp pay_time
        timestamp shipped_time
        timestamp completed_time
        timestamp loaded_at
    }

    Dim_Products {
        int product_key PK
        varchar sku UK
        varchar product_name
        varchar category
        varchar sub_category
        numeric unit_cost
        numeric weight_kg
        timestamp created_at
        timestamp updated_at
    }

    Dim_Shops {
        int shop_key PK
        varchar shop_id
        varchar platform
        varchar shop_name
        varchar connection_id
        timestamp created_at
    }

    Dim_Geography {
        int geo_key PK
        varchar state
        varchar city
        varchar district
        varchar country
        varchar region
        timestamp created_at
    }

    Dim_Dates {
        int date_key PK
        date full_date UK
        smallint day_of_week
        varchar day_name
        smallint week_of_year
        smallint month
        smallint year
        boolean is_weekend
        boolean is_holiday
        boolean is_mega_sale
    }

    Dim_Payment {
        int payment_key PK
        varchar payment_method UK
        boolean is_cod
        timestamp created_at
    }

    Dim_Carriers {
        int carrier_key PK
        varchar carrier_name UK
        timestamp created_at
    }""",

    "diagram_8_feature_engineering_flow": """flowchart TD
    subgraph S_DWH["1. KHO DỮ LIỆU ĐẦU VÀO (POSTGRESQL DWH)"]
        DWH_Q["Fact_Orders JOIN Dim_Products<br/>• Lọc đơn hợp lệ: is_cancelled = FALSE<br/>• Trường: sku, create_time, quantity, original_price, buyer_total_amount, category"]
    end

    subgraph S_SEG["2. PHÂN KHÚC HÀNG HÓA (ABC/XYZ & INVENTORY POLICY)"]
        direction TB
        ABC["Phân tích Pareto ABC (Doanh thu lũy kế)<br/>• Nhóm A: 80% Doanh thu (Sản phẩm cốt lõi)<br/>• Nhóm B: 15% Doanh thu tiếp theo<br/>• Nhóm C: 5% Doanh thu còn lại (Long-tail)"]
        XYZ["Phân tích Biến động XYZ (Coefficient of Variation)<br/>• CV = std_demand / mean_demand<br/>• X: CV <= 0.5 (Ổn định, dễ dự báo)<br/>• Y: 0.5 < CV <= 1.0 (Mùa vụ)<br/>• Z: CV > 1.0 (Thất thường, lumpy)"]
        POLICY["Ma trận 9 ô ABC/XYZ & Hoạch định Tồn kho<br/>• Gán nhãn: AX, AY, AZ, BX, BY, BZ, CX, CY, CZ<br/>• Tính Safety Stock: SS = Z × sigma × sqrt(L)<br/>• Tính Reorder Point: ROP = (mean × L) + SS"]
        ABC & XYZ --> POLICY
    end

    subgraph S_AGG["3. TỔNG HỢP HẠT NGÀY & LƯỚI THỜI GIAN LIÊN TỤC"]
        direction TB
        DAILY_GROUP["Gom nhóm theo (sku, date)<br/>• daily_demand = sum(quantity)<br/>• daily_revenue = sum(buyer_total_amount)<br/>• avg_unit_price = mean(original_price)<br/>• order_count = count(orders)"]
        GRID["Full Cartesian Grid Imputation<br/>• Tạo MultiIndex: (Tất cả SKU) × (Toàn bộ ngày liên tục)<br/>• Điền số 0 cho ngày không có đơn: demand=0, rev=0<br/>• Forward-fill / Backward-fill giá đơn vị"]
        DAILY_GROUP --> GRID
    end

    subgraph S_FEAT["4. TRÍCH XUẤT ĐẶC TRƯNG CHUỖI THỜI GIAN (TIME-SERIES FEATURES)"]
        direction TB
        CAL["Lịch & Mùa vụ (Calendar Features)<br/>• DayOfWeek (0-6), DayOfMonth, Month, Quarter<br/>• is_weekend (Thứ 7, CN = 1)<br/>• is_mega_sale (Ngày đôi TMĐT: 9/9, 10/10, 11/11, 12/12)"]
        LAGS["Biến trễ Nhu cầu (Lag Features)<br/>• lag_1, lag_2, lag_3 (Quán tính ngắn hạn)<br/>• lag_7, lag_14, lag_21, lag_28 (Chu kỳ lặp tuần)"]
        ROLL["Thống kê trượt (Rolling Statistics)<br/>• shift(1) chống Data Leakage tuyệt đối<br/>• Cửa sổ: 7, 14, 28 ngày<br/>• Metrics: rolling_mean, rolling_std, rolling_max, rolling_min"]
        EMA["Trung bình trượt số mũ (EMA)<br/>• ema_7, ema_14 (Trọng số phân rã hàm mũ)"]
        MOM["Quán tính tăng trưởng (Momentum)<br/>• growth_wow = (lag_1 - lag_7) / (lag_7 + 1.0)"]
        TARGET["Biến mục tiêu (Target Demand)<br/>• target_t_plus_1 = shift(-1) của daily_demand"]
    end

    subgraph S_MERGE["5. ĐÓNG GÓI & FEATURE STORE EXPORT"]
        direction TB
        JOIN_FEAT["Ghép nối Features + ABC/XYZ Metadata"]
        ENC["Mã hóa biến phân loại (Categorical Codes)<br/>category_code, abc_class_code, xyz_class_code, matrix_class_code"]
        BURNIN["Khử giai đoạn khởi động (Burn-in Dropna)<br/>Loại bỏ dòng thiếu lag_28 và target_t_plus_1"]
        STORE[("Feature Store (Parquet)<br/>data/features_daily.parquet<br/>Sẵn sàng huấn luyện LightGBM / XGBoost / PyTorch")]
        JOIN_FEAT --> ENC --> BURNIN --> STORE
    end

    DWH_Q --> S_SEG
    DWH_Q --> S_AGG
    GRID --> S_FEAT
    S_SEG & S_FEAT --> S_MERGE

    classDef dwhStyle fill:#1e293b,stroke:#a855f7,stroke-width:2px,color:#f8fafc;
    classDef segStyle fill:#1e293b,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;
    classDef aggStyle fill:#1e293b,stroke:#06b6d4,stroke-width:2px,color:#f8fafc;
    classDef featStyle fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;
    classDef storeStyle fill:#1e293b,stroke:#34d399,stroke-width:2px,color:#f8fafc;

    class DWH_Q dwhStyle;
    class ABC,XYZ,POLICY segStyle;
    class DAILY_GROUP,GRID aggStyle;
    class CAL,LAGS,ROLL,EMA,MOM,TARGET featStyle;
    class JOIN_FEAT,ENC,BURNIN,STORE storeStyle;""",

    "diagram_9_walk_forward_validation": """flowchart TD
    subgraph S_COMP["SO SÁNH PHƯƠNG PHÁP CHIA TẬP DỮ LIỆU"]
        direction TB
        WRONG["❌ RANDOM K-FOLD / SHUFFLE SPLIT (SAI LẦM TRONG TIME-SERIES)<br/>• Xáo trộn ngẫu nhiên dữ liệu quá khứ và tương lai<br/>• Data Leakage nghiêm trọng: Dùng thông tin ngày mai dự báo ngày hôm nay<br/>• Kết quả trên giấy tờ rất cao nhưng sập hoàn toàn khi chạy Production"]
        RIGHT["✅ WALK-FORWARD VALIDATION / EXPANDING WINDOW (CHUẨN TIME-SERIES)<br/>• Bảo toàn tuyệt đối quan hệ nhân quả và dòng chảy thời gian<br/>• Không rò rỉ thông tin tương lai vào mô hình huấn luyện<br/>• Mô phỏng trung thực quy trình tự động tái huấn luyện (Retraining) định kỳ"]
    end

    subgraph S_SPLITS["CƠ CHẾ HOẠT ĐỘNG WALK-FORWARD VALIDATION (N_SPLITS = 3)"]
        direction TB
        F1["FOLD 1:<br/>[━━━━━━━━ Train Window (Quá khứ ban đầu) ━━━━━━━━] [Val: 7d] [Test: 14d]"]
        F2["FOLD 2 (Cửa sổ Train mở rộng tịnh tiến):<br/>[━━━━━━━━━━━━━━ Train Window (Mở rộng lần 1) ━━━━━━━━━━━━━━] [Val: 7d] [Test: 14d]"]
        F3["FOLD 3 (Cửa sổ Train mở rộng đầy đủ nhất):<br/>[━━━━━━━━━━━━━━━━━━━━ Train Window (Toàn diện) ━━━━━━━━━━━━━━━━━━━━] [Val: 7d] [Test: 14d]"]
        F1 --> F2 --> F3
    end

    subgraph S_USAGE["MỤC TIÊU CÁC TẬP DỮ LIỆU CON TRONG TỪNG FOLD"]
        direction LR
        TR["Train Window (Expanding)<br/>• Học trọng số mô hình<br/>(LightGBM / XGBoost / PyTorch)"]
        VA["Validation Window (7 ngày)<br/>• Tinh chỉnh siêu tham số (Optuna)<br/>• Early Stopping chống Overfitting"]
        TE["Test Window (14 ngày)<br/>• Kiểm thử khách quan (Unseen data)<br/>• Đo đạc WAPE, MAE, RMSE, R2"]
        TR --> VA --> TE
    end

    RIGHT --> S_SPLITS
    S_SPLITS --> S_USAGE

    classDef badStyle fill:#450a0a,stroke:#ef4444,stroke-width:2px,color:#fca5a5;
    classDef goodStyle fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#a7f3d0;
    classDef foldStyle fill:#1e293b,stroke:#38bdf8,stroke-width:2px,color:#f8fafc;
    classDef useStyle fill:#1e293b,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;

    class WRONG badStyle;
    class RIGHT goodStyle;
    class F1,F2,F3 foldStyle;
    class TR,VA,TE useStyle;"""
}

def render_to_svg(code):
    payload = {
        "code": code,
        "mermaid": {
            "theme": "dark",
            "themeVariables": {
                "fontSize": "15px",
                "fontFamily": "Inter, Segoe UI, sans-serif"
            }
        }
    }
    json_bytes = json.dumps(payload).encode('utf-8')
    compressed = zlib.compress(json_bytes, level=9)
    encoded = base64.urlsafe_b64encode(compressed).decode('utf-8')
    url = f"https://mermaid.ink/svg/pako:{encoded}"
    
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode('utf-8')

def main():
    target_dir = os.path.join("docs", "02-warehouse-etl-feature-store")
    os.makedirs(target_dir, exist_ok=True)
    
    for name, code in DIAGRAMS.items():
        print(f"Generating SVG for {name}...")
        try:
            svg_content = render_to_svg(code)
            filepath = os.path.join(target_dir, f"{name}.svg")
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(svg_content)
            print(f" -> Successfully saved: {filepath} ({len(svg_content)} bytes)")
        except Exception as e:
            print(f" -> ERROR generating {name}: {e}")

if __name__ == "__main__":
    main()
