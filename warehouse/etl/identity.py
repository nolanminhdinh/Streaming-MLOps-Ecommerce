"""Phát hiện xung đột danh tính đơn hàng trước khi upsert vào Fact_Orders.

Một mã đơn (order_id, platform) chỉ thuộc về MỘT đơn hàng. Các sự kiện cập nhật trạng thái
của cùng một đơn luôn mang cùng thời điểm tạo đơn (create_time); bản mới nhất thắng nhờ
source_updated_at trong load.py. Nếu hai sự kiện cùng mã nhưng KHÁC thời điểm tạo, đó là hai đơn
khác nhau dùng chung mã (lỗi nguồn): upsert sẽ gộp chúng và âm thầm làm mất một đơn.

Quy tắc (17/11/2026): đơn đến trước giữ mã. Mốc so sánh là create_time đã lưu trong warehouse;
nếu mã chưa có trong warehouse thì mốc là create_time sớm nhất trong lô. Mọi dòng lệch mốc bị cách ly
với lý do ORDER_IDENTITY_CONFLICT, không ghi đè và không bị bỏ đi trong im lặng.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from warehouse.etl import load as loader

CONFLICT_REASON = "ORDER_IDENTITY_CONFLICT"
KEYS = ["order_id", "platform"]


def _second(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.floor("s")


def stored_create_times(keys: pd.DataFrame, engine=None) -> pd.DataFrame:
    """create_time đã lưu của các mã đơn có trong lô (chỉ đọc)."""
    if keys.empty:
        return pd.DataFrame(columns=[*KEYS, "stored_create_time"])
    engine = engine or loader.get_engine()
    ids = sorted({str(value) for value in keys["order_id"]})
    query = text("""
        SELECT order_id, platform, MIN(create_time) AS stored_create_time
        FROM Fact_Orders WHERE order_id = ANY(:ids)
        GROUP BY order_id, platform
    """)
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(query, {"ids": ids}).mappings()]
    return pd.DataFrame(rows, columns=[*KEYS, "stored_create_time"])


def split_identity_conflicts(valid: pd.DataFrame, lookup=stored_create_times):
    """Tách các dòng hợp lệ thành (giữ lại, xung đột danh tính).

    Dòng xung đột mang cột rejection_reasons để đi cùng luồng quarantine hiện có.
    """
    if valid.empty or not set(KEYS + ["create_time"]).issubset(valid.columns):
        return valid, valid.iloc[0:0].assign(rejection_reasons=pd.Series(dtype="object"))
    frame = valid.copy()
    created = _second(frame["create_time"])
    keyed = frame[KEYS].notna().all(axis=1) & created.notna()
    anchors = (
        pd.DataFrame({"order_id": frame.loc[keyed, "order_id"].astype(str),
                      "platform": frame.loc[keyed, "platform"].astype(str),
                      "batch_create_time": created[keyed]})
        .groupby(KEYS, as_index=False)["batch_create_time"].min()
    )
    stored = lookup(anchors[KEYS])
    if not stored.empty:
        stored = stored.assign(order_id=stored["order_id"].astype(str), platform=stored["platform"].astype(str),
                               stored_create_time=_second(stored["stored_create_time"]))
        anchors = anchors.merge(stored, on=KEYS, how="left")
        anchors["anchor"] = anchors["stored_create_time"].fillna(anchors["batch_create_time"])
    else:
        anchors["anchor"] = anchors["batch_create_time"]
    probe = pd.DataFrame({"order_id": frame["order_id"].astype(str), "platform": frame["platform"].astype(str)},
                         index=frame.index)
    anchor = pd.to_datetime(probe.merge(anchors[[*KEYS, "anchor"]], on=KEYS, how="left")["anchor"])
    anchor.index = frame.index
    conflict = keyed & anchor.notna() & (created != anchor)
    rejected = frame[conflict].copy()
    rejected["rejection_reasons"] = CONFLICT_REASON
    return frame[~conflict].copy(), rejected
