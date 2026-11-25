"""Tự xác nhận coverage ngày (quyết định 16/11/2026): chỉ khi ngày đã qua, hết object chờ và lag Kafka = 0."""
from datetime import date
from unittest import mock

import pytest

from datetime import timedelta

from warehouse.etl.coverage import confirm_completed_days, volume_plausible

DAYS = [date(2026, 11, 14), date(2026, 11, 15)]


def connection(days):
    conn = mock.MagicMock()
    conn.execute.return_value.scalars.return_value = iter(days)
    return conn


def inserted(conn):
    calls = [c for c in conn.execute.call_args_list if "INSERT INTO Warehouse_Daily_Coverage" in str(c.args[0])]
    return [row["day"] for c in calls for row in c.args[1]]


def test_confirms_past_days_when_no_backlog_and_no_lag():
    conn = connection(DAYS)
    assert confirm_completed_days(conn, date(2026, 11, 16), 0, lambda: 0) == DAYS
    assert inserted(conn) == DAYS


@pytest.mark.parametrize("pending, lag", [(3, 0), (0, 5)])
def test_backlog_or_kafka_lag_blocks_confirmation(pending, lag):
    conn = connection(DAYS)
    assert confirm_completed_days(conn, date(2026, 11, 16), pending, lambda: lag) == []
    assert inserted(conn) == []


def test_unreadable_lag_never_confirms():
    def broken():
        raise ConnectionError("Kafka không trả lời")
    conn = connection(DAYS)
    with pytest.raises(ConnectionError):
        confirm_completed_days(conn, date(2026, 11, 16), 0, broken)
    assert inserted(conn) == []


def test_volume_check_holds_days_when_source_stopped():
    """Hồi quy 16/11/2026: 01–14/11 producer tắt (0 đơn) từng được xác nhận là đủ dữ liệu."""
    last_normal = date(2026, 10, 31)
    volumes = {last_normal - timedelta(days=i): 400 for i in range(28)}
    assert not volume_plausible(date(2026, 11, 3), {**volumes, date(2026, 11, 3): 0})
    assert not volume_plausible(date(2026, 11, 15), {**volumes, date(2026, 11, 15): 74})   # producer chạy vài phút
    assert volume_plausible(date(2026, 11, 1), {**volumes, date(2026, 11, 1): 300})
    assert volume_plausible(date(2026, 1, 1), {date(2026, 1, 1): 3})                       # chưa đủ tham chiếu


def test_lag_is_not_read_when_nothing_to_confirm():
    reader = mock.Mock(return_value=0)
    assert confirm_completed_days(connection([]), date(2026, 11, 16), 0, reader) == []
    reader.assert_not_called()
