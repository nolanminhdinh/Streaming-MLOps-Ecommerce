"""Install the BI contract after public source views have been installed."""
from pathlib import Path


def install_bi_schema(conn):
    conn.exec_driver_sql(Path(__file__).with_name('bi_schema.sql').read_text(encoding='utf-8'))
