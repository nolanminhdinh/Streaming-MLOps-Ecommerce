"""Capture actual exporter SELECT statements for PostgreSQL integration checks."""
import json
import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from powerbi.export_powerbi_dataset import export_powerbi_data

queries = []
def capture(conn, sql):
    queries.append(sql)
    return [{"units_sold": 1}]

with mock.patch("sqlalchemy.create_engine"), \
     mock.patch("powerbi.export_powerbi_dataset._install_views"), \
     mock.patch("powerbi.export_powerbi_dataset._query", side_effect=capture), \
     mock.patch("powerbi.export_powerbi_dataset._write_csv", return_value=1):
    export_powerbi_data(str(Path(sys.argv[1]).parent / "query-collection"))
Path(sys.argv[1]).write_text(json.dumps(queries), encoding="utf-8")
