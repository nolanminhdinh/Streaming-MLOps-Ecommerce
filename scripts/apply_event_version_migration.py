"""Apply only additive event-version columns to an existing warehouse."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from warehouse.etl.load import get_engine


def main():
    script = (Path(__file__).resolve().parents[1] / "warehouse/ddl/03_event_versions.sql").read_text(encoding="utf-8")
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(script)
    finally:
        engine.dispose()
    print("Event-version migration applied; existing facts preserved.")


if __name__ == "__main__":
    main()
