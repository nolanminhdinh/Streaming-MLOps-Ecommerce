"""One PostgreSQL session lock coordinates scheduled ETL and explicit backfills."""
from contextlib import contextmanager
from sqlalchemy import text

PIPELINE_LOCK_KEY = 2026110801


class PipelineBusy(RuntimeError):
    pass


@contextmanager
def pipeline_lock(engine):
    with engine.connect() as connection:
        acquired = connection.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": PIPELINE_LOCK_KEY}).scalar_one()
        if not acquired:
            raise PipelineBusy("Another ETL/backfill is running; no checkpoint or data has been changed")
        try:
            yield
        finally:
            connection.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": PIPELINE_LOCK_KEY})

