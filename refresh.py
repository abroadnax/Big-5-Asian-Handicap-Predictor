# refresh.py (no Postgres required)
import os
import sys
import runpy
import logging
from contextlib import suppress
from sqlalchemy import create_engine, text

LOG = logging.getLogger("refresh")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Always use a local SQLite database
DB_URL = "sqlite:///app.db"

def db_engine():
    """Always use local SQLite for testing/offline runs."""
    LOG.info("Using local SQLite DB: app.db")
    return create_engine(DB_URL, pool_pre_ping=True)

def run_manage_refresh():
    """
    Runs your manage.py 'refresh' command directly.
    If manage.py reads CLI args, we simulate them.
    """
    LOG.info("Running manage.py refresh (local mode)")
    argv_old = sys.argv[:]
    try:
        sys.argv = ["manage.py", "refresh"]
        runpy.run_path("manage.py", run_name="__main__")
    finally:
        sys.argv = argv_old

def db_check():
    """
    Optional integrity check — skip or make it soft-fail for local use.
    """
    if os.getenv("SKIP_DB_CHECK") == "1":
        LOG.warning("Skipping DB check (SKIP_DB_CHECK=1)")
        return

    eng = db_engine()
    with eng.begin() as con:
        try:
            row = con.execute(text("""
                SELECT
                  COUNT(*) AS total,
                  MIN(match_date) AS min_dt,
                  MAX(match_date) AS max_dt
                FROM predictions
            """)).one()
            LOG.info("DB CHECK: total=%s min=%s max=%s", *row)
        except Exception as e:
            LOG.warning("DB check skipped — table may not exist yet: %s", e)

def print_db_host():
    LOG.info("Local SQLite file: app.db")

if __name__ == "__main__":
    LOG.info("Starting refresh job (offline mode)")
    print_db_host()
    run_manage_refresh()
    LOG.info("manage.py finished successfully (in-process)")
    db_check()
    LOG.info("Refresh job complete")
