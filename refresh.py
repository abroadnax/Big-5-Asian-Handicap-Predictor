# refresh.py
import os
import re
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

def redacted(url: str) -> str:
    if not url:
        return ""
    return re.sub(r"://([^:]+):([^@]+)@", "://***:***@", url)

def db_engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        LOG.error("DATABASE_URL is not set. Set it in Render -> Environment.")
        sys.exit(2)
    LOG.info("Using DB: %s", redacted(url))
    return create_engine(url, pool_pre_ping=True)

def run_manage_refresh():
    """
    Calls your existing manage.py 'refresh' command in-process.
    If your manage.py expects CLI args, we simulate them.
    """
    LOG.info("Running manage.py refresh")
    # Fake argv for manage.py if it reads sys.argv
    argv_old = sys.argv[:]
    try:
        sys.argv = ["manage.py", "refresh"]
        # This executes your manage.py as if run from CLI.
        runpy.run_path("manage.py", run_name="__main__")
    finally:
        sys.argv = argv_old

def db_check():
    """
    Prove there is data in the DB *and* some in the next 7 days (UTC).
    Fail the job loudly if not.
    """
    eng = db_engine()
    with eng.begin() as con:
        # If your table is named differently, change 'predictions' here.
        # The date filter uses ::date to avoid timezone confusion.
        row = con.execute(text("""
            SELECT
              COUNT(*)                                  AS total,
              MIN(match_date)                            AS min_dt,
              MAX(match_date)                            AS max_dt,
              COUNT(*) FILTER (
                WHERE match_date::date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
              )                                          AS upcoming7
            FROM predictions
        """)).one()
        total, min_dt, max_dt, upcoming7 = row
        LOG.info("DB CHECK: total=%s min=%s max=%s upcoming7=%s", total, min_dt, max_dt, upcoming7)

        # Hard-fail if nothing usable was written, so Render marks this job FAILED.
        if not total or int(upcoming7 or 0) == 0:
            LOG.error("Refresh produced 0 usable rows (total=%s, upcoming7=%s).", total, upcoming7)
            sys.exit(1)

def print_db_host():
    u = os.getenv("DATABASE_URL", "")
    host = ""
    with suppress(Exception):
        host = re.sub(r".*@", "", u).split("?")[0]
    LOG.info("DB host: %s", host or "(unknown)")

if __name__ == "__main__":
    LOG.info("Starting refresh job")
    print_db_host()
    run_manage_refresh()
    LOG.info("manage.py finished successfully (in-process)")
    db_check()
    LOG.info("Refresh job complete")
