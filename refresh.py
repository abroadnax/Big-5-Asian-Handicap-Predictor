from __future__ import annotations
import os
import sys
import runpy
import logging
import subprocess
from datetime import datetime, timezone

from app import create_app
from models import db  # ensures SQLAlchemy is bound once app context is active

# ---------- logging ----------
logging.basicConfig(
    level=os.environ.get("LOGLEVEL", "INFO"),
    format="%(asctime)s %(levelname)s: %(message)s",
)
log = logging.getLogger("refresh")


def run_manage_py_inprocess(args: list[str]) -> None:
    """
    Execute manage.py in-process with runpy so it shares the Flask app context & DB session.
    """
    original_argv = sys.argv[:]
    try:
        sys.argv = ["manage.py"] + args
        log.info("Running manage.py %s", " ".join(args))
        runpy.run_path("manage.py", run_name="__main__")
        log.info("manage.py finished successfully (in-process)")
    finally:
        sys.argv = original_argv


def run_manage_py_subprocess(args: list[str]) -> None:
    """
    Fallback: execute manage.py as a subprocess (isolation if runpy path fails).
    """
    cmd = [sys.executable, "manage.py"] + args
    log.info("Running subprocess: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)
    log.info("manage.py finished successfully (subprocess)")


def refresh_predictions() -> None:
    """
    Entrypoint invoked by cron.
    MANAGE_ARGS env var controls which manage.py command runs.
      - e.g., MANAGE_ARGS="refresh"  -> python manage.py refresh
      - e.g., MANAGE_ARGS="refresh --days 7"
    If MANAGE_ARGS is unset, we try:
      1) python manage.py refresh
      2) python manage.py
    """
    manage_args = os.environ.get("MANAGE_ARGS", "").strip()
    try_order: list[list[str]] = [manage_args.split()] if manage_args else [["refresh"], []]

    for args in try_order:
        try:
            run_manage_py_inprocess(args)
            return
        except SystemExit as e:
            # argparse-style error codes -> try next pattern
            if getattr(e, "code", 0) not in (0, None):
                log.warning("manage.py %s exited with code %s (in-process). Trying next pattern…", args, e.code)
            else:
                return
        except Exception:
            log.exception("In-process execution failed. Falling back to subprocess for args: %s", args)
            run_manage_py_subprocess(args)
            return

    # If we got here, both patterns failed
    raise RuntimeError("Failed to execute manage.py via all tried patterns.")


if __name__ == "__main__":
    log.info("Starting refresh job at %s", datetime.now(timezone.utc).isoformat())
    app = create_app()
    with app.app_context():
        refresh_predictions()
        # If manage.py made DB changes using the same db.session (in-process path),
        # ensure commit. (Subprocess path commits on its own.)
        try:
            db.session.commit()
        except Exception:
            pass
    log.info("Refresh job complete.")
