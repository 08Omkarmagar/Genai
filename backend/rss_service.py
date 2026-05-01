"""
rss_service.py
==============
Standalone scheduler / CLI wrapper for the RSS feed collector.
All models, engine, and outlet config are imported from the canonical
modules (models.py, database.py, outlets.py) — no duplicate definitions.

Can be used two ways:
  1. Standalone : python rss_service.py [--schedule] [--hours N]
  2. Via import  : call start_background_scheduler() from app startup
"""

import time
import logging
import threading

from sqlalchemy.orm import Session

from database import engine
from models import RSSArticle, init_db
from outlets import INDIAN_OUTLETS
from rss_fetcher import run_full_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("feeds.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("FeedCollector")


def print_summary():
    """Prints article counts per outlet from the database."""
    with Session(engine) as session:
        log.info(f"\n{'=' * 55}")
        log.info("  DATABASE SUMMARY")
        log.info(f"{'=' * 55}")
        log.info(f"  {'OUTLET':<26} {'BIAS':^8} {'ARTICLES':>8}")
        log.info(f"  {'-'*26} {'-'*8} {'-'*8}")

        grand_total = 0
        for lean in ("right", "center", "left"):
            for outlet_name, info in INDIAN_OUTLETS.items():
                if info["bias"] != lean:
                    continue
                count = session.query(RSSArticle).filter_by(
                    outlet=outlet_name).count()
                if count > 0:
                    log.info(f"  {outlet_name:<26} {lean:^8} {count:>8}")
                    grand_total += count

        log.info(f"  {'-'*26} {'-'*8} {'-'*8}")
        log.info(f"  {'TOTAL':<26} {'':^8} {grand_total:>8}")
        log.info(f"{'=' * 55}\n")


_scheduler_started = False


def start_background_scheduler(interval_hours: int = 4):
    """
    Starts the feed collector in a background daemon thread.
    Designed to be called ONCE from app startup.
    """
    global _scheduler_started

    if _scheduler_started:
        log.info("Scheduler already running — skipping duplicate start.")
        return

    _scheduler_started = True
    interval_seconds = interval_hours * 3600

    def _loop():
        log.info(
            f"[Scheduler] Started — will collect every {interval_hours}h.")
        while True:
            try:
                run_full_pipeline()
                print_summary()
            except Exception as e:
                log.error(f"[Scheduler] Run failed: {e}")
            log.info(f"[Scheduler] Next run in {interval_hours}h.")
            time.sleep(interval_seconds)

    thread = threading.Thread(target=_loop, name="FeedScheduler", daemon=True)
    thread.start()
    log.info("[Scheduler] Background thread started.")


def main():
    """Run once immediately (default) or on a schedule with --schedule flag."""
    import argparse

    parser = argparse.ArgumentParser(description="NewsHere RSS Feed Collector")
    parser.add_argument(
        "--schedule", action="store_true",
        help="Keep running on a schedule (default: every 4 hours)",
    )
    parser.add_argument(
        "--hours", type=int, default=4,
        help="Hours between scheduled runs (default: 4)",
    )
    args = parser.parse_args()

    init_db()

    if args.schedule:
        log.info(
            f"Schedule mode — running every {args.hours}h. Ctrl+C to stop.")
        while True:
            try:
                run_full_pipeline()
                print_summary()
            except Exception as e:
                log.error(f"Run failed: {e}")
            log.info(f"Sleeping {args.hours}h...")
            time.sleep(args.hours * 3600)
    else:
        run_full_pipeline()
        print_summary()


if __name__ == "__main__":
    main()
