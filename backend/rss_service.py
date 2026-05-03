"""
rss_service.py — Background scheduler and CLI for the NewsHere ingestion pipeline.

This module acts as the operational wrapper for the RSS collection system. It can be
executed as a standalone script for periodic updates or imported as a background 
service for the main FastAPI application.

Capabilities:
- Standalone CLI: Manual or scheduled execution via command-line flags.
- Background Scheduling: Thread-safe daemon for automated, periodic collection.
- Health Monitoring: Generates summarized article counts per outlet after every run.
"""

import time
import logging
import threading
import argparse
from sqlalchemy.orm import Session
from database import engine
from models import RSSArticle, init_db
from outlets import INDIAN_OUTLETS
from rss_fetcher import run_full_pipeline

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(), logging.FileHandler("feeds.log", encoding="utf-8")],
)
log = logging.getLogger("FeedCollector")

# ---------------------------------------------------------------------------
# Monitoring Utilities
# ---------------------------------------------------------------------------

def print_summary():
    """
    Logs a formatted table showing the current distribution of articles in the DB.
    Groups results by political bias and outlet name.
    """
    with Session(engine) as session:
        log.info(f"\n{'=' * 55}")
        log.info("  CURRENT DATABASE STATE (RSSArticles)")
        log.info(f"{'=' * 55}")
        log.info(f"  {'OUTLET':<26} {'BIAS':^8} {'ARTICLES':>8}")
        log.info(f"  {'-'*26} {'-'*8} {'-'*8}")

        grand_total = 0
        for lean in ("right", "center", "left"):
            for name, info in INDIAN_OUTLETS.items():
                if info["bias"] == lean:
                    count = session.query(RSSArticle).filter_by(outlet=name).count()
                    if count > 0:
                        log.info(f"  {name:<26} {lean:^8} {count:>8}")
                        grand_total += count

        log.info(f"  {'-'*26} {'-'*8} {'-'*8}")
        log.info(f"  {'SYSTEM TOTAL':<26} {'':^8} {grand_total:>8}")
        log.info(f"{'=' * 55}\n")

# ---------------------------------------------------------------------------
# Background Scheduling Logic
# ---------------------------------------------------------------------------

_scheduler_started = False

def start_background_scheduler(interval_hours: int = 4):
    """
    Initializes the feed collector as a background daemon thread.
    This is intended to be called during the FastAPI application's startup phase.
    """
    global _scheduler_started
    if _scheduler_started:
        log.info("Scheduler already active.")
        return

    _scheduler_started = True
    interval_seconds = interval_hours * 3600

    def _loop():
        log.info(f"[Scheduler] Background loop active (Interval: {interval_hours}h).")
        while True:
            try:
                run_full_pipeline()
                print_summary()
            except Exception as e:
                log.error(f"[Scheduler] Pipeline error: {e}")
            
            log.info(f"[Scheduler] Next cycle scheduled in {interval_hours}h.")
            time.sleep(interval_seconds)

    # Use a daemon thread to ensure it exits automatically when the main process stops
    thread = threading.Thread(target=_loop, name="FeedScheduler", daemon=True)
    thread.start()
    log.info("[Scheduler] Thread spawned successfully.")

# ---------------------------------------------------------------------------
# CLI Execution Entry Point
# ---------------------------------------------------------------------------

def main():
    """
    Command-line interface for the ingestion pipeline.
    Usage: python rss_service.py [--schedule] [--hours N]
    """
    parser = argparse.ArgumentParser(description="NewsHere RSS Ingestion Service")
    parser.add_argument("--schedule", action="store_true", help="Keep running on a periodic schedule")
    parser.add_argument("--hours", type=int, default=4, help="Hours between scheduled runs (default: 4)")
    args = parser.parse_args()

    init_db()

    if args.schedule:
        log.info(f"Schedule mode active: Fetching every {args.hours}h. Use Ctrl+C to exit.")
        while True:
            try:
                run_full_pipeline()
                print_summary()
            except Exception as e:
                log.error(f"Execution failure: {e}")
            time.sleep(args.hours * 3600)
    else:
        # Run-once mode
        run_full_pipeline()
        print_summary()

if __name__ == "__main__":
    main()
