"""
save_indian_feeds.py
====================
Self-contained RSS feed collector for 33 Indian news outlets.
Covers right-leaning, left-leaning, and center publications.

Models are defined inline — no separate models.py needed.

Can be used two ways:
  1. Standalone : python save_indian_feeds.py [--schedule] [--hours N]
  2. Via Flask  : import start_background_scheduler and call it in app.py
"""

import feedparser
import time
import os
import uuid
import logging
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine,
    Column, String, Text, DateTime, Integer, Boolean,
)
from sqlalchemy.orm import DeclarativeBase, Session

# ── LOAD ENV VARIABLES ────────────────────────────────────────────
load_dotenv()

DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "newsdb")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── LOGGING ───────────────────────────────────────────────────────
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

# ── DATABASE SETUP ────────────────────────────────────────────────
engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=5,
    max_overflow=2,
    pool_pre_ping=True,
)


# ── MODELS ────────────────────────────────────────────────────────

class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    """
    One row per article fetched from an RSS feed.
    At this stage we only store what RSS gives us —
    no full article body yet (that comes in the next agent).
    """
    __tablename__ = "rss_articles"

    id          = Column(String,   primary_key=True, default=lambda: str(uuid.uuid4()))
    outlet      = Column(String,   nullable=False)          # e.g. "The Hindu"
    bias        = Column(String,   nullable=False)          # "left" / "center" / "right"
    country     = Column(String,   default="IN")            # India-focused feeds
    title       = Column(Text,     nullable=False)
    url         = Column(String,   unique=True)             # unique prevents duplicate saves
    summary     = Column(Text)                              # RSS snippet (not full article)
    published   = Column(String)                            # raw date string from feed
    fetched_at  = Column(DateTime, default=datetime.utcnow)
    body_fetched = Column(Boolean, default=False)           # set True after full-body fetch


class FetchLog(Base):
    """
    Logs every fetch run — how many articles were saved,
    how many were skipped (duplicates), and any errors.
    Useful for debugging and monitoring.
    """
    __tablename__ = "fetch_logs"

    id            = Column(String,  primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id        = Column(String)                          # groups all outlets in one run
    outlet        = Column(String)
    run_at        = Column(DateTime, default=datetime.utcnow)
    articles_new  = Column(Integer,  default=0)            # newly inserted
    articles_skip = Column(Integer,  default=0)            # skipped (already in DB)
    status        = Column(String)                         # "success" or "failed"
    error_message = Column(Text)                           # filled if status = "failed"


def init_db():
    """Creates tables if they don't already exist. Safe to call multiple times."""
    Base.metadata.create_all(engine)
    log.info("Database tables ready.")


# ── OUTLET REGISTRY ───────────────────────────────────────────────
# 33 Indian news outlets with RSS URLs and AllSides-style bias labels.
# Indian outlets don't have official AllSides ratings — labels are
# based on academic consensus (weak priors, not verdicts).

INDIAN_OUTLETS = {

    # ── RIGHT-LEANING ─────────────────────────────────────────────
    "OpIndia": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.opindia.com/feed/",
        ],
    },
    "Swarajya": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://swarajyamag.com/feed",
        ],
    },
    "Organiser": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://organiser.org/feed/",
        ],
    },
    "Republic World": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.republicworld.com/rss.xml",
        ],
    },
    "DNA India": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.dnaindia.com/feeds/india.xml",
            "https://www.dnaindia.com/feeds/politics.xml",
        ],
    },
    "The Sunday Guardian": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.sundayguardianlive.com/feed",
        ],
    },
    "Zee News": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://zeenews.india.com/rss/india-national-news.xml",
            "https://zeenews.india.com/rss/politics-news.xml",
        ],
    },
    "India Today": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.indiatoday.in/rss/1206578",
            "https://www.indiatoday.in/rss/home",
        ],
    },
    "Firstpost": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.firstpost.com/rss/india.xml",
            "https://www.firstpost.com/rss/politics.xml",
        ],
    },
    "News18": {
        "bias": "right", "country": "IN",
        "feeds": [
            "https://www.news18.com/rss/india.xml",
            "https://www.news18.com/rss/politics.xml",
        ],
    },

    # ── LEFT-LEANING ──────────────────────────────────────────────
    "The Wire": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://thewire.in/rss/politics/feed",
            "https://thewire.in/rss/law/feed",
            "https://thewire.in/rss/media/feed",
        ],
    },
    "Scroll.in": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://scroll.in/feed",
        ],
    },
    "The Caravan": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://caravanmagazine.in/feed",
        ],
    },
    "Newslaundry": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://www.newslaundry.com/feed",
        ],
    },
    "The News Minute": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://www.thenewsminute.com/rss.xml",
        ],
    },
    "Article 14": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://article-14.com/feed",
        ],
    },
    "Alt News": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://www.altnews.in/feed/",
        ],
    },
    "Maktoob Media": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://maktoobmedia.com/feed/",
        ],
    },
    "The Leaflet": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://theleaflet.in/feed/",
        ],
    },
    "The Citizen": {
        "bias": "left", "country": "IN",
        "feeds": [
            "https://www.thecitizen.in/rss/feed/all",
        ],
    },

    # ── CENTER / MAINSTREAM ───────────────────────────────────────
    "The Hindu": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://www.thehindu.com/news/national/?service=rss",
            "https://www.thehindu.com/news/politics-and-government/?service=rss",
            "https://www.thehindu.com/opinion/?service=rss",
        ],
    },
    "Indian Express": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://indianexpress.com/feed/",
            "https://indianexpress.com/section/political-pulse/feed/",
        ],
    },
    # "Hindustan Times": {
    #     "bias": "center", "country": "IN",
    #     "feeds": [
    #         "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    #         "https://www.hindustantimes.com/feeds/rss/politics/rssfeed.xml",
    #     ],
    # },
    "NDTV": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://feeds.feedburner.com/ndtvnews-india-news",
            "https://feeds.feedburner.com/ndtvnews-top-stories",
        ],
    },
    "Times of India": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
            "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
        ],
    },
    "Business Standard": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://www.business-standard.com/rss/politics-current-affairs-10601.rss",
            "https://www.business-standard.com/rss/current-affairs-10605.rss",
        ],
    },
    "Economic Times": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://economictimes.indiatimes.com/rssfeedsdefault.cms",
            "https://economictimes.indiatimes.com/news/politics-and-nation/rssfeeds/28311565.cms",
        ],
    },
    "Deccan Herald": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://www.deccanherald.com/rss-feed/national.rss",
            "https://www.deccanherald.com/rss-feed/politics.rss",
        ],
    },
    "The Print": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://theprint.in/feed/",
            "https://theprint.in/category/politics/feed/",
        ],
    },
    "The Quint": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://www.thequint.com/feed",
        ],
    },
    "BBC India": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml",
        ],
    },
    "Reuters India": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://feeds.reuters.com/reuters/INtopNews",
        ],
    },
    "AP India": {
        "bias": "center", "country": "IN",
        "feeds": [
            "https://rsshub.app/apnews/topics/apf-india",
        ],
    },
}


# ── CORE FUNCTIONS ────────────────────────────────────────────────

def article_exists(session: Session, url: str) -> bool:
    """Check if an article URL is already in the database."""
    return session.query(RSSArticle).filter_by(url=url).first() is not None


def save_article(session: Session, data: dict) -> bool:
    """
    Insert one article. Returns True if saved, False if skipped (duplicate).
    Uses URL as the unique key — same article won't be saved twice
    even if you run the script multiple times a day.
    """
    if not data.get("url") or not data.get("title"):
        return False

    if article_exists(session, data["url"]):
        return False

    try:
        session.add(RSSArticle(
            outlet    = data["outlet"],
            bias      = data["bias"],
            country   = data["country"],
            title     = data["title"],
            url       = data["url"],
            summary   = data["summary"],
            published = data["published"],
            # body_fetched defaults to False via the model
        ))
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False  # race-condition duplicate — safe to skip


def fetch_outlet(outlet_name: str, outlet_info: dict, run_id: str) -> dict:
    """
    Fetches all RSS feeds for one outlet and saves new articles to the DB.
    Logs the result to the fetch_logs table.
    """
    log.info(f"\n{'─' * 55}")
    log.info(f"  Outlet : {outlet_name}  [{outlet_info['bias'].upper()}]")
    log.info(f"  Feeds  : {len(outlet_info['feeds'])} feed(s)")

    total_new  = 0
    total_skip = 0
    error_msg  = None
    status     = "success"

    try:
        with Session(engine) as session:
            for feed_url in outlet_info["feeds"]:
                log.info(f"\n    Parsing: {feed_url}")

                feed = feedparser.parse(feed_url)

                if not feed.entries:
                    log.warning("    [!] No entries found — feed may be empty or blocked")
                    continue

                log.info(f"    Found {len(feed.entries)} entries")

                for entry in feed.entries:
                    data = {
                        "outlet":    outlet_name,
                        "bias":      outlet_info["bias"],
                        "country":   outlet_info["country"],
                        "title":     entry.get("title",     "").strip(),
                        "url":       entry.get("link",      "").strip(),
                        "summary":   entry.get("summary",   "").strip(),
                        "published": entry.get("published", "").strip(),
                    }

                    saved = save_article(session, data)
                    if saved:
                        total_new += 1
                        log.info(f"    [+] {data['title'][:70]}")
                    else:
                        total_skip += 1

                time.sleep(0.5)  # polite delay between feeds

    except Exception as e:
        status    = "failed"
        error_msg = str(e)
        log.error(f"  [ERROR] {outlet_name}: {e}")

    # ── Write to fetch_logs ────────────────────────────────────────
    try:
        with Session(engine) as log_session:
            log_session.add(FetchLog(
                run_id        = run_id,
                outlet        = outlet_name,
                articles_new  = total_new,
                articles_skip = total_skip,
                status        = status,
                error_message = error_msg,
            ))
            log_session.commit()
    except Exception as e:
        log.warning(f"  Could not write fetch log: {e}")

    log.info(f"\n  Result → Saved: {total_new}  |  Skipped: {total_skip}  |  Status: {status}")

    return {"outlet": outlet_name, "new": total_new, "skip": total_skip, "status": status}


def print_summary():
    """Prints article counts per outlet from the database."""
    with Session(engine) as session:
        log.info(f"\n{'═' * 55}")
        log.info("  DATABASE SUMMARY")
        log.info(f"{'═' * 55}")
        log.info(f"  {'OUTLET':<26} {'BIAS':^8} {'ARTICLES':>8}")
        log.info(f"  {'─'*26} {'─'*8} {'─'*8}")

        grand_total = 0
        for lean in ("right", "center", "left"):
            for outlet_name, info in INDIAN_OUTLETS.items():
                if info["bias"] != lean:
                    continue
                count = session.query(RSSArticle).filter_by(outlet=outlet_name).count()
                if count > 0:
                    log.info(f"  {outlet_name:<26} {lean:^8} {count:>8}")
                    grand_total += count

        log.info(f"  {'─'*26} {'─'*8} {'─'*8}")
        log.info(f"  {'TOTAL':<26} {'':^8} {grand_total:>8}")
        log.info(f"{'═' * 55}\n")


# ── MAIN COLLECTION RUN ───────────────────────────────────────────

def run_collection():
    """
    Runs one full collection pass across all outlets.
    Called directly (standalone) or by the scheduler every N hours.
    """
    run_id  = str(uuid.uuid4())[:8]
    started = datetime.now(timezone.utc)

    log.info("=" * 55)
    log.info("  Indian News RSS Feed Collector")
    log.info(f"  Run ID  : {run_id}")
    log.info(f"  Started : {started.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info(f"  Outlets : {len(INDIAN_OUTLETS)}")
    log.info("=" * 55)

    results = []
    for outlet_name, outlet_info in INDIAN_OUTLETS.items():
        result = fetch_outlet(outlet_name, outlet_info, run_id)
        results.append(result)
        time.sleep(1)  # polite delay between outlets

    elapsed    = (datetime.now(timezone.utc) - started).seconds
    total_new  = sum(r["new"]  for r in results)
    total_skip = sum(r["skip"] for r in results)
    failed     = [r["outlet"] for r in results if r["status"] == "failed"]

    log.info("")
    log.info("=" * 55)
    log.info(f"  RUN COMPLETE — {elapsed}s elapsed")
    log.info(f"  New articles saved : {total_new}")
    log.info(f"  Duplicates skipped : {total_skip}")
    if failed:
        log.warning(f"  Failed outlets     : {', '.join(failed)}")
    log.info("=" * 55)

    print_summary()

    return total_new


# ── BACKGROUND SCHEDULER (used by Flask) ─────────────────────────
_scheduler_started = False


def start_background_scheduler(interval_hours: int = 4):
    """
    Starts the feed collector in a background daemon thread.
    Designed to be called ONCE from Flask app startup.
    """
    global _scheduler_started

    if _scheduler_started:
        log.info("Scheduler already running — skipping duplicate start.")
        return

    _scheduler_started = True
    interval_seconds   = interval_hours * 3600

    def _loop():
        log.info(f"[Scheduler] Started — will collect every {interval_hours}h.")
        while True:
            try:
                run_collection()
            except Exception as e:
                log.error(f"[Scheduler] Run failed: {e}")
            log.info(f"[Scheduler] Next run in {interval_hours}h.")
            time.sleep(interval_seconds)

    thread = threading.Thread(target=_loop, name="FeedScheduler", daemon=True)
    thread.start()
    log.info("[Scheduler] Background thread started.")


# ── STANDALONE ENTRY POINT ────────────────────────────────────────

def main():
    """Run once immediately (default) or on a schedule with --schedule flag."""
    import argparse

    parser = argparse.ArgumentParser(description="Indian News RSS Feed Collector")
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
        log.info(f"Schedule mode — running every {args.hours}h. Ctrl+C to stop.")
        while True:
            try:
                run_collection()
            except Exception as e:
                log.error(f"Run failed: {e}")
            log.info(f"Sleeping {args.hours}h...")
            time.sleep(args.hours * 3600)
    else:
        run_collection()


if __name__ == "__main__":
    main()