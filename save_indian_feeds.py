import feedparser
import time
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, String, Text, DateTime, Integer
)
from sqlalchemy.orm import DeclarativeBase, Session

# ── LOAD ENV VARIABLES ────────────────────────────────────────────
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "newsdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── DATABASE SETUP ────────────────────────────────────────────────
engine = create_engine(DATABASE_URL, echo=False)


class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    """
    One row per article fetched from an RSS feed.
    At this stage we only store what RSS gives us —
    no full article body yet (that comes in the next agent).
    """
    __tablename__ = "rss_articles"

    id = Column(String,   primary_key=True, default=lambda: str(uuid.uuid4()))
    outlet = Column(String,   nullable=False)        # e.g. "The Hindu"
    # "left" / "center" / "right"
    bias = Column(String,   nullable=False)
    country = Column(String,   default="IN")          # India-focused feeds
    title = Column(Text,     nullable=False)
    # unique prevents duplicate saves
    url = Column(String,   unique=True)
    # RSS snippet (not full article)
    summary = Column(Text)
    # raw date string from feed
    published = Column(String)
    fetched_at = Column(DateTime, default=datetime.utcnow)


class FetchLog(Base):
    """
    Logs every fetch run — how many articles were saved, 
    how many were skipped (duplicates), and any errors.
    Useful for debugging and monitoring.
    """
    __tablename__ = "fetch_logs"

    id = Column(String,  primary_key=True, default=lambda: str(uuid.uuid4()))
    outlet = Column(String)
    run_at = Column(DateTime, default=datetime.utcnow)
    articles_new = Column(Integer,  default=0)    # newly inserted
    articles_skip = Column(Integer,  default=0)    # skipped (already in DB)
    status = Column(String)                 # "success" or "failed"
    # filled if status = "failed"
    error_message = Column(Text)


def init_db():
    """Creates tables if they don't already exist."""
    Base.metadata.create_all(engine)
    print("Database tables ready.\n")


# ── OUTLET REGISTRY ───────────────────────────────────────────────
# Indian news outlets with their RSS URLs and AllSides-style bias labels.
# Indian outlets don't have official AllSides ratings, so these are
# based on academic consensus (labels are weak priors, not verdicts).

INDIAN_OUTLETS = {
    "The Hindu": {
        "bias":    "left",
        "country": "IN",
        "feeds": [
            "https://www.thehindu.com/news/national/?service=rss",
            "https://www.thehindu.com/opinion/?service=rss",
        ]
    },
    "Times of India": {
        "bias":    "center",
        "country": "IN",
        "feeds": [
            "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
            "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
        ]
    },
    "The Quint": {
        "bias":    "left",
        "country": "IN",
        "feeds": [
            "https://www.thequint.com/feed",
        ]
    },
    "BBC India": {
        "bias":    "center",
        "country": "IN",
        "feeds": [
            "https://feeds.bbci.co.uk/news/world/asia/india/rss.xml",
        ]
    },
}


# ── CORE FUNCTIONS ────────────────────────────────────────────────

def article_exists(session: Session, url: str) -> bool:
    """Check if an article URL is already in the database."""
    return session.query(RSSArticle).filter_by(url=url).first() is not None


def save_article(session: Session, data: dict) -> bool:
    """
    Insert one article. Returns True if saved, False if skipped (duplicate).
    Uses the URL as the unique key — same article won't be saved twice
    even if you run the script multiple times a day.
    """
    if not data.get("url"):
        return False  # skip articles with no URL

    if article_exists(session, data["url"]):
        return False  # already in database, skip

    article = RSSArticle(
        outlet=data["outlet"],
        bias=data["bias"],
        country=data["country"],
        title=data["title"],
        url=data["url"],
        summary=data["summary"],
        published=data["published"],
    )
    session.add(article)
    return True


def fetch_outlet(outlet_name: str, outlet_info: dict) -> None:
    """
    Fetches all RSS feeds for one outlet and saves new articles to the DB.
    Logs the result to the fetch_logs table.
    """
    print(f"\n{'─' * 50}")
    print(f"Outlet : {outlet_name}  [{outlet_info['bias']}]")
    print(f"Feeds  : {len(outlet_info['feeds'])} feed(s)")

    total_new = 0
    total_skip = 0
    error_msg = None
    status = "success"

    try:
        with Session(engine) as session:
            for feed_url in outlet_info["feeds"]:
                print(f"\n  Parsing: {feed_url}")

                feed = feedparser.parse(feed_url)

                if not feed.entries:
                    print(
                        f"  [!] No entries found — feed may be unavailable or empty")
                    continue

                print(f"  Found {len(feed.entries)} entries in feed")

                for entry in feed.entries:
                    data = {
                        "outlet":    outlet_name,
                        "bias":      outlet_info["bias"],
                        "country":   outlet_info["country"],
                        "title":     entry.get("title", "").strip(),
                        "url":       entry.get("link", "").strip(),
                        "summary":   entry.get("summary", "").strip(),
                        "published": entry.get("published", "").strip(),
                    }

                    # Skip entries with no title or URL
                    if not data["title"] or not data["url"]:
                        continue

                    saved = save_article(session, data)

                    if saved:
                        total_new += 1
                        print(f"  [+] {data['title'][:65]}...")
                    else:
                        total_skip += 1

                time.sleep(0.5)  # polite delay between feed requests

            session.commit()

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        print(f"\n  [ERROR] {outlet_name} failed: {e}")

    # ── Write to fetch log ─────────────────────────────────────────
    with Session(engine) as log_session:
        log = FetchLog(
            outlet=outlet_name,
            articles_new=total_new,
            articles_skip=total_skip,
            status=status,
            error_message=error_msg,
        )
        log_session.add(log)
        log_session.commit()

    print(
        f"\n  Result → Saved: {total_new}  |  Skipped (duplicates): {total_skip}  |  Status: {status}")


def print_summary():
    """Prints a quick count of what's in the database after the run."""
    with Session(engine) as session:
        print(f"\n{'═' * 50}")
        print("DATABASE SUMMARY")
        print(f"{'═' * 50}")

        for outlet_name in INDIAN_OUTLETS:
            count = session.query(RSSArticle).filter_by(
                outlet=outlet_name).count()
            print(f"  {outlet_name:<22} {count:>4} articles")

        total = session.query(RSSArticle).count()
        print(f"{'─' * 50}")
        print(f"  {'TOTAL':<22} {total:>4} articles")
        print(f"{'═' * 50}\n")


# ── MAIN ──────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  Indian News RSS Feed Collector")
    print(
        f"  Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 50)

    # Step 1: Make sure tables exist
    init_db()

    # Step 2: Fetch each outlet
    for outlet_name, outlet_info in INDIAN_OUTLETS.items():
        fetch_outlet(outlet_name, outlet_info)
        time.sleep(1)  # delay between outlets

    # Step 3: Print what we collected
    print_summary()


run_collection = main
if __name__ == "__main__":
    main()
