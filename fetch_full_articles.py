import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from newspaper import Article

from sqlalchemy import create_engine, Column, String, Text, DateTime, Boolean, text
from sqlalchemy.orm import DeclarativeBase, Session

# ── LOAD ENV ──────────────────────────────────────────────────────
load_dotenv()

DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    f"?host=localhost"
)

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=20,        # allow enough DB connections for parallel threads
    max_overflow=10,
)

# ── MODELS ────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    __tablename__ = "rss_articles"

    id           = Column(String,  primary_key=True)
    outlet       = Column(String)
    bias         = Column(String)
    country      = Column(String)
    title        = Column(Text)
    url          = Column(String,  unique=True)
    summary      = Column(Text)
    published    = Column(String)
    body         = Column(Text,    nullable=True)
    body_fetched = Column(Boolean, default=False)
    content_type = Column(String,  nullable=True)   # "news" / "opinion" / "analysis"
    fetched_at   = Column(DateTime)


# ── SETUP ─────────────────────────────────────────────────────────
def add_body_columns():
    """
    Adds missing columns safely.
    """
    with engine.connect() as conn:
        columns_to_check = [
            "ALTER TABLE rss_articles ADD COLUMN body TEXT",
            "ALTER TABLE rss_articles ADD COLUMN body_fetched BOOLEAN DEFAULT FALSE",
            "ALTER TABLE rss_articles ADD COLUMN content_type TEXT",
            
            # Just in case these are missing from your earlier DB schema!
            "ALTER TABLE rss_articles ADD COLUMN bias TEXT",
            "ALTER TABLE rss_articles ADD COLUMN country TEXT",
            "ALTER TABLE rss_articles ADD COLUMN fetched_at TIMESTAMP",
        ]
        
        for col_sql in columns_to_check:
            try:
                conn.execute(text(col_sql))
                conn.commit()
                col_name = col_sql.split("COLUMN ")[1].split(" ")[0]
                print(f"  Added column  : {col_name}")
            except Exception:
                # CRITICAL: Postgres aborts the transaction if the column already exists.
                # We MUST rollback to clear the error state before the next loop iteration.
                conn.rollback()


# ── CONTENT TYPE DETECTOR ─────────────────────────────────────────
def detect_content_type(url: str, title: str) -> str:
    """
    Detects whether an article is news, opinion, or analysis
    based on URL path patterns and title keywords.
    This is a weak signal — Bias Agent refines this later.
    """
    url_lower   = url.lower()
    title_lower = title.lower()

    opinion_url_signals = [
        "/opinion/", "/opinions/", "/commentary/",
        "/editorial/", "/op-ed/", "/viewpoint/", "/columns/",
    ]
    analysis_url_signals = [
        "/analysis/", "/explainer/", "/fact-check/",
        "/deep-dive/", "/in-depth/", "/explain/",
    ]
    opinion_title_signals = [
        "opinion:", "editorial:", "column:",
        "why i ", "we must", "it's time", "should be",
    ]

    if any(s in url_lower for s in opinion_url_signals):
        return "opinion"
    if any(s in url_lower for s in analysis_url_signals):
        return "analysis"
    if any(s in title_lower for s in opinion_title_signals):
        return "opinion"

    return "news"


# ── FULL TEXT EXTRACTOR ───────────────────────────────────────────
def extract_body(url: str) -> str | None:
    """
    Downloads a URL and extracts clean article body text.
    Returns None if:
      - Network error or timeout
      - Paywalled (text too short)
      - Bot-blocked (empty response)
    """
    try:
        article = Article(url, request_timeout=10)
        article.download()
        article.parse()

        body = article.text.strip()

        # Too short = paywall or failed extraction
        if len(body) < 200:
            return None

        return body

    except Exception as e:
        # Tip: Add logging here later if you want to track specific URL failures
        return None


# ── SINGLE ARTICLE WORKER ─────────────────────────────────────────
def fetch_and_save(article_id: str) -> dict:
    """
    Fetches and saves body text for one article.
    Each parallel thread runs this function independently.
    Opens its own DB session — safe for concurrent use.
    """
    with Session(engine) as session:
        article = session.query(RSSArticle).filter_by(id=article_id).first()

        if not article:
            return {"id": article_id, "success": False, "outlet": "?", "title": "?"}

        outlet = article.outlet
        title  = article.title

        body         = extract_body(article.url)
        content_type = detect_content_type(article.url, article.title)

        article.body         = body
        article.body_fetched = True
        article.content_type = content_type

        session.commit()

        return {
            "id":      article_id,
            "outlet":  outlet,
            "title":   title[:55],
            "success": body is not None,
            "chars":   len(body) if body else 0,
            "type":    content_type,
        }


# ── PARALLEL FETCHER ──────────────────────────────────────────────
def fetch_full_articles(batch_size: int = 200, max_workers: int = 10):
    """
    Fetches full body text for all pending articles using parallel threads.

    batch_size  : how many articles to process per run
    max_workers : how many articles fetched simultaneously
                  keep at 10 or below to avoid getting IP-blocked
    """
    print("=" * 58)
    print("  Full Article Body Fetcher  (parallel mode)")
    print(f"  Started : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Workers : {max_workers} parallel threads")
    print("=" * 58)

    # ── Get IDs of articles not yet processed ─────────────────────
    with Session(engine) as session:
        pending_ids = [
            row.id for row in
            session.query(RSSArticle.id)
            .filter(RSSArticle.body_fetched.is_(False))
            .limit(batch_size)
            .all()
        ]

    total = len(pending_ids)
    print(f"\n  Pending articles : {total}")

    if total == 0:
        print("\n  Nothing to do — all articles already processed.")
        print_db_summary()
        return

    print(f"  Processing {total} articles with {max_workers} threads...\n")
    print(f"  {'#':<8} {'STATUS':<6} {'OUTLET':<20} {'CHARS':>6}  TITLE")
    print(f"  {'─'*8} {'─'*6} {'─'*20} {'─'*6}  {'─'*30}")

    # ── Run in parallel ───────────────────────────────────────────
    success = 0
    failed  = 0
    done    = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_save, aid): aid
            for aid in pending_ids
        }

        for future in as_completed(futures):
            result = future.result()
            done  += 1

            if result["success"]:
                success += 1
                print(
                    f"  [{done}/{total}]"
                    f"  ✓ "
                    f"  {result['outlet']:<20}"
                    f"  {result['chars']:>6} chars"
                    f"  {result['title']}..."
                )
            else:
                failed += 1
                print(
                    f"  [{done}/{total}]"
                    f"  ✗ "
                    f"  {result.get('outlet','?'):<20}"
                    f"  {'blocked':>6}      "
                    f"  {result.get('title','?')}..."
                )

    # ── Print results ─────────────────────────────────────────────
    print(f"\n{'=' * 58}")
    print("  BATCH COMPLETE")
    print(f"{'=' * 58}")
    print(f"  Successfully extracted : {success}")
    print(f"  Failed / blocked       : {failed}")
    print(f"  Total processed        : {done}")
    print(f"{'=' * 58}")

    print_db_summary()


# ── DB SUMMARY ────────────────────────────────────────────────────
def print_db_summary():
    """Prints how many articles have body text per outlet."""
    outlets = ["The Hindu", "Times of India", "The Quint", "BBC India"]

    with Session(engine) as session:
        print(f"\n  {'─' * 52}")
        print(f"  {'OUTLET':<22} {'WITH BODY':>10} {'WITHOUT':>10} {'TOTAL':>8}")
        print(f"  {'─' * 52}")

        grand_total      = 0
        grand_with_body  = 0

        for outlet in outlets:
            total     = session.query(RSSArticle).filter_by(outlet=outlet).count()
            with_body = (
                session.query(RSSArticle)
                .filter_by(outlet=outlet)
                .filter(RSSArticle.body.isnot(None))
                .count()
            )
            without_body = total - with_body
            grand_total     += total
            grand_with_body += with_body

            print(f"  {outlet:<22} {with_body:>10} {without_body:>10} {total:>8}")

        print(f"  {'─' * 52}")
        print(f"  {'TOTAL':<22} {grand_with_body:>10} {grand_total - grand_with_body:>10} {grand_total:>8}")
        print(f"  {'─' * 52}\n")


# ── RUN ───────────────────────────────────────────────────────────
if __name__ == "__main__":

    print("\nChecking / adding columns...")
    add_body_columns()
    print()

    fetch_full_articles(
        batch_size=3000,    # process up to 300 articles per run
        max_workers=10,    # 10 simultaneous downloads
                           # reduce to 5 if you see many failures
    )