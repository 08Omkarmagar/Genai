import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from newspaper import Article

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# FIX: import shared models instead of redefining them
from models import Base, RSSArticle

import time  # add this at the top if not already there

# ── LOAD ENV ──────────────────────────────────────────────────────
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")   # FIX: was hardcoded; also removed the broken ?host=localhost query param
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "newsdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# FIX: removed the malformed `?host=localhost` suffix which conflicted
# with the host already present in the URL and could cause connection errors.
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_size=20,
    max_overflow=10,
)


# ── CONTENT TYPE DETECTOR ─────────────────────────────────────────
def detect_content_type(url: str, title: str) -> str:
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
    try:
        article = Article(url, request_timeout=10)
        article.download()
        article.parse()

        body = article.text.strip()

        if len(body) < 200:
            return None

        return body

    except Exception:
        return None


# ── SINGLE ARTICLE WORKER ─────────────────────────────────────────

def fetch_and_save(article_id: str) -> dict:
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

        time.sleep(1)  # ← ADD THIS — 1 second delay per article

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
    print("=" * 58)
    print("  Full Article Body Fetcher  (parallel mode)")
    print(f"  Started : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Workers : {max_workers} parallel threads")
    print("=" * 58)

    # FIX: filter now correctly uses Boolean False — previously the DB stored
    # the string "false" (from save_indian_feeds) while this queried for Boolean False,
    # so zero articles were ever picked up. Unified to Boolean in models.py.
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
    with Session(engine) as session:
        outlets = [
            row[0] for row in
            session.query(RSSArticle.outlet).distinct().order_by(RSSArticle.outlet).all()
        ]

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
    fetch_full_articles(
        batch_size=3000,
        max_workers=20,
    )