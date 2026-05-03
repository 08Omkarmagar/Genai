"""
body_fetcher.py — Robust full-text extraction pipeline for news articles.

This module retrieves raw HTML from news URLs and extracts clean, readable text.
It uses a multi-stage approach (Trafilatura -> Newspaper3k) and includes
quality scoring to filter out non-news content like paywalls or cookie consent pages.

Anti-blocking measures included:
- Randomized 'human' delays between requests
- Browser-mimicking headers
- Parallel thread management with rate-pacing
"""

import uuid
import logging
import time
import random
import trafilatura
from datetime import datetime, timezone
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from newspaper import Article as NewspaperArticle, Config
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import engine
from models import RSSArticle, ArticleToRemove, FetchLog, init_db
from clustering_service import ClusteringService

# ---------------------------------------------------------------------------
# Configuration & Logging
# ---------------------------------------------------------------------------

# Logging setup to track extraction success and failures across runs
log = logging.getLogger("BodyFetcher")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    formatter = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in (
        logging.StreamHandler(),
        logging.FileHandler("logs/body_fetch.log", encoding="utf-8"),
    ):
        handler.setFormatter(formatter)
        log.addHandler(handler)

# Network and extraction constraints
CONNECT_TIMEOUT = 5
READ_TIMEOUT = 15
TIMEOUT = READ_TIMEOUT
MAX_FAIL_COUNT = 3  # Permanent ignore after N failures
MAX_BODY_BYTES = 5 * 1024 * 1024 # Reject pages over 5MB to avoid memory bloat
BATCH_SIZE = 3000

# User-Agent mimicking a modern browser to reduce bot-detection triggers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def human_delay():
    """
    Introduces a randomized pause between web requests.
    This simulates human-like browsing patterns and prevents IP-based rate limiting.
    """
    delay = random.uniform(3.0, 5.0)

    # Occasionally take a significantly longer pause to further randomize patterns
    if random.random() < 0.10:
        extra_pause = 1
        delay += extra_pause
        print(f"⏳ Taking a longer reading pause: {delay:.2f} seconds...")
    else:
        print(f"⏳ Waiting: {delay:.2f} seconds...")

    time.sleep(delay)

def _is_valid_url(url: str | None) -> bool:
    """
    Sanitizes and validates URLs to prevent malformed requests or local-file exploits.
    """
    if not url or not url.strip():
        return False
    try:
        parsed = urlparse(url.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Content Extraction & Quality Scoring
# ---------------------------------------------------------------------------

def score_body(text: str | None) -> float:
    """
    Heuristic-based quality assessment of extracted text.
    Rejects content that looks like navigation menus, cookie walls, or ads.
    
    Returns: 0.0 (junk) to 1.0 (perfectly readable news article)
    """
    if not text:
        return 0.0

    words = text.strip().split()
    word_count = len(words)

    # Rule 1: Length check. Most news stories are at least 100-150 words.
    if word_count < 100:
        return 0.0
    wc_score = min(0.6, 0.3 + (word_count - 100) / 1000)

    # Rule 2: Junk phrase detection. Common in paywalls or footer scrapes.
    junk_phrases = ["subscribe", "advertisement", "sign up", "newsletter", "follow us"]
    lower_text = text.lower()
    junk_penalty = 0.5 if any(p in lower_text for p in junk_phrases) else 0.0

    # Rule 3: Gibberish detection. Average word length < 3 indicates non-natural language.
    avg_len = sum(len(w) for w in words) / len(words)
    if avg_len < 3:
        len_score = 0.0
    elif avg_len < 4:
        len_score = 0.05
    else:
        len_score = 0.1

    # Bonus for clean text (no junk phrases)
    base_bonus = 0.3 if junk_penalty == 0 else 0.0

    final_score = wc_score + base_bonus + len_score - junk_penalty
    return max(0.0, min(1.0, final_score))

def _fetch_body(url: str) -> tuple[str | None, float, str | None]:
    """
    The core extraction engine. 
    Uses Trafilatura first (better for modern blogs/SPA) 
    and falls back to Newspaper3k (better for legacy news templates).
    """
    # Primary attempt: Trafilatura
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            if len(downloaded) > MAX_BODY_BYTES:
                log.warning(f"    [!] Page too large — skipping {url}")
                return None, 0.0, None
            
            body = trafilatura.extract(
                downloaded,
                include_comments=False,
                include_tables=False,
                no_fallback=False,
                favor_precision=True,
            )
            score = score_body(body)
            if score >= 0.5:
                return body.strip(), score, "text/html"
    except Exception as e:
        log.warning(f"    [!] trafilatura failed for {url}: {e}")

    # Fallback attempt: Newspaper3k
    try:
        config = Config()
        config.browser_user_agent = HEADERS["User-Agent"]
        config.request_timeout = TIMEOUT

        article = NewspaperArticle(url, config=config)
        article.download()
        article.parse()
        body = article.text.strip() if article.text else None
        score = score_body(body)
        if score >= 0.5:
            return body, score, "text/html"
    except Exception as e:
        log.warning(f"    [!] newspaper fallback failed for {url}: {e}")

    return None, 0.0, None

# ---------------------------------------------------------------------------
# Database Helpers
# ---------------------------------------------------------------------------

def _flag_for_removal(session: Session, article: RSSArticle, reason: str):
    """
    Marks an article for cleanup if the source is fundamentally broken 
    (e.g., dead link, 404, invalid URL).
    """
    existing = session.get(ArticleToRemove, article.id)
    if existing:
        return

    session.add(ArticleToRemove(
        id=article.id,
        outlet=article.outlet,
        url=article.url,
        reason=reason,
        flagged_at=datetime.utcnow(),
    ))
    session.commit()
    log.info(f"    [FLAGGED] {article.id[:8]}… reason={reason}")

def _get_unfetched(session: Session, batch_size: int) -> list[RSSArticle]:
    """
    Retrieves the oldest articles that haven't been processed yet.
    Excludes articles that have failed too many times or are already flagged.
    """
    flagged_ids_subquery = session.query(ArticleToRemove.id)

    return (
        session.query(RSSArticle)
        .filter(
            RSSArticle.body_fetched == False,
            RSSArticle.fetch_fail_count < MAX_FAIL_COUNT,
            RSSArticle.id.not_in(flagged_ids_subquery),
        )
        .order_by(RSSArticle.fetched_at.desc())
        .limit(batch_size)
        .all()
    )

# ---------------------------------------------------------------------------
# Main Job Execution
# ---------------------------------------------------------------------------

def run_body_fetch() -> dict:
    """
    Orchestrates the body fetch job.
    Uses multi-threading for the network I/O bound fetch stage,
    then paces the database commits to maintain session stability.
    """
    print("\n" + "="*60)
    print(">>> [BACKGROUND] STARTING BODY FETCH JOB <<<")
    print("="*60 + "\n")
    
    run_id = str(uuid.uuid4())[:8]
    started = datetime.now(timezone.utc)

    log.info(f"  Body Fetcher  |  run_id={run_id}")
    log.info(f"  Workers : Parallel threads with human-pacing")

    total_fetched = 0
    total_skipped = 0
    total_failed = 0
    total_flagged = 0
    top_error = None

    try:
        with Session(engine) as session:
            articles = _get_unfetched(session, BATCH_SIZE)
            log.info(f"  Articles to process: {len(articles)}")

            if not articles:
                log.info("  Nothing to fetch — all articles up to date.")

            # Extract data into simple dicts to avoid sqlalchemy session binding issues across threads
            pending = [
                {"id": a.id, "url": a.url, "title": a.title,
                 "outlet": a.outlet, "fail_count": a.fetch_fail_count or 0}
                for a in articles
            ]

        # Pre-filter invalid URLs to avoid wasting network resources
        valid_pending = []
        invalid_pending = []
        for a in pending:
            if _is_valid_url(a["url"]):
                valid_pending.append(a)
            else:
                invalid_pending.append(a)

        # Cleanup invalid records
        if invalid_pending:
            with Session(engine) as session:
                for a in invalid_pending:
                    article = session.get(RSSArticle, a["id"])
                    if article:
                        _flag_for_removal(session, article, "invalid_url")
                        total_skipped += 1
                        total_flagged += 1

        def fetch_one(a: dict) -> dict:
            """Wrapper for parallel execution."""
            body, score, content_type = _fetch_body(a["url"])
            return {**a, "body": body, "body_quality": score, "content_type": content_type}

        done = 0
        total = len(valid_pending)

        # NOTE: max_workers=10 provides a good balance between speed and IP reputation risk.
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_one, a): a for a in valid_pending}

            for future in as_completed(futures):
                result = future.result()
                done += 1

                # Pace the commit rate to avoid database deadlocks and simulate sequential browsing
                human_delay()

                with Session(engine) as session:
                    article = session.get(RSSArticle, result["id"])
                    if not article:
                        continue

                    if result["body"]:
                        article.body = result["body"]
                        article.body_quality = result["body_quality"]
                        article.content_type = result["content_type"]
                        article.body_fetched = True
                        article.fetch_fail_count = 0
                        session.commit()
                        log.info(f"  [{done}/{total}] ✓  {result['outlet']:<20}  Q:{result['body_quality']:.2f}")
                        total_fetched += 1
                    else:
                        # Increment fail count on soft failures (blocked, 404, low quality)
                        article.fetch_fail_count = result["fail_count"] + 1
                        session.commit()
                        log.warning(f"  [{done}/{total}] ✗  {result['outlet']:<20}  blocked")
                        total_failed += 1

                        if article.fetch_fail_count >= MAX_FAIL_COUNT:
                            log.warning(f"    [!] Max retries reached for {result['id'][:8]}…")

    except Exception as e:
        top_error = str(e)
        log.error(f"  [FATAL] Run failed: {e}")

    # Summary and Telemetry
    elapsed = (datetime.now(timezone.utc) - started).seconds
    run_status = "error" if top_error else ("partial" if total_failed > 0 else "success")
    readability_ratio = total_fetched / total if total > 0 else 0.0

    # Write job logs for the Admin dashboard
    try:
        with Session(engine) as ls:
            ls.add(FetchLog(
                run_id=run_id,
                outlet="body_fetcher",
                articles_new=total_fetched,
                articles_skip=total_skipped,
                status=run_status,
                error_message=top_error,
            ))
            ls.commit()
    except Exception as e:
        log.warning(f"  Could not write fetch log: {e}")

    print("\n" + "="*60)
    print(f">>> [BACKGROUND] BODY FETCH COMPLETE | Fetched: {total_fetched} <<<")
    print("="*60 + "\n")

    return {
        "status":             "started",
        "run_id":             run_id,
        "elapsed_sec":        elapsed,
        "articles_new":       total_fetched,
        "articles_skip":      total_skipped,
        "articles_failed":    total_failed,
        "articles_flagged":   total_flagged,
        "run_status":         run_status,
        "readability_ratio":  readability_ratio,
    }

if __name__ == "__main__":
    # Script entry point for manual triggering
    init_db()
    summary = run_body_fetch()
    print(summary)
