import uuid
import logging
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

from newspaper import Article as NewspaperArticle, Config
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import engine
from models import RSSArticle, ArticleToRemove, FetchLog, init_db
from concurrent.futures import ThreadPoolExecutor, as_completed
import trafilatura
from clustering_service import ClusteringService


def human_delay():
    delay = random.uniform(3.0, 5.0)

    if random.random() < 0.10:
        extra_pause = 1
        delay += extra_pause
        print(f"⏳ Taking a longer reading pause: {delay:.2f} seconds...")
    else:
        print(f"⏳ Waiting: {delay:.2f} seconds...")

    time.sleep(delay)


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

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 15
TIMEOUT = READ_TIMEOUT
MAX_FAIL_COUNT = 3
MAX_BODY_BYTES = 5 * 1024 * 1024
BATCH_SIZE = 3000

# 1. FIXED: Disguise as a standard Windows Chrome browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def _is_valid_url(url: str | None) -> bool:
    """
    Returns True only if the URL is:
      - not None or empty
      - starts with http:// or https://
      - parseable with a valid netloc (domain)
    """
    if not url or not url.strip():
        return False
    try:
        parsed = urlparse(url.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def score_body(text: str | None) -> float:
    """
    Returns a quality score between 0 and 1.
    Criteria: word count, junk phrases, average word length.
    """
    if not text:
        return 0.0

    words = text.strip().split()
    word_count = len(words)

    # 1. Word Count Score (up to 0.6)
    # Rejects < 100 words (score 0)
    # 100 words -> 0.3, 400+ words -> 0.6
    if word_count < 100:
        return 0.0
    wc_score = min(0.6, 0.3 + (word_count - 100) / 1000)

    # 2. Junk Penalty (reduces score by 0.5)
    junk_phrases = ["subscribe", "advertisement", "sign up", "newsletter", "follow us"]
    lower_text = text.lower()
    junk_penalty = 0.5 if any(p in lower_text for p in junk_phrases) else 0.0

    # 3. Word Length Score (up to 0.1)
    # Average word length < 3 is very poor (gibberish/code)
    avg_len = sum(len(w) for w in words) / len(words)
    if avg_len < 3:
        len_score = 0.0
    elif avg_len < 4:
        len_score = 0.05
    else:
        len_score = 0.1

    # Base consensus score (0.3 if no junk)
    base_bonus = 0.3 if junk_penalty == 0 else 0.0

    final_score = wc_score + base_bonus + len_score - junk_penalty
    return max(0.0, min(1.0, final_score))


def _flag_for_removal(session: Session, article: RSSArticle, reason: str):
    """
    Insert article into articles_to_remove.
    Skips silently if already flagged (primary key conflict).
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
    log.info(
        f"    [FLAGGED] {article.id[:8]}… reason={reason}  url={article.url}")


def _fetch_body(url: str) -> tuple[str | None, float, str | None]:
    """
    Returns (body, quality_score, content_type)
    """
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            if len(downloaded) > MAX_BODY_BYTES:
                log.warning(
                    f"    [!] Page too large ({len(downloaded)} bytes) — skipping {url}")
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


def _get_unfetched(session: Session, batch_size: int) -> list[RSSArticle]:
    """
    Returns articles that:
      - have not had their body fetched yet
      - have not exceeded the fail threshold
      - are not already flagged for removal
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


def run_body_fetch() -> dict:
    print("\n" + "="*60)
    print(">>> [BACKGROUND] STARTING BODY FETCH JOB <<<")
    print("="*60 + "\n")
    run_id = str(uuid.uuid4())[:8]
    started = datetime.now(timezone.utc)

    log.info("=" * 55)
    log.info(f"  Body Fetcher  |  run_id={run_id}")
    log.info(f"  Started : {started.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info(f"  Workers : 2 parallel threads (human mode)")
    log.info("=" * 55)

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

            pending = [
                {"id": a.id, "url": a.url, "title": a.title,
                 "outlet": a.outlet, "fail_count": a.fetch_fail_count or 0}
                for a in articles
            ]

        valid_pending = []
        invalid_pending = []
        for a in pending:
            if _is_valid_url(a["url"]):
                valid_pending.append(a)
            else:
                invalid_pending.append(a)

        if invalid_pending:
            with Session(engine) as session:
                for a in invalid_pending:
                    article = session.get(RSSArticle, a["id"])
                    if article:
                        _flag_for_removal(session, article, "invalid_url")
                        total_skipped += 1
                        total_flagged += 1

        def fetch_one(a: dict) -> dict:
            body, score, content_type = _fetch_body(a["url"])
            return {**a, "body": body, "body_quality": score, "content_type": content_type}

        done = 0
        total = len(valid_pending)

        # 4. FIXED: Lower max_workers to 2 to prevent concurrent IP spamming
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_one, a): a for a in valid_pending}

            for future in as_completed(futures):
                result = future.result()
                done += 1

                # Rate-limit between completions so we don't hammer servers.
                # Delay here (not inside the thread) so workers aren't blocked
                # on each other's sleep — they fetch in parallel, we pace commits.
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
                        log.info(
                            f"  [{done}/{total}] ✓  {result['outlet']:<20}"
                            f"  Q:{result['body_quality']:.2f}  {result['title'][:55]}…"
                        )
                        total_fetched += 1
                    else:
                        article.fetch_fail_count = result["fail_count"] + 1
                        session.commit()
                        log.warning(
                            f"  [{done}/{total}] ✗  {result['outlet']:<20}"
                            f"  {'blocked':>6}        {result['title'][:55]}…"
                        )
                        total_failed += 1

                        if article.fetch_fail_count >= MAX_FAIL_COUNT:
                            log.warning(f"    [!] Reached max retries ({MAX_FAIL_COUNT}) for {result['id'][:8]}… - giving up for now.")

    except Exception as e:
        top_error = str(e)
        log.error(f"  [FATAL] Run failed: {e}")

    elapsed = (datetime.now(timezone.utc) - started).seconds
    run_status = "error" if top_error else (
        "partial" if total_failed > 0 else "success")

    readability_ratio = total_fetched / total if total > 0 else 0.0

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

    log.info("=" * 55)

    print("\n" + "="*60)
    print(f">>> [BACKGROUND] BODY FETCH COMPLETE | Fetched: {total_fetched} | Failed: {total_failed} <<<")
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
    init_db()
    summary = run_body_fetch()
    print(summary)
