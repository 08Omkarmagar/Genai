import time
import uuid
import logging
import socket
import ipaddress
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy.orm import Session

from database import engine, SessionLocal
from models import RSSArticle, FetchLog, init_db
from outlets import INDIAN_OUTLETS, GLOBAL_OUTLETS
from clustering_service import ClusteringService

log = logging.getLogger("RSSFetcher")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    formatter = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for handler in (
        logging.StreamHandler(),
        logging.FileHandler("logs/feeds.log", encoding="utf-8"),
    ):
        handler.setFormatter(formatter)
        log.addHandler(handler)

# Reuses validators during one run so unchanged feeds are skipped.
_feed_cache: dict[str, dict] = {}

# Stops repeated failures from hammering one outlet.
CIRCUIT_THRESHOLD = 3
CIRCUIT_COOLDOWN = 300


class CircuitBreaker:
    def __init__(self):
        self._failures:   dict[str, int] = {}
        self._opened_at:  dict[str, float] = {}
        self._lock = threading.Lock()

    def is_open(self, key: str) -> bool:
        with self._lock:
            if key not in self._failures:
                return False
            if self._failures[key] < CIRCUIT_THRESHOLD:
                return False
            elapsed = time.time() - self._opened_at.get(key, 0)
            if elapsed >= CIRCUIT_COOLDOWN:
                log.info(
                    f"[CircuitBreaker] {key}: cooldown expired, allowing retry")
                self._failures[key] = 0
                return False
            remaining = int(CIRCUIT_COOLDOWN - elapsed)
            log.warning(
                f"[CircuitBreaker] {key}: circuit OPEN — skipping ({remaining}s remaining)")
            return True

    def record_failure(self, key: str):
        with self._lock:
            self._failures[key] = self._failures.get(key, 0) + 1
            if self._failures[key] >= CIRCUIT_THRESHOLD:
                self._opened_at[key] = time.time()
                log.warning(
                    f"[CircuitBreaker] {key}: circuit opened after {self._failures[key]} failures")

    def record_success(self, key: str):
        with self._lock:
            self._failures.pop(key, None)
            self._opened_at.pop(key, None)


_circuit = CircuitBreaker()

_ALLOWED_SCHEMES = {"http", "https"}


def _is_safe_url(url: str) -> bool:
    """Return True only if the URL uses http/https and doesn't point at a private IP."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in _ALLOWED_SCHEMES:
            log.warning(f"[Security] Blocked non-http scheme: {url}")
            return False

        hostname = parsed.hostname
        if not hostname:
            return False

        # Blocks SSRF targets such as localhost, private networks, and link-local IPs.
        ip = ipaddress.ip_address(socket.gethostbyname(hostname))
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            log.warning(
                f"[Security] Blocked private/loopback URL: {url} → {ip}")
            return False

        return True
    except Exception:
        return False


CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
MAX_FEED_BYTES = 5 * 1024 * 1024


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://",  adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "NewsHere/1.0 RSS Aggregator"})
    return session


def _fetch_feed_raw(
    session: requests.Session,
    feed_url: str,
    cancel_event: threading.Event,
) -> feedparser.FeedParserDict | None:
    """
    Download raw feed bytes with all safety checks, then parse with feedparser.
    Returns None on any failure.
    """
    if not _is_safe_url(feed_url):
        log.warning(f"    [!] Unsafe URL blocked: {feed_url}")
        return None

    cached = _feed_cache.get(feed_url, {})
    headers = {}
    if cached.get("etag"):
        headers["If-None-Match"] = cached["etag"]
    if cached.get("last_modified"):
        headers["If-Modified-Since"] = cached["last_modified"]

    try:
        if cancel_event.is_set():
            log.info("    [!] Cancelled before fetch")
            return None

        response = session.get(
            feed_url,
            headers=headers,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            stream=True,
        )

        if response.status_code == 304:
            log.info(f"    [~] 304 Not Modified — skipping {feed_url}")
            return None

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            log.warning(f"    [429] Rate limited — waiting {retry_after}s")
            time.sleep(min(retry_after, 120))
            return None

        if not response.ok:
            log.warning(f"    [HTTP {response.status_code}] {feed_url}")
            return None

        new_cache = {}
        if "ETag" in response.headers:
            new_cache["etag"] = response.headers["ETag"]
        if "Last-Modified" in response.headers:
            new_cache["last_modified"] = response.headers["Last-Modified"]
        if new_cache:
            _feed_cache[feed_url] = new_cache

        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if cancel_event.is_set():
                log.info("    [!] Cancelled mid-stream")
                response.close()
                return None

            total += len(chunk)
            if total > MAX_FEED_BYTES:
                log.warning(
                    f"    [!] Feed too large (>{MAX_FEED_BYTES // 1024}KB) — skipping {feed_url}")
                response.close()
                return None
            chunks.append(chunk)

        raw_bytes = b"".join(chunks)

        if len(raw_bytes) < 100:
            log.warning(
                f"    [!] Response suspiciously small ({len(raw_bytes)} bytes) — skipping")
            return None

        feed = feedparser.parse(raw_bytes)

        if feed.bozo and not feed.entries:
            log.warning(f"    [!] Malformed/incomplete feed: {feed_url}")
            return None

        return feed

    except requests.exceptions.ConnectionError as e:
        log.error(f"    [NetworkError] {feed_url}: {e}")
        return None
    except requests.exceptions.Timeout:
        log.error(
            f"    [Timeout] {feed_url}: exceeded {READ_TIMEOUT}s read / {CONNECT_TIMEOUT}s connect")
        return None
    except requests.exceptions.RequestException as e:
        log.error(f"    [RequestError] {feed_url}: {e}")
        return None


def _article_exists(session: Session, url: str) -> bool:
    return session.query(RSSArticle).filter_by(url=url).first() is not None


def _save_article(session: Session, data: dict) -> bool:
    if not data.get("url") or not data.get("title"):
        return False
    if _article_exists(session, data["url"]):
        return False
    try:
        session.add(RSSArticle(
            outlet=data["outlet"],
            bias=data["bias"],
            country=data["country"],
            title=data["title"],
            url=data["url"],
            summary=data["summary"],
            published=data["published"],
        ))
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False


def _fetch_one_outlet(
    outlet_name: str,
    outlet_info: dict,
    run_id: str,
    cancel_event: threading.Event,
) -> dict:
    """Fetch all feeds for one outlet. Returns a summary dict."""

    if _circuit.is_open(outlet_name):
        return {"outlet": outlet_name, "new": 0, "skip": 0, "status": "skipped"}

    log.info(f"  → {outlet_name}  [{outlet_info['bias'].upper()}]")

    total_new = 0
    total_skip = 0
    error_msg = None
    status = "success"

    session_http = _make_session()

    try:
        with Session(engine) as db:
            for feed_url in outlet_info["feeds"]:
                if cancel_event.is_set():
                    log.info(
                        f"    [!] Collection cancelled during {outlet_name}")
                    status = "cancelled"
                    break

                log.info(f"    Fetching: {feed_url}")
                feed = _fetch_feed_raw(session_http, feed_url, cancel_event)

                if feed is None:
                    continue

                if not feed.entries:
                    log.warning(f"    [!] No entries in feed")
                    continue

                log.info(f"    Found {len(feed.entries)} entries")

                for entry in feed.entries:
                    raw_summary = entry.get("summary", "")
                    if len(raw_summary) > 50_000:
                        raw_summary = raw_summary[:50_000]

                    data = {
                        "outlet":    outlet_name,
                        "bias":      outlet_info["bias"],
                        "country":   outlet_info["country"],
                        "title":     entry.get("title",     "").strip()[:500],
                        "url":       entry.get("link",      "").strip(),
                        "summary":   raw_summary.strip(),
                        "published": entry.get("published", "").strip(),
                    }

                    if _save_article(db, data):
                        total_new += 1
                    else:
                        total_skip += 1

                time.sleep(0.5)

        _circuit.record_success(outlet_name)

    except Exception as e:
        status = "failed"
        error_msg = str(e)
        log.error(f"  [ERROR] {outlet_name}: {e}")
        _circuit.record_failure(outlet_name)

    finally:
        session_http.close()

    try:
        with Session(engine) as ls:
            ls.add(FetchLog(
                run_id=run_id,
                outlet=outlet_name,
                articles_new=total_new,
                articles_skip=total_skip,
                status=status,
                error_message=error_msg,
            ))
            ls.commit()
    except Exception as e:
        log.warning(f"  Could not write fetch log: {e}")

    log.info(f"    saved={total_new}  skipped={total_skip}  status={status}")
    return {"outlet": outlet_name, "new": total_new, "skip": total_skip, "status": status}


_cancel_event = threading.Event()


def cancel_current_run():
    """Signal the currently running collection to stop cleanly."""
    _cancel_event.set()
    log.info("[Collector] Cancellation requested.")


def run_rss_collection() -> dict:
    print("\n" + "="*60)
    print(">>> [BACKGROUND] STARTING RSS COLLECTION <<<")
    print("="*60 + "\n")
    
    global _cancel_event
    _cancel_event = threading.Event()

    run_id = str(uuid.uuid4())[:8]
    started = datetime.now(timezone.utc)

    # Merge regional and global outlets into a unified task list
    all_tasks = {}
    for name, info in INDIAN_OUTLETS.items():
        all_tasks[name] = info
    for item in GLOBAL_OUTLETS:
        all_tasks[item["name"]] = {
            "bias": item["bias"],
            "country": item["country"],
            "feeds": [item["url"]]
        }

    log.info("=" * 55)
    log.info(f"  RSS Collection  |  run_id={run_id}")
    log.info(f"  Started : {started.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info(f"  Outlets : {len(all_tasks)}")
    log.info("=" * 55)

    results = []

    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="rss") as pool:
        futures = {
            pool.submit(
                _fetch_one_outlet,
                outlet_name,
                outlet_info,
                run_id,
                _cancel_event,
            ): outlet_name
            for outlet_name, outlet_info in all_tasks.items()
        }

        for future in as_completed(futures):
            outlet_name = futures[future]
            try:
                result = future.result()
            except Exception as e:
                log.error(f"  Unexpected error for {outlet_name}: {e}")
                result = {"outlet": outlet_name, "new": 0,
                          "skip": 0, "status": "failed"}
            results.append(result)

    elapsed = (datetime.now(timezone.utc) - started).seconds
    total_new = sum(r["new"] for r in results)
    total_skip = sum(r["skip"] for r in results)
    
    print("\n" + "="*60)
    print(f">>> [BACKGROUND] RSS COLLECTION COMPLETE | New: {total_new} | Skip: {total_skip} <<<")
    print("="*60 + "\n")

    log.info("=" * 55)
    log.info(f"  DONE  {elapsed}s  |  new={total_new}  skipped={total_skip}")
    if failed:
        log.warning(f"  Failed : {', '.join(failed)}")
    if skipped:
        log.info(f"  Circuit-skipped : {', '.join(skipped)}")
    log.info("=" * 55)

    return {
        "status":           "started",
        "run_id":           run_id,
        "elapsed_sec":      elapsed,
        "articles_new":     total_new,
        "articles_skip":    total_skip,
        "failed_outlets":   failed,
        "skipped_outlets":  skipped,
        "outlets_total":    len(INDIAN_OUTLETS) + len(GLOBAL_OUTLETS),
    }


def trigger_clustering():
    try:
        with SessionLocal() as session:
            service = ClusteringService(session)
            service.process_unassigned_articles(limit=500)
    except Exception as e:
        log.error(f"[AutoClusteringError] {e}")


def run_full_pipeline():
    result = run_rss_collection()
    trigger_clustering()
    return result


if __name__ == "__main__":
    init_db()
    summary = run_full_pipeline()
    print(summary)
