"""
rss_fetcher.py — High-performance ingestion engine for RSS news feeds.

This module orchestrates the collection of news articles from hundreds of diverse sources.
It is designed for reliability, safety, and speed, featuring a multi-threaded execution
model and defensive networking patterns.

Key Features:
- Circuit Breaker: Automatically pauses requests to failing or rate-limited outlets.
- SSRF Protection: Validates URLs to prevent attacks against internal infrastructure.
- ETags/Last-Modified: Respects HTTP caching headers to minimize bandwidth usage.
- Multi-threading: Parallelizes I/O-bound fetch operations across 10 concurrent workers.
- Telemetry: Detailed logging and per-run audit trails in the FetchLog table.
"""

import time
import uuid
import logging
import socket
import ipaddress
import threading
import feedparser
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy.orm import Session

from database import engine, SessionLocal
from models import RSSArticle, FetchLog, init_db
from outlets import INDIAN_OUTLETS, GLOBAL_OUTLETS
from clustering_service import ClusteringService

# ---------------------------------------------------------------------------
# Logging & Metrics Setup
# ---------------------------------------------------------------------------

log = logging.getLogger("RSSFetcher")
log.setLevel(logging.INFO)
log.propagate = False
if not log.handlers:
    formatter = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    for h in (logging.StreamHandler(), logging.FileHandler("logs/feeds.log", encoding="utf-8")):
        h.setFormatter(formatter)
        log.addHandler(h)

# ---------------------------------------------------------------------------
# Reliability & Security Utilities
# ---------------------------------------------------------------------------

CIRCUIT_THRESHOLD = 3
CIRCUIT_COOLDOWN = 300

class CircuitBreaker:
    """
    Prevents the aggregator from hammering an outlet that is down or blocking us.
    If an outlet fails 3 times, it enters a 5-minute 'OPEN' state.
    """
    def __init__(self):
        self._failures = {}
        self._opened_at = {}
        self._lock = threading.Lock()

    def is_open(self, key: str) -> bool:
        with self._lock:
            if self._failures.get(key, 0) < CIRCUIT_THRESHOLD: return False
            if (time.time() - self._opened_at.get(key, 0)) >= CIRCUIT_COOLDOWN:
                self._failures[key] = 0 # Reset on cooldown expiry
                return False
            return True

    def record_failure(self, key: str):
        with self._lock:
            self._failures[key] = self._failures.get(key, 0) + 1
            if self._failures[key] >= CIRCUIT_THRESHOLD:
                self._opened_at[key] = time.time()

    def record_success(self, key: str):
        with self._lock:
            self._failures.pop(key, None)
            self._opened_at.pop(key, None)

_circuit = CircuitBreaker()

def _is_safe_url(url: str) -> bool:
    """
    SSRF Protection: Blocks requests to non-HTTP schemes or private/loopback IP ranges.
    This ensures the fetcher cannot be tricked into scanning the internal network.
    """
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}: return False
        if not parsed.hostname: return False
        
        ip = ipaddress.ip_address(socket.gethostbyname(parsed.hostname))
        if ip.is_private or ip.is_loopback or ip.is_link_local:
            log.warning(f"[Security] Blocked unsafe URL: {url} → {ip}")
            return False
        return True
    except Exception: return False

# ---------------------------------------------------------------------------
# Networking Configuration
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    """Creates a pre-configured requests session with retries and a custom User-Agent."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "NewsHere/1.0 RSS Aggregator"})
    return session

# Global cache for HTTP caching headers (persists for the lifetime of the process)
_feed_cache = {}

def _fetch_feed_raw(session: requests.Session, feed_url: str, cancel_event: threading.Event) -> feedparser.FeedParserDict | None:
    """
    Downloads and parses an RSS/Atom feed.
    Implements conditional GET (ETags) and size limits (5MB) to save bandwidth.
    """
    if not _is_safe_url(feed_url): return None

    # Load caching headers if we've seen this feed before
    cached = _feed_cache.get(feed_url, {})
    headers = {}
    if cached.get("etag"): headers["If-None-Match"] = cached["etag"]
    if cached.get("last_modified"): headers["If-Modified-Since"] = cached["last_modified"]

    try:
        if cancel_event.is_set(): return None
        response = session.get(feed_url, headers=headers, timeout=(10, 30), stream=True)

        if response.status_code == 304: return None # Content unchanged
        if not response.ok: return None

        # Update cache headers for next run
        new_cache = {}
        if "ETag" in response.headers: new_cache["etag"] = response.headers["ETag"]
        if "Last-Modified" in response.headers: new_cache["last_modified"] = response.headers["Last-Modified"]
        if new_cache: _feed_cache[feed_url] = new_cache

        # Stream download to enforce 5MB size limit
        chunks = []
        total = 0
        for chunk in response.iter_content(chunk_size=65536):
            if cancel_event.is_set(): 
                response.close()
                return None
            total += len(chunk)
            if total > 5 * 1024 * 1024: 
                response.close()
                return None
            chunks.append(chunk)

        feed = feedparser.parse(b"".join(chunks))
        return feed if not (feed.bozo and not feed.entries) else None

    except Exception as e:
        log.error(f"Feed fetch error {feed_url}: {e}")
        return None

# ---------------------------------------------------------------------------
# Data Persistence
# ---------------------------------------------------------------------------

def _save_article(session: Session, data: dict) -> bool:
    """Safely persists an article to the DB if it doesn't already exist."""
    # Deduplication check
    exists = session.query(RSSArticle).filter_by(url=data["url"]).first()
    if exists or not data.get("url") or not data.get("title"): return False
    
    try:
        session.add(RSSArticle(**data))
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False

# ---------------------------------------------------------------------------
# Pipeline Orchestration
# ---------------------------------------------------------------------------

def _fetch_one_outlet(outlet_name: str, outlet_info: dict, run_id: str, cancel_event: threading.Event) -> dict:
    """Handles the full ingestion cycle for a single news outlet (multiple feeds)."""
    if _circuit.is_open(outlet_name):
        return {"outlet": outlet_name, "new": 0, "status": "skipped"}

    total_new, total_skip = 0, 0
    status, error_msg = "success", None
    session_http = _make_session()

    try:
        with Session(engine) as db:
            for feed_url in outlet_info["feeds"]:
                if cancel_event.is_set():
                    status = "cancelled"
                    break
                
                feed = _fetch_feed_raw(session_http, feed_url, cancel_event)
                if not feed: continue

                for entry in feed.entries:
                    data = {
                        "outlet":    outlet_name,
                        "bias":      outlet_info["bias"],
                        "country":   outlet_info["country"],
                        "title":     entry.get("title", "").strip()[:500],
                        "url":       entry.get("link", "").strip(),
                        "summary":   entry.get("summary", "")[:50000].strip(),
                        "published": entry.get("published", "").strip(),
                    }
                    if _save_article(db, data): total_new += 1
                    else: total_skip += 1
                
                # Polite delay between feeds of the same outlet
                time.sleep(0.5)

        _circuit.record_success(outlet_name)

    except Exception as e:
        status, error_msg = "failed", str(e)
        _circuit.record_failure(outlet_name)
    finally:
        session_http.close()

    # Log telemetry for this outlet run
    with Session(engine) as ls:
        ls.add(FetchLog(run_id=run_id, outlet=outlet_name, articles_new=total_new, articles_skip=total_skip, status=status, error_message=error_msg))
        ls.commit()

    return {"outlet": outlet_name, "new": total_new, "skip": total_skip, "status": status}

def run_rss_collection() -> dict:
    """
    Main entry point for the ingestion job. 
    Combines regional (Indian) and Global outlets into a parallelized task queue.
    """
    print("\n>>> [BACKGROUND] STARTING RSS COLLECTION <<<")
    
    run_id = str(uuid.uuid4())[:8]
    started = datetime.now(timezone.utc)
    cancel_event = threading.Event()

    # Consolidate all sources
    all_tasks = {name: info for name, info in INDIAN_OUTLETS.items()}
    for g in GLOBAL_OUTLETS:
        all_tasks[g["name"]] = {"bias": g["bias"], "country": g["country"], "feeds": [g["url"]]}

    log.info(f"Started RSS Run {run_id} | Outlets: {len(all_tasks)}")

    results = []
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="rss") as pool:
        futures = {pool.submit(_fetch_one_outlet, name, info, run_id, cancel_event): name for name, info in all_tasks.items()}
        for f in as_completed(futures):
            try: results.append(f.result())
            except Exception as e: log.error(f"Thread failure: {e}")

    elapsed = (datetime.now(timezone.utc) - started).seconds
    total_new = sum(r.get("new", 0) for r in results)
    
    print(f">>> [BACKGROUND] RSS COLLECTION COMPLETE | New: {total_new} <<<")
    return {"run_id": run_id, "elapsed_sec": elapsed, "articles_new": total_new, "outlets_total": len(all_tasks)}

if __name__ == "__main__":
    init_db()
    run_rss_collection()
