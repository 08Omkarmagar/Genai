"""
constants.py — Shared configuration values and static data.

This module consolidates configuration constants used across multiple backend
services, including stop words for NLP, pagination limits, and fetcher timeouts.
"""

# Common stop words used to filter noisy terms out of search queries and tokens
STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
    "for", "of", "with", "by", "from", "as", "is", "was", "are",
    "were", "been", "be", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall",
    "can", "need", "must", "it", "its", "not", "no", "nor", "so",
    "if", "then", "than", "too", "very", "just", "about", "above",
    "after", "before", "between", "under", "over", "again", "once",
    "here", "there", "when", "where", "why", "how", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such",
    "only", "own", "same", "that", "this", "these", "those", "what",
    "which", "who", "whom", "up", "out", "off", "down", "into",
    "during", "through", "while", "also", "back", "now", "new",
    "one", "two", "three", "says", "said", "briefly", "amid", "via",
    "per", "vs", "etc", "being", "found", "seen", "shut", "broken",
    "using", "uses", "became", "become", "next", "last", "week", "day",
    "month", "year", "news", "update", "report",
}

# Default items per page for API article/story lists
PAGE_SIZE = 40

# ---------------------------------------------------------------------------
# Ingestion & Fetcher Settings
# ---------------------------------------------------------------------------

MAX_BODY_BYTES = 5 * 1024 * 1024 # Reject pages larger than 5MB
MAX_FAIL_COUNT = 3               # Mark URL as broken after 3 failed attempts
BATCH_SIZE = 300                 # Max articles per ingestion batch
TIMEOUT = 15.0                   # Network request timeout in seconds

# standard browser User-Agent to avoid basic bot blocking
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
