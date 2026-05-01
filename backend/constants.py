
"""
Constants and static data for the NewsHere project.
"""

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

PAGE_SIZE = 40

# Fetcher configurations
MAX_BODY_BYTES = 5 * 1024 * 1024
MAX_FAIL_COUNT = 3
BATCH_SIZE = 300
TIMEOUT = 15.0

# User Agent for HTTP requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
