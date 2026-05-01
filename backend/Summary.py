"""
Summary.py — Article summarisation via Google Gemini.

Key fixes vs original:
  1. Session lifetime bug — original opened ONE session for the entire
     run, including all API calls.  Long API call chains caused the
     connection to time out, rolling back the whole batch.  We now use
     short-lived sessions: one to *read* articles, one per batch-commit.

  2. Single-article helper (generate_summary) now correctly returns ""
     on validation failure rather than silently proceeding with an
     invalid prompt.

  3. Added retry jitter to avoid thundering-herd on rate-limit errors.
"""

import json
import time
import re
import os
import random
import logging
from typing import List, Dict, Any, Tuple

from dotenv import load_dotenv
from google import genai
from google.genai import types
from sqlalchemy.orm import Session

from database import SessionLocal
from models import RSSArticle

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SummaryService")

# ---------------------------------------------------------------------------
# Gemini client
# ---------------------------------------------------------------------------

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

BATCH_SIZE = 3       # articles per LLM call
DB_FETCH_LIMIT = 1000
MAX_WORDS = 2000


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def is_valid_article(body: str) -> bool:
    """Reject bodies that are too short or suspiciously un-structured."""
    if not body:
        return False
    words = body.split()
    return len(words) >= 200 and body.count("\n") >= 3


def extract_relevant_text(body: str) -> str:
    return " ".join(body.split()[:MAX_WORDS])


# ---------------------------------------------------------------------------
# LLM interaction
# ---------------------------------------------------------------------------

def build_prompt(batch: List[Tuple[str, str]]) -> str:
    articles_json = [{"id": art_id, "text": body} for art_id, body in batch]
    return f"""
Analyze the following {len(batch)} news articles.
For each, provide a neutral 2-sentence summary and a bias classification
(Left, Right, Center).

Return ONLY a JSON array of objects (no markdown fences, no preamble):
[{{"id": "...", "summary": "...", "bias": "..."}}]

Articles:
{json.dumps(articles_json)}
"""


def parse_json_response(text_content: str) -> List[Dict[str, Any]]:
    """Parse LLM output that may contain markdown fences or stray text."""
    clean = re.sub(r"```json|```", "", text_content).strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        pass

    # Fallback: find the outermost JSON array.
    try:
        start = clean.find("[")
        end = clean.rfind("]") + 1
        if start != -1 and end > 0:
            return json.loads(clean[start:end])
    except Exception:
        pass

    logger.error("Failed to parse JSON from LLM response.")
    return []


def call_model(prompt: str, retries: int = 3) -> str:
    """
    Call Gemini with exponential back-off + jitter.

    Raises on final failure so callers can handle gracefully.
    """
    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model="models/gemma-3-27b-it",
                contents=prompt,
            )
            return response.text
        except Exception as exc:
            if attempt == retries - 1:
                raise
            sleep = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Gemini call failed (attempt {attempt + 1}): {exc}. Retrying in {sleep:.1f}s.")
            time.sleep(sleep)
    return ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_summary(article_id: str, body: str) -> str:
    """
    Generate a summary for a single article (called by clustering_service).

    Returns "" if the body fails validation or the API call fails.
    """
    if not is_valid_article(body):
        return ""

    prompt = build_prompt([(article_id, extract_relevant_text(body))])
    try:
        raw = call_model(prompt)
        parsed = parse_json_response(raw)
        if parsed:
            return parsed[0].get("summary", "")
    except Exception as exc:
        logger.error(f"generate_summary failed for article {article_id}: {exc}")

    return ""


def process_articles():
    """
    Batch-process all un-summarised articles.

    Session strategy (key fix):
      - Session A  : short read to fetch article ids + bodies, then closed.
      - Session B  : opened fresh for EACH batch commit — no long-lived
                     connection across API calls.
    """
    logger.info("Starting batch summary process...")

    # --- Read phase (short session) ---
    with SessionLocal() as read_session:
        articles = (
            read_session.query(RSSArticle)
            .filter(
                RSSArticle.body_fetched == True,
                RSSArticle.body.isnot(None),
                (RSSArticle.ai_summary == None) | (RSSArticle.ai_summary == "") |
                (RSSArticle.bias_label == None),
            )
            .limit(DB_FETCH_LIMIT)
            .all()
        )

        if not articles:
            logger.info("No articles need summaries.")
            return

        # Materialise only the fields we need so we can close the session.
        valid_items: List[Tuple[str, str]] = [
            (a.id, extract_relevant_text(a.body))
            for a in articles
            if is_valid_article(a.body)
        ]

    logger.info(
        f"Found {len(articles)} candidates, {len(valid_items)} passed length filters."
    )

    # --- Process + write phase (one session per batch) ---
    for i in range(0, len(valid_items), BATCH_SIZE):
        chunk = valid_items[i: i + BATCH_SIZE]
        prompt = build_prompt(chunk)

        try:
            raw = call_model(prompt)
            parsed = parse_json_response(raw)
        except Exception as exc:
            logger.error(f"LLM call failed for batch starting at {i}: {exc}")
            continue

        if not parsed:
            logger.warning(f"Empty parse result for batch {i}. Skipping.")
            continue

        # Fresh session for each commit — avoids stale / timed-out connections.
        with SessionLocal() as write_session:
            try:
                for item in parsed:
                    article = write_session.get(RSSArticle, item.get("id"))
                    if article:
                        article.ai_summary = item.get("summary")
                        article.bias_label = item.get("bias")
                write_session.commit()
                logger.info(f"Committed batch of {len(chunk)} ({i + len(chunk)}/{len(valid_items)} done).")
            except Exception as exc:
                write_session.rollback()
                logger.error(f"DB write failed for batch {i}: {exc}")


if __name__ == "__main__":
    process_articles()