"""
Summary.py — AI-powered summarization and bias classification service.

This module leverages Google Gemini (Gemma 3) to generate neutral summaries and initial
bias labels for articles in the database. It is designed to run as a background task
or as a utility for the clustering service.

Key Features:
- Short-Lived Sessions: Prevents DB timeouts during long LLM API calls by separating
  read and write phases into distinct, transactional session windows.
- Robust JSON Parsing: Extracts valid JSON from LLM output even when wrapped in markdown fences.
- Batch Processing: Optimizes API costs by grouping articles into small batches.
- Error Resilience: Implements exponential backoff with jitter for rate-limit handling.
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
from sqlalchemy.orm import Session
from database import SessionLocal
from models import RSSArticle

# ---------------------------------------------------------------------------
# Initialization & Config
# ---------------------------------------------------------------------------

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SummaryService")

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

# Processing constraints
BATCH_SIZE = 3       # Number of articles analyzed per single LLM call
DB_FETCH_LIMIT = 1000
MAX_WORDS = 2000     # Token limit per article snippet to stay within model limits

# ---------------------------------------------------------------------------
# Logic Helpers
# ---------------------------------------------------------------------------

def is_valid_article(body: str) -> bool:
    """
    Validation gate to prevent wasting API tokens on empty, short, or malformed scrapes.
    Most news stories are at least 200 words and contain multiple paragraph breaks.
    """
    if not body: return False
    words = body.split()
    return len(words) >= 200 and body.count("\n") >= 3

def build_prompt(batch: List[Tuple[str, str]]) -> str:
    """Constructs a structured prompt for batch analysis."""
    articles_json = [{"id": art_id, "text": body} for art_id, body in batch]
    return f"""
Analyze the following {len(batch)} news articles.
For each, provide a neutral 2-sentence summary and a bias classification (Left, Right, Center).

Return ONLY a JSON array of objects (no markdown fences, no preamble):
[{{"id": "...", "summary": "...", "bias": "..."}}]

Articles:
{json.dumps(articles_json)}
"""

def parse_json_response(text_content: str) -> List[Dict[str, Any]]:
    """
    Parses LLM output that may contain markdown code blocks or conversational text.
    Uses regex as a fallback to isolate the primary JSON array.
    """
    clean = re.sub(r"```json|```", "", text_content).strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        # Fallback: Find the first '[' and last ']' to isolate the array
        start, end = clean.find("["), clean.rfind("]") + 1
        if start != -1 and end > 0:
            try: return json.loads(clean[start:end])
            except: pass
    logger.error("LLM JSON parsing failed.")
    return []

# ---------------------------------------------------------------------------
# API Orchestration
# ---------------------------------------------------------------------------

def call_model(prompt: str, retries: int = 3) -> str:
    """Executes a model call with exponential backoff and randomized jitter."""
    for attempt in range(retries):
        try:
            response = client.models.generate_content(model="models/gemma-3-27b-it", contents=prompt)
            return response.text
        except Exception as exc:
            if attempt == retries - 1: raise
            sleep = (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"Gemini error (attempt {attempt+1}): {exc}. Retrying in {sleep:.1f}s.")
            time.sleep(sleep)
    return ""

def generate_summary(article_id: str, body: str) -> str:
    """Public helper for single-article summary generation (e.g., from clustering_service)."""
    if not is_valid_article(body): return ""
    try:
        raw = call_model(build_prompt([(article_id, body[:MAX_WORDS*5])]))
        parsed = parse_json_response(raw)
        return parsed[0].get("summary", "") if parsed else ""
    except Exception: return ""

# ---------------------------------------------------------------------------
# Batch Processing Job
# ---------------------------------------------------------------------------

def process_articles():
    """
    Main job loop. 
    Uses a 'Read-Process-Write' cycle with fresh DB sessions for each phase 
    to ensure database stability across long-running network operations.
    """
    logger.info("Starting summary batch...")

    # Phase 1: Materialize targets (Short read session)
    with SessionLocal() as db:
        articles = db.query(RSSArticle).filter(
            RSSArticle.body_fetched == True,
            (RSSArticle.ai_summary == None) | (RSSArticle.ai_summary == "")
        ).limit(DB_FETCH_LIMIT).all()
        
        valid_items = [(a.id, a.body[:5000]) for a in articles if is_valid_article(a.body)]

    if not valid_items:
        logger.info("No work found.")
        return

    # Phase 2 & 3: Process and Commit in chunks
    for i in range(0, len(valid_items), BATCH_SIZE):
        chunk = valid_items[i : i + BATCH_SIZE]
        try:
            raw = call_model(build_prompt(chunk))
            parsed = parse_json_response(raw)
            if not parsed: continue

            # Fresh session per chunk commit
            with SessionLocal() as db_write:
                for item in parsed:
                    article = db_write.get(RSSArticle, item.get("id"))
                    if article:
                        article.ai_summary = item.get("summary")
                        article.bias_label = item.get("bias")
                db_write.commit()
                logger.info(f"Processed batch of {len(chunk)}.")
        except Exception as e:
            logger.error(f"Batch {i} failed: {e}")

if __name__ == "__main__":
    process_articles()