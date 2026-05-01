"""
agent.py — LangGraph-based per-story bias analysis agent.

Key fixes vs original:
  1. SDK UPDATE — Switched from deprecated `google-generativeai` to
     modern `google-genai` SDK for better performance and long-term
     support.

  2. LangGraph fan-in bug — both `evaluate` and `cross_examine` had edges
     into `synthesize`.  LangGraph requires an explicit fan-in node when
     two parallel branches merge; otherwise the second branch's output
     silently overwrites the first.  Added a `merge_parallel_node` to
     collect both results before synthesis.

  3. Removed unused imports (deque, threading, operator, date).

  4. Rate-limit state (LAST_CALL_TIME etc.) is now encapsulated in a
     small class instead of module-level mutable globals.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
import random
import httpx

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from dotenv import load_dotenv
from typing import TypedDict, Annotated, List, Dict, Any, Literal

from google import genai
from google.genai import types
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel
from sqlalchemy.orm import Session

from body_fetcher import _fetch_body
from database import engine
from models import RSSArticle
from utils import parse_robust_json, merge_dicts, add_lists
from schemas import (
    BiasReport, BatchArticleAnalysis, BatchAnalysisResult,
    RelationshipLink, CrossExaminationResult
)

load_dotenv("../.env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agent")

# ---------------------------------------------------------------------------
# Gemini client
# ---------------------------------------------------------------------------

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
MODEL_NAME = "models/gemma-3-27b-it"  # Using Gemma 3 for analysis as requested


class _RateLimiter:
    """Simple token-bucket style rate limiter for the Gemini free tier."""

    def __init__(self, min_interval: float = 6.5, daily_max: int = 1_400):
        self._min_interval = min_interval
        self._daily_max = daily_max
        self._last_call = 0.0
        self._count = 0

    def call(self, prompt: str, retries: int = 5) -> str:
        for attempt in range(retries):
            if self._count >= self._daily_max:
                raise RuntimeError("Daily Gemini API limit reached.")

            elapsed = time.time() - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)

            try:
                response = client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt
                )
                self._count += 1
                self._last_call = time.time()
                return response.text
            except Exception as exc:
                logger.warning(f"Gemini attempt {attempt + 1} failed: {exc}")
                time.sleep((2 ** attempt) + random.uniform(0, 1))

        raise RuntimeError("Max Gemini retries exceeded.")


_limiter = _RateLimiter()


def call_gemini(prompt: str, retries: int = 5) -> str:
    return _limiter.call(prompt, retries=retries)





# ---------------------------------------------------------------------------
# Agent state
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    topic: str
    urls: Annotated[List[str], add_lists]
    articles: Annotated[Dict[str, str], merge_dicts]
    summaries: Annotated[Dict[str, str], merge_dicts]
    bias_reports: Annotated[Dict[str, dict], merge_dicts]
    intent: Dict[str, Any]
    retry_count: Annotated[Dict[str, int], merge_dicts]

    # Populated after parallel branches merge
    comparison: str
    balanced_brief: str
    visualization_path: str
    diversity_score: float
    confidence_score: float
    agreement_score: float
    is_polarized: bool
    relationships: Annotated[List[dict], add_lists]
    errors: Annotated[List[str], add_lists]
    readability_ratio: float





# ---------------------------------------------------------------------------
# Graph nodes
# ---------------------------------------------------------------------------

def analyze_query_node(state: AgentState) -> dict:
    """Extract intent and core questions from the topic query."""
    topic = state["topic"]
    logger.info(f"--- [NODE: analyze_query] Analyzing topic: '{topic}' ---")

    prompt = f"""
Analyze the user's search topic and extract the primary intent.
Categorize into: 'informational', 'comparative', 'investigative', or 'fact-check'.
Also identify 2-3 core questions the analysis should address.

TOPIC: {topic}

Output exactly as a JSON object:
{{
  "category": "...",
  "core_questions": ["...", "..."],
  "reasoning": "..."
}}
"""
    try:
        raw = call_gemini(prompt)
        data = parse_robust_json(raw)
        res = data if data else {"category": "informational", "core_questions": [], "reasoning": "Fallback"}
        logger.info(f"--- [NODE: analyze_query] Intent categorized as: {res.get('category')} ---")
        return {"intent": res}
    except Exception as exc:
        logger.error(f"--- [NODE: analyze_query] Error: {exc} ---")
        return {"intent": {"category": "informational", "core_questions": [], "reasoning": "Error"}}


def fetch_bodies_node(state: AgentState) -> dict:
    """Load article bodies from DB or live fetch."""
    new_urls = [u for u in state["urls"] if u not in state["articles"]]
    logger.info(f"--- [NODE: fetch_bodies] Fetching content for {len(new_urls)} new URLs ---")
    articles_text: Dict[str, str] = {}

    with Session(engine) as session:
        for url in new_urls:
            row = session.query(RSSArticle).filter(RSSArticle.url == url).first()
            if row and row.body:
                articles_text[url] = row.body[:8_000]
            else:
                # Try to extract metadata if it was passed from GDELT
                meta = {"title": state["topic"], "source": "GDELT"}
                existing_meta = state["articles"].get(url)
                if existing_meta and existing_meta.startswith("{"):
                    try:
                        meta = json.loads(existing_meta)
                    except: pass

                body, score, _ = _fetch_body(url)
                if body:
                    articles_text[url] = body[:8_000]
                    if not row:
                        row = RSSArticle(
                            url=url,
                            outlet=meta.get("source", "GDELT"),
                            bias="Unknown",
                            title=meta.get("title", state["topic"]),
                            published=meta.get("published", ""),
                        )
                        session.add(row)
                    else:
                        # Update title if it was a placeholder
                        if row.outlet == "GDELT" or not row.title:
                            row.title = meta.get("title", row.title)

                    row.body = body
                    row.body_quality = score
                    row.body_fetched = True
                    session.commit()

    # Calculate readability ratio for the whole batch
    total_requested = len(state["urls"])
    total_valid = len(state["articles"]) + len(articles_text)
    ratio = total_valid / total_requested if total_requested > 0 else 1.0

    retries = state.get("retry_count", {}).get("fetch", 0)
    logger.info(f"--- [NODE: fetch_bodies] DONE. Valid bodies: {len(articles_text)} (Ratio: {ratio:.2f}) ---")
    print(f"\n>>> [NODE: fetch_bodies] Valid: {len(articles_text)} articles | Ratio: {ratio:.2f} <<<")
    return {
        "articles": articles_text, 
        "readability_ratio": ratio,
        "retry_count": {"fetch": retries + 1}
    }


def gdelt_fetch_node(state: AgentState) -> dict:
    """Fallback data source: Call GDELT DOC API to find relevant articles."""
    topic = state["topic"]
    logger.info(f"--- [NODE: gdelt_fetch] Falling back to GDELT for topic: '{topic}' ---")

    # GDELT DOC API: artlist mode
    # We query the topic directly.
    query = topic.replace(" ", "%20")
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=artlist&maxrecords=10&format=json"

    new_articles: Dict[str, str] = {}
    new_urls: List[str] = []

    try:
        resp = httpx.get(url, timeout=10.0)
        if resp.status_code == 200:
            data = resp.json()
            gdelt_list = data.get("articles", [])
            with Session(engine) as session:
                for item in gdelt_list:
                    art_url = item.get("url")
                    if not art_url:
                        continue
                    
                    new_urls.append(art_url)
                    
                    # 1. Check if article already exists
                    row = session.query(RSSArticle).filter_by(url=art_url).first()
                    if not row:
                        # 2. Create new record with all GDELT metadata
                        row = RSSArticle(
                            url=art_url,
                            outlet=item.get("sourcecountry", "GDELT"),
                            country=item.get("sourcecountry", "Unknown")[:2].upper(),
                            bias="Unknown",
                            title=item.get("title", topic),
                            published=item.get("seoname", ""),
                        )
                        session.add(row)
                    
                    # Pass structured metadata for the next node
                    new_articles[art_url] = json.dumps({
                        "title": item.get("title", topic),
                        "source": item.get("sourcecountry", "GDELT"),
                        "published": item.get("seoname", ""),
                        "is_gdelt": True
                    })
                
                session.commit()
        else:
            logger.warning(f"GDELT API returned status {resp.status_code}")
    except Exception as exc:
        logger.error(f"gdelt_fetch_node error: {exc}")

    retries = state.get("retry_count", {}).get("fallback", 0)
    logger.info(f"--- [NODE: gdelt_fetch] Found {len(new_urls)} URLs on GDELT ---")
    print(f">>> [NODE: gdelt_fetch] Success: Found {len(new_urls)} URLs on GDELT <<<")
    return {
        "articles": new_articles,
        "urls": new_urls,
        "retry_count": {"fallback": retries + 1}
    }


def batch_analyze_node(state: AgentState) -> dict:
    """Summarise and bias-score articles in batches of 4."""
    topic = state["topic"]
    pending = [u for u in state["articles"] if u not in state["summaries"]]
    logger.info(f"--- [NODE: batch_analyze] Processing {len(pending)} articles for topic: '{topic}' ---")

    new_summaries: Dict[str, str] = {}
    new_bias: Dict[str, dict] = {}

    for i in range(0, len(pending), 4):
        batch_urls = pending[i: i + 4]
        combined = "".join(
            f"\n\n--- ARTICLE URL: {u} ---\n{state['articles'][u]}\n"
            for u in batch_urls
        )

        prompt = f"""
Analyze the following news articles about {topic}.
Output exactly as a JSON object with an 'articles' key containing a list.
Each element must have: url, summary, bias_report.
bias_report fields:
  emotional_language_used (bool), loaded_terms (list), missing_viewpoints (list),
  bias_score (int 1-10), political_alignment (Left|Center|Right),
  bias_reasoning (str), confidence (float), ambiguity_detected (bool).

ARTICLES:
{combined}
"""

        try:
            raw = call_gemini(prompt)
            data = parse_robust_json(raw)
            if not data:
                continue

            result = BatchAnalysisResult.model_validate(data)

            with Session(engine) as session:
                for item in result.articles:
                    url = item.url
                    new_summaries[url] = item.summary
                    report = item.bias_report.model_dump()
                    report["label"] = item.bias_report.political_alignment
                    new_bias[url] = report

                    row = session.query(RSSArticle).filter_by(url=url).first()
                    if row:
                        row.ai_summary = item.summary
                        row.bias_score = item.bias_report.bias_score
                        row.bias_label = item.bias_report.political_alignment
                        row.bias_reasoning = item.bias_report.bias_reasoning
                        row.confidence_score = item.bias_report.confidence
                session.commit()

        except Exception as exc:
            logger.error(f"batch_analyze_node error: {exc}")

    print(f">>> [NODE: batch_analyze] Batch Complete. Summaries: {len(new_summaries)} <<<")
    return {"summaries": new_summaries, "bias_reports": new_bias}


def evaluate_metrics_node(state: AgentState) -> dict:
    """Compute diversity / agreement / polarisation metrics."""
    print("[NODE: evaluate] Computing bias metrics...")
    reports = list(state["bias_reports"].values())
    if not reports:
        return {
            "diversity_score": 0.0,
            "agreement_score": 0.0,
            "confidence_score": 0.0,
            "is_polarized": False,
        }

    alignments = [r["political_alignment"] for r in reports]
    unique = set(alignments)
    diversity = len(unique) / 3.0
    top = max(unique, key=alignments.count)
    agreement = alignments.count(top) / len(alignments)
    avg_conf = sum(r.get("confidence", 0.0) for r in reports) / len(reports)
    is_split = "Left" in unique and "Right" in unique

    return {
        "diversity_score": diversity,
        "agreement_score": agreement,
        "confidence_score": avg_conf,
        "is_polarized": is_split,
    }


def cross_examine_node(state: AgentState) -> dict:
    """Detect relationships (supports / contradicts / etc.) between articles."""
    summaries = state["summaries"]
    print(f"[NODE: cross_examine] Comparing {len(summaries)} source summaries...")
    if len(summaries) < 2:
        return {"relationships": []}

    combined = "".join(
        f"\nSOURCE: {url}\nSUMMARY: {s}\n" for url, s in summaries.items()
    )

    prompt = f"""
Compare these news summaries about a shared topic.
Output a JSON object with a 'links' key containing a list.
Each link: source_url, target_url, relationship_type (supports|contradicts|expands|divergent_framing),
strength (0.0-1.0), evidence (str).

SUMMARIES:
{combined}
"""

    try:
        raw = call_gemini(prompt)
        data = parse_robust_json(raw)
        if not data:
            raise ValueError("Could not parse JSON from cross-examine response.")
        result = CrossExaminationResult.model_validate(data)
        return {"relationships": [link.model_dump() for link in result.links]}
    except Exception as exc:
        logger.error(f"cross_examine_node error: {exc}")
        return {"relationships": [], "errors": [f"Cross-exam failed: {exc}"]}


# FIX: explicit fan-in node so both parallel branches are fully committed
# to state before synthesize runs.
def merge_parallel_node(state: AgentState) -> dict:
    """
    No-op merge point.

    LangGraph collects the outputs of evaluate_metrics_node and
    cross_examine_node here before proceeding to synthesize.
    Without this node, whichever branch finishes last would silently
    discard the other branch's updates.
    """
    return {}   # state already merged by LangGraph reducers


def synthesize_node(state: AgentState) -> dict:
    """Produce a concise, neutral multi-source synthesis."""
    summaries = state["summaries"]
    topic = state["topic"]
    print(f"[NODE: synthesize] Generating synthesis for topic: {topic}")

    if not summaries:
        return {
            "balanced_brief": (
                f"Analysis failed: no readable content from the "
                f"{len(state['urls'])} articles for this story."
            ),
            "comparison": "No content available.",
        }

    text_block = "\n\n".join(
        f"Source ({url}):\n{s}" for url, s in summaries.items()
    )

    prompt = f"""
You are a neutral news analyst. Write a CONCISE, NEUTRAL synthesis.

TOPIC: {topic}

SUMMARIES:
{text_block}

RULES:
1. Write 1-2 paragraph summary.
2. Include only confirmed or widely reported facts.
3. If multiple sources agree, reflect consensus.
4. If sources differ, briefly note the variation without speculation.
5. Prioritize the most recent and relevant developments.
6. Use neutral, factual language (e.g., "reports indicate", "according to sources").
7. No markdown, no bullet points.
8. Return ONLY the paragraph.
"""

    try:
        content = call_gemini(prompt)
        if any(p in content.lower() for p in ("paste the summaries", "please provide")):
            content = "Reliable synthesis could not be generated from the available source text."
        content = re.sub(r"^#+.*$", "", content, flags=re.MULTILINE).strip()
    except Exception as exc:
        logger.error(f"synthesize_node error: {exc}")
        content = "Error generating synthesis. Please check individual sources."

    retries = state.get("retry_count", {}).get("synthesize", 0)
    return {
        "balanced_brief": content,
        "comparison": "Consolidated perspectives analysed.",
        "retry_count": {"synthesize": retries + 1}
    }


def visualize_node(state: AgentState) -> dict:
    """Generate a bias-score bar chart and save it to the frontend public dir."""
    print("[NODE: visualize] Generating bias distribution chart...")
    reports = state["bias_reports"]
    if not reports:
        return {}

    data = [
        {
            "Source": url.split("/")[2] if "//" in url else url,
            "Score": r["bias_score"],
            "Alignment": r["political_alignment"],
        }
        for url, r in reports.items()
    ]

    df = pd.DataFrame(data)
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x="Source",
        y="Score",
        hue="Alignment",
        data=df,
        palette={"Left": "#6A7EFC", "Center": "#EDF2F6", "Right": "#FF5656"},
    )
    plt.xticks(rotation=45)
    plt.tight_layout()

    filename = f"bias_{uuid.uuid4().hex[:6]}.png"
    out_path = os.path.join(
        os.getcwd(), "..", "frontend", "react", "public", "charts", filename
    )
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path)
    plt.close()

    return {"visualization_path": f"/charts/{filename}"}


def route_after_fetch(state: AgentState) -> Literal["retry", "gdelt", "continue"]:
    """Routing after fetch based on quality and retries."""
    ratio = state.get("readability_ratio", 0.0)
    fallback_count = state.get("retry_count", {}).get("fallback", 0)
    fetch_count = state.get("retry_count", {}).get("fetch", 0)

    print(f"[EDGE: route_after_fetch] Ratio: {ratio:.2f}, Fallback: {fallback_count}, Fetch: {fetch_count}")

    # Rule 1: No articles initially provided -> GDELT
    if len(state.get("urls", [])) == 0 and fallback_count == 0:
        print("  -> No initial URLs provided. Routing to GDELT for search.")
        return "gdelt"

    # Rule 2: High quality -> Continue
    if ratio >= 0.4:
        print("  -> Quality OK. Continuing to analysis.")
        return "continue"

    # Rule 3: Low quality & Fallback not used -> GDELT
    if ratio < 0.4 and fallback_count == 0:
        print("  -> Quality LOW. Routing to GDELT fallback.")
        return "gdelt"

    # Rule 3: Low quality but fallback already used OR simple retry logic
    if ratio == 0 and fetch_count < 2:
         print("  -> No articles found. Retrying basic fetch.")
         return "retry"

    print("  -> Continuing with available content.")
    return "continue"


def route_post_synthesis(state: AgentState) -> Literal["retry", "visualize", "end"]:
    """Route after synthesis: check quality first, then check visualization requirements."""
    # 1. Quality Check (Retry once if too short)
    answer = state.get("balanced_brief", "")
    if not answer or len(answer.strip()) < 60:
        # The count was incremented inside the node, so 1 means first attempt just finished
        count = state.get("retry_count", {}).get("synthesize", 0)
        if count <= 1:
            print(f"[EDGE: post_synthesis] Synthesis too short ({len(answer)} chars). Retrying (Attempt 1/1)...")
            return "retry"
        print(f"[EDGE: post_synthesis] Synthesis still short ({len(answer)} chars) but max retries reached.")

    # 2. Visualization Gate
    if state.get("bias_reports"):
        print("[EDGE: post_synthesis] Quality OK. Routing to visualize.")
        return "visualize"

    print("[EDGE: post_synthesis] Quality OK. No bias reports. Ending.")
    return "end"


def should_cross_examine(state: AgentState) -> Literal["cross_examine", "merge"]:
    """Only run cross-examination if there are multiple summaries to compare."""
    if len(state.get("summaries", {})) > 1:
        print("[EDGE: cross_examine_gate] Multiple sources found. Routing to cross_examine.")
        return "cross_examine"
    print("[EDGE: cross_examine_gate] Single or no source. Skipping cross_examine.")
    return "merge"


    return "end"


def route_analysis_depth(state: AgentState) -> Literal["quick", "deep"]:
    """Route to fetch (quick) or direct analysis (deep) based on intent."""
    # Priority: If we have 0 article bodies, we MUST go to fetch
    # (This will either fetch bodies for existing URLs or trigger GDELT if 0 URLs)
    if len(state.get("articles", {})) == 0:
        print("[EDGE: route_depth] No article content available. Routing to fetch/research.")
        return "quick"

    intent = state.get("intent", {}).get("category", "informational")
    print(f"[EDGE: route_depth] Intent category: {intent} -> Routing to: {'fetch' if intent in ['fact-check', 'informational'] else 'analyze'}")
    if intent in ["fact-check", "informational"]:
        return "quick"
    return "deep"


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_agent():
    builder = StateGraph(AgentState)

    builder.add_node("analyze_query", analyze_query_node)
    builder.add_node("fetch", fetch_bodies_node)
    builder.add_node("gdelt_fetch", gdelt_fetch_node)
    builder.add_node("analyze", batch_analyze_node)

    # Parallel branches
    builder.add_node("evaluate", evaluate_metrics_node)
    builder.add_node("cross_examine", cross_examine_node)

    # FIX: explicit merge node before synthesize
    builder.add_node("merge", merge_parallel_node)

    builder.add_node("synthesize", synthesize_node)
    builder.add_node("visualize", visualize_node)

    builder.add_edge(START, "analyze_query")

    builder.add_conditional_edges(
        "analyze_query",
        route_analysis_depth,
        {
            "quick": "fetch",
            "deep": "analyze"
        }
    )

    builder.add_conditional_edges(
        "fetch",
        route_after_fetch,
        {
            "retry": "fetch",
            "gdelt": "gdelt_fetch",
            "continue": "analyze"
        }
    )

    builder.add_edge("gdelt_fetch", "fetch")

    # builder.add_edge("fetch", "analyze")  <-- Removed in favor of conditional edge

    # Fan-out from analyze
    builder.add_edge("analyze", "evaluate")
    builder.add_conditional_edges(
        "analyze",
        should_cross_examine,
        {
            "cross_examine": "cross_examine",
            "merge": "merge"
        }
    )

    # Fan-in — both branches converge on merge
    builder.add_edge("evaluate", "merge")
    builder.add_edge("cross_examine", "merge")

    builder.add_edge("merge", "synthesize")
    
    builder.add_conditional_edges(
        "synthesize",
        route_post_synthesis,
        {
            "retry": "synthesize",
            "visualize": "visualize",
            "end": END
        }
    )
    builder.add_edge("visualize", END)

    return builder.compile()


agent_executor = build_agent()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_agent(topic: str, urls: List[str], prefetched_bodies: Dict[str, str] | None = None) -> dict:
    """Run the full analysis graph for a topic and its article URLs."""
    initial_state: AgentState = {
        "topic": topic,
        "urls": urls,
        "articles": prefetched_bodies or {},
        "summaries": {},
        "bias_reports": {},
        "comparison": "",
        "balanced_brief": "",
        "visualization_path": "",
        "diversity_score": 0.0,
        "confidence_score": 0.0,
        "agreement_score": 0.0,
        "is_polarized": False,
        "relationships": [],
        "errors": [],
        "readability_ratio": 0.0,
    }

    try:
        out = agent_executor.invoke(initial_state)
        return {
            "summaries": out["summaries"],
            "bias_reports": out["bias_reports"],
            "comparison": out["comparison"],
            "balanced_brief": out["balanced_brief"],
            "visualization_path": out["visualization_path"],
            "metrics": {
                "diversity": out["diversity_score"],
                "confidence": out["confidence_score"],
                "agreement": out["agreement_score"],
                "is_polarized": out["is_polarized"],
            },
            "relationships": out.get("relationships", []),
            "errors": out["errors"],
        }
    except Exception as exc:
        logger.error(f"[run_agent] Critical failure: {exc}")
        return {"errors": [str(exc)]}