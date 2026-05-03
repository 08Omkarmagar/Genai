"""
agent.py — LangGraph-powered analysis agent for news story bias and polarization.

This module implements a stateful agent that analyzes a set of news articles (a "Story")
to detect bias, summarize content, and visualize ideological distribution. It uses 
LangGraph for workflow orchestration and Google Gemini (Gemma 3) for LLM-based analysis.

Key functionalities:
- Intent analysis of user queries
- Article body retrieval (DB fallback or live fetch)
- GDELT fallback for expanded research
- Batch summarization and bias scoring
- Parallel cross-examination and metric evaluation
- Multi-source synthesis and bias visualization
"""

from __future__ import annotations

# Standard library imports
import json
import logging
import os
import re
import time
import uuid
import random
import httpx
from typing import TypedDict, Annotated, List, Dict, Any, Literal

# Third-party library imports
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server-side chart generation
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from dotenv import load_dotenv
from google import genai
from google.genai import types
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel
from sqlalchemy.orm import Session

# Local project imports
from body_fetcher import _fetch_body
from database import engine
from models import RSSArticle
from utils import parse_robust_json, merge_dicts, add_lists
from schemas import (
    BiasReport, BatchArticleAnalysis, BatchAnalysisResult,
    RelationshipLink, CrossExaminationResult
)

# Initialize environment and logging
load_dotenv("../.env")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agent")

# ---------------------------------------------------------------------------
# LLM Client & Rate Limiting
# ---------------------------------------------------------------------------

client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
MODEL_NAME = "models/gemma-3-27b-it"  

class _RateLimiter:
    """
    Manages API call frequency to adhere to Gemini's free tier quotas.
    Uses a simple wait-and-retry strategy with exponential backoff.
    """

    def __init__(self, min_interval: float = 6.5, daily_max: int = 1_400):
        self._min_interval = min_interval
        self._daily_max = daily_max
        self._last_call = 0.0
        self._count = 0

    def call(self, prompt: str, retries: int = 5) -> str:
        """Executes a model call while enforcing time intervals and daily limits."""
        for attempt in range(retries):
            if self._count >= self._daily_max:
                raise RuntimeError("Daily Gemini API limit reached.")

            # Ensure minimum spacing between calls to avoid 429 errors
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
                # Exponential backoff with jitter
                time.sleep((2 ** attempt) + random.uniform(0, 1))

        raise RuntimeError("Max Gemini retries exceeded.")

# Singleton rate limiter instance
_limiter = _RateLimiter()

def call_gemini(prompt: str, retries: int = 5) -> str:
    """Convenience wrapper for rate-limited Gemini calls."""
    return _limiter.call(prompt, retries=retries)

# ---------------------------------------------------------------------------
# Agent State Definition
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    """
    The internal state managed by the LangGraph workflow.
    Uses Reducers (merge_dicts, add_lists) to handle parallel updates to the state.
    """
    topic: str
    urls: Annotated[List[str], add_lists]
    articles: Annotated[Dict[str, str], merge_dicts]      # Raw content
    summaries: Annotated[Dict[str, str], merge_dicts]     # AI-generated summaries
    bias_reports: Annotated[Dict[str, dict], merge_dicts] # Per-article bias analysis
    intent: Dict[str, Any]                                # Analysis of the user's query
    retry_count: Annotated[Dict[str, int], merge_dicts]   # Tracks attempts per node

    # Results populated after the parallel branches (evaluate & cross_examine) merge
    comparison: str
    balanced_brief: str
    visualization_path: str
    diversity_score: float
    confidence_score: float
    agreement_score: float
    is_polarized: bool
    relationships: Annotated[List[dict], add_lists]       # Source-to-source relationships
    errors: Annotated[List[str], add_lists]
    readability_ratio: float                             # Successfully parsed articles vs total

# ---------------------------------------------------------------------------
# Workflow Nodes (Processing Steps)
# ---------------------------------------------------------------------------

def analyze_query_node(state: AgentState) -> dict:
    """
    Categorizes the user's query and extracts key research questions.
    This helps the agent decide whether to go for a deep search or a quick summary.
    """
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
    """
    Retrieves the full text of articles.
    Prioritizes the local DB; if missing, triggers a fresh scrape with quality scoring.
    """
    new_urls = [u for u in state["urls"] if u not in state["articles"]]
    logger.info(f"--- [NODE: fetch_bodies] Fetching content for {len(new_urls)} new URLs ---")
    articles_text: Dict[str, str] = {}

    with Session(engine) as session:
        for url in new_urls:
            row = session.query(RSSArticle).filter(RSSArticle.url == url).first()
            if row and row.body:
                articles_text[url] = row.body[:8_000] # Cap length to avoid context window issues
            else:
                # Prepare metadata for GDELT-sourced articles
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
                        # Update title if it was previously a placeholder
                        if row.outlet == "GDELT" or not row.title:
                            row.title = meta.get("title", row.title)

                    row.body = body
                    row.body_quality = score
                    row.body_fetched = True
                    session.commit()

    # Track how many requested articles were actually successfully read
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
    """
    Research fallback: Queries the GDELT DOC API to find additional articles
    when the initial source list is insufficient or of low quality.
    """
    topic = state["topic"]
    logger.info(f"--- [NODE: gdelt_fetch] Falling back to GDELT for topic: '{topic}' ---")

    # URL encode topic for API request
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
                    
                    # Persist metadata even before the body is fetched
                    row = session.query(RSSArticle).filter_by(url=art_url).first()
                    if not row:
                        row = RSSArticle(
                            url=art_url,
                            outlet=item.get("sourcecountry", "GDELT"),
                            country=item.get("sourcecountry", "Unknown")[:2].upper(),
                            bias="Unknown",
                            title=item.get("title", topic),
                            published=item.get("seoname", ""),
                        )
                        session.add(row)
                    
                    # Store structured metadata to pass to the fetch node
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
    """
    Performs AI summarization and bias scoring for articles.
    Processes in batches of 4 to balance speed and context window limits.
    """
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

            # Validate against Pydantic model for type safety
            result = BatchAnalysisResult.model_validate(data)

            with Session(engine) as session:
                for item in result.articles:
                    url = item.url
                    new_summaries[url] = item.summary
                    report = item.bias_report.model_dump()
                    report["label"] = item.bias_report.political_alignment
                    new_bias[url] = report

                    # Sync AI results back to DB for future caching
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
    """
    Computes aggregate metrics (diversity, agreement, polarization) for the story.
    These scores drive the visualization and high-level story cards.
    """
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
    
    # NOTE: Scoring logic is heuristic. Diversity 1.0 means all 3 alignments (L/C/R) are present.
    diversity = len(unique) / 3.0
    top = max(unique, key=alignments.count)
    agreement = alignments.count(top) / len(alignments)
    avg_conf = sum(r.get("confidence", 0.0) for r in reports) / len(reports)
    
    # Polarized if both ideological extremes are present
    is_split = "Left" in unique and "Right" in unique

    return {
        "diversity_score": diversity,
        "agreement_score": agreement,
        "confidence_score": avg_conf,
        "is_polarized": is_split,
    }

def cross_examine_node(state: AgentState) -> dict:
    """
    Identifies discursive relationships between sources (support, contradiction, etc.).
    Helps identify when outlets are framing the same facts in divergent ways.
    """
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

def merge_parallel_node(state: AgentState) -> dict:
    """
    Critical Fan-In Node.
    
    LangGraph requires an explicit merge point when two parallel branches finish.
    This ensures both 'evaluate' and 'cross_examine' results are committed to 
    the state before 'synthesize' starts, preventing data race conditions.
    """
    return {}   # State is automatically merged by LangGraph reducers

def synthesize_node(state: AgentState) -> dict:
    """
    Generates a neutral, consolidated brief from all available sources.
    Fails gracefully if no usable content was found during the pipeline.
    """
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
        # Sanitization: Ensure the model didn't just repeat the prompt instructions
        if any(p in content.lower() for p in ("paste the summaries", "please provide")):
            content = "Reliable synthesis could not be generated from the available source text."
        
        # Strip any markdown headers that might have snuck in
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
    """
    Generates a bias-score distribution chart.
    Saves directly to the frontend's public directory for instant UI updates.
    """
    print("[NODE: visualize] Generating bias distribution chart...")
    reports = state["bias_reports"]
    if not reports:
        return {}

    # Extract domain name for cleaner X-axis labels
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
    
    # Custom color palette matching the project's design system
    sns.barplot(
        x="Source",
        y="Score",
        hue="Alignment",
        data=df,
        palette={"Left": "#6A7EFC", "Center": "#EDF2F6", "Right": "#FF5656"},
    )
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Generate unique filename to avoid browser caching issues
    filename = f"bias_{uuid.uuid4().hex[:6]}.png"
    out_path = os.path.join(
        os.getcwd(), "..", "frontend", "react", "public", "charts", filename
    )
    
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path)
    plt.close()

    return {"visualization_path": f"/charts/{filename}"}

# ---------------------------------------------------------------------------
# Routing Logic (Conditional Edges)
# ---------------------------------------------------------------------------

def route_after_fetch(state: AgentState) -> Literal["retry", "gdelt", "continue"]:
    """
    Decides whether to retry fetching, fall back to GDELT research, or proceed.
    Logic is based on 'readability_ratio' (successfully parsed content vs requested).
    """
    ratio = state.get("readability_ratio", 0.0)
    fallback_count = state.get("retry_count", {}).get("fallback", 0)
    fetch_count = state.get("retry_count", {}).get("fetch", 0)

    print(f"[EDGE: route_after_fetch] Ratio: {ratio:.2f}, Fallback: {fallback_count}, Fetch: {fetch_count}")

    # Case A: Starting with 0 URLs (e.g. custom search query) -> Go to GDELT research
    if len(state.get("urls", [])) == 0 and fallback_count == 0:
        print("  -> No initial URLs provided. Routing to GDELT for search.")
        return "gdelt"

    # Case B: Sufficient content fetched -> Proceed to AI analysis
    if ratio >= 0.4:
        print("  -> Quality OK. Continuing to analysis.")
        return "continue"

    # Case C: Low quality but haven't researched GDELT yet -> Trigger Fallback
    if ratio < 0.4 and fallback_count == 0:
        print("  -> Quality LOW. Routing to GDELT fallback.")
        return "gdelt"

    # Case D: Hard failure on fetch -> Single retry before moving on
    if ratio == 0 and fetch_count < 2:
         print("  -> No articles found. Retrying basic fetch.")
         return "retry"

    print("  -> Continuing with available content.")
    return "continue"

def route_post_synthesis(state: AgentState) -> Literal["retry", "visualize", "end"]:
    """
    Validates synthesis quality and determines if visualization is possible.
    """
    # 1. Quality Gate: Retry synthesis if the model returned an empty/malformed response
    answer = state.get("balanced_brief", "")
    if not answer or len(answer.strip()) < 60:
        count = state.get("retry_count", {}).get("synthesize", 0)
        if count <= 1:
            print(f"[EDGE: post_synthesis] Synthesis too short ({len(answer)} chars). Retrying (Attempt 1/1)...")
            return "retry"
        print(f"[EDGE: post_synthesis] Synthesis still short but max retries reached.")

    # 2. Visualization Gate: Only route to visualize if we have bias metrics to show
    if state.get("bias_reports"):
        print("[EDGE: post_synthesis] Quality OK. Routing to visualize.")
        return "visualize"

    print("[EDGE: post_synthesis] Quality OK. No bias reports. Ending.")
    return "end"

def should_cross_examine(state: AgentState) -> Literal["cross_examine", "merge"]:
    """Skip relationship detection if there is only one source to analyze."""
    if len(state.get("summaries", {})) > 1:
        print("[EDGE: cross_examine_gate] Multiple sources found. Routing to cross_examine.")
        return "cross_examine"
    print("[EDGE: cross_examine_gate] Single or no source. Skipping cross_examine.")
    return "merge"

def route_analysis_depth(state: AgentState) -> Literal["quick", "deep"]:
    """
    Diverts flow based on the categorized intent of the query.
    Informational/Fact-check intents always prioritize fresh research (fetch).
    """
    if len(state.get("articles", {})) == 0:
        print("[EDGE: route_depth] No article content available. Routing to fetch/research.")
        return "quick"

    intent = state.get("intent", {}).get("category", "informational")
    print(f"[EDGE: route_depth] Intent: {intent} -> Routing to: {'fetch' if intent in ['fact-check', 'informational'] else 'analyze'}")
    
    if intent in ["fact-check", "informational"]:
        return "quick"
    return "deep"

# ---------------------------------------------------------------------------
# Graph Construction (LangGraph)
# ---------------------------------------------------------------------------

def build_agent():
    """Compiles the state machine orchestration logic."""
    builder = StateGraph(AgentState)

    # Core Pipeline
    builder.add_node("analyze_query", analyze_query_node)
    builder.add_node("fetch", fetch_bodies_node)
    builder.add_node("gdelt_fetch", gdelt_fetch_node)
    builder.add_node("analyze", batch_analyze_node)

    # Parallel branches for deep metrics
    builder.add_node("evaluate", evaluate_metrics_node)
    builder.add_node("cross_examine", cross_examine_node)

    # Merging and Synthesis
    builder.add_node("merge", merge_parallel_node)
    builder.add_node("synthesize", synthesize_node)
    builder.add_node("visualize", visualize_node)

    # Definitive Edges
    builder.add_edge(START, "analyze_query")
    builder.add_edge("gdelt_fetch", "fetch")
    builder.add_edge("evaluate", "merge")
    builder.add_edge("cross_examine", "merge")
    builder.add_edge("merge", "synthesize")
    builder.add_edge("visualize", END)

    # Conditional Routing Edges
    builder.add_conditional_edges(
        "analyze_query",
        route_analysis_depth,
        {"quick": "fetch", "deep": "analyze"}
    )

    builder.add_conditional_edges(
        "fetch",
        route_after_fetch,
        {"retry": "fetch", "gdelt": "gdelt_fetch", "continue": "analyze"}
    )

    builder.add_conditional_edges(
        "analyze",
        should_cross_examine,
        {"cross_examine": "cross_examine", "merge": "merge"}
    )

    builder.add_conditional_edges(
        "synthesize",
        route_post_synthesis,
        {"retry": "synthesize", "visualize": "visualize", "end": END}
    )

    return builder.compile()

# Global compiled agent instance
agent_executor = build_agent()

# ---------------------------------------------------------------------------
# Public API Entry Point
# ---------------------------------------------------------------------------

def run_agent(topic: str, urls: List[str], prefetched_bodies: Dict[str, str] | None = None) -> dict:
    """
    Orchestrates the execution of the bias analysis agent for a given story.
    
    Args:
        topic: The headline or research topic.
        urls: A list of article URLs to analyze.
        prefetched_bodies: Optional pre-loaded content to bypass the fetch stage.
    """
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
        # NOTE: LangGraph's .invoke is synchronous in this context but can be async if needed.
        out = agent_executor.invoke(initial_state)
        
        # Flatten the internal state into a clean dictionary for the FastAPI frontend
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
        logger.error(f"[run_agent] Critical pipeline failure: {exc}")
        return {"errors": [str(exc)]}