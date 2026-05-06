"""
main.py — FastAPI entry point and orchestration layer for the NewsHere project.

This module defines the RESTful API endpoints used by the React frontend to interact 
with news articles, stories, and the AI analysis agent. It handles routing, 
middleware, database session management, and background task triggering.

Key Route Groups:
- Articles: Retrieval and filtering of individual news sources.
- Stories: Clustered events with keyword and semantic search capabilities.
- Analysis: Deep-dive bias detection and cross-examination via the AI Agent.
- Ingestion: Synchronous triggers for RSS polling and body extraction.
- Monitoring: Telemetry and logs for backend jobs.
"""

from constants import STOP_WORDS, PAGE_SIZE
from rss_fetcher import run_rss_collection
from models import BiasAnalysisReport, FetchLog, RSSArticle, Story, StoryArticle, init_db
from database import engine, get_db
from clustering_service import ClusteringService, get_embedding_model
from body_fetcher import run_body_fetch
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, text
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Query, Depends
import asyncio
import warnings
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

# Suppress common charset-normalizer warnings from third-party libraries
warnings.filterwarnings(
    "ignore",
    message=".*urllib3.*charset_normalizer.*",
)


# Local service imports

# ---------------------------------------------------------------------------
# Application Lifecycle & Middleware
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles startup events like database initialization."""
    init_db()
    yield

app = FastAPI(title="NewsHere API", lifespan=lifespan)

# NOTE: Configuring CORS to allow the React development server to communicate with the API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Article Management Routes
# ---------------------------------------------------------------------------


@app.get("/articles")
def get_articles(
    q: Optional[str] = None,
    outlet: Optional[str] = None,
    bias: Optional[str] = None,
    country: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(PAGE_SIZE, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_db)
):
    """
    Retrieves a list of articles with multi-parameter filtering.
    Only returns articles that have had their body successfully fetched.
    """
    query = session.query(
        RSSArticle.id, RSSArticle.outlet, RSSArticle.bias, RSSArticle.country,
        RSSArticle.title, RSSArticle.url, RSSArticle.summary, RSSArticle.published,
        RSSArticle.fetched_at, RSSArticle.body_fetched,
    )

    # Content quality filter
    query = query.filter(
        RSSArticle.body_fetched == True,
        RSSArticle.body.is_not(None),
        RSSArticle.body != ""
    )

    # Keyword search (Title or Summary)
    if q and q.strip():
        search_term = f"%{q.strip().lower()}%"
        query = query.filter(
            or_(
                func.lower(RSSArticle.title).like(search_term),
                func.lower(RSSArticle.summary).like(search_term),
            )
        )

    # Categorical filters
    if outlet:
        query = query.filter(RSSArticle.outlet == outlet)
    if bias:
        query = query.filter(RSSArticle.bias == bias)
    if country:
        query = query.filter(RSSArticle.country == country)

    # Date range filters
    if date_from:
        try:
            query = query.filter(RSSArticle.fetched_at >=
                                 datetime.fromisoformat(date_from))
        except ValueError:
            pass
    if date_to:
        try:
            date_to_end = datetime.fromisoformat(date_to) + timedelta(days=1)
            query = query.filter(RSSArticle.fetched_at < date_to_end)
        except ValueError:
            pass

    rows = query.order_by(RSSArticle.fetched_at.desc()
                          ).offset(offset).limit(limit).all()
    return [row._asdict() for row in rows]


@app.get("/articles/{article_id}")
def get_article(article_id: str, session: Session = Depends(get_db)):
    """Retrieves full details for a single article, including its scraped body text."""
    article = session.get(RSSArticle, article_id)
    if not article:
        raise HTTPException(status_code=404, detail="Article not found")

    return {
        "id": article.id, "outlet": article.outlet, "bias": article.bias,
        "country": article.country, "title": article.title, "url": article.url,
        "summary": article.summary, "published": article.published,
        "fetched_at": article.fetched_at, "body": article.body,
        "body_fetched": article.body_fetched, "content_type": article.content_type,
    }


@app.get("/outlets")
def get_outlets(session: Session = Depends(get_db)):
    """Returns a unique list of news outlets represented in the database."""
    outlets = session.query(RSSArticle.outlet).distinct().order_by(
        RSSArticle.outlet).all()
    return [outlet[0] for outlet in outlets]

# ---------------------------------------------------------------------------
# Story & Clustering Routes
# ---------------------------------------------------------------------------


def get_query_embedding(query: str) -> str:
    """Generates a vector string for pgvector search."""
    vector = [float(x) for x in get_embedding_model().encode(query)]
    return str(vector)


def tokenize_query(query: str) -> list[str]:
    """Cleans and tokenizes search queries, stripping common stop words."""
    raw_query = query.strip().lower()
    tokens = [t for t in raw_query.split() if len(
        t) > 2 and t not in STOP_WORDS]
    return tokens or [raw_query]


@app.get("/stories")
@app.get("/search/stories")
def get_stories(q: Optional[str] = None, limit: int = Query(20, ge=1, le=50), session: Session = Depends(get_db)):
    """
    Retrieves news stories (clusters).
    If a query 'q' is provided, performs a hybrid Keyword + Semantic search.
    If no query is provided, returns the most recently updated stories.
    """
    # CASE 1: Recent Stories (Default)
    if not q or not q.strip():
        stories = (
            session.query(Story)
            .join(StoryArticle, StoryArticle.story_id == Story.id)
            .join(RSSArticle, RSSArticle.id == StoryArticle.article_id)
            .filter(RSSArticle.body_fetched == True)
            .group_by(Story.id)
            .order_by(Story.updated_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "id": s.id, "title": s.title, "summary": s.summary,
                "article_count": s.article_count, "bias_distribution": s.bias_distribution,
                "updated_at": s.updated_at, "matched_articles_count": s.article_count,
            } for s in stories
        ]

    # CASE 2:  Search
    tokens = tokenize_query(q)
    min_required_tokens = len(tokens) if len(tokens) <= 2 else len(tokens) - 1
    query_vector_str = get_query_embedding(q)

    # Raw SQL for pgvector performance (ordering by cosine distance)
    # This block performs keyword intersection followed by semantic ranking.
    token_match_cases = " + ".join(
        [f"(CASE WHEN LOWER(a.title) LIKE :t{i} OR LOWER(s.title) LIKE :t{i} THEN 1 ELSE 0 END)" for i in range(len(tokens))])
    token_where = " OR ".join(
        [f"LOWER(a.title) LIKE :t{i} OR LOWER(s.title) LIKE :t{i}" for i in range(len(tokens))])
    token_params = {f"t{i}": f"%{token}%" for i, token in enumerate(tokens)}
    token_params.update(
        {"v": query_vector_str, "limit": limit, "min_tokens": min_required_tokens})

    keyword_stmt = text(f"""
        WITH scores AS (
            SELECT sa.story_id, ({token_match_cases}) as hit FROM story_articles sa
            JOIN rss_articles a ON sa.article_id = a.id JOIN stories s ON sa.story_id = s.id
            WHERE ({token_where}) AND a.body_fetched = TRUE
        )
        SELECT s.*, (s.centroid_vector <=> CAST(:v AS vector)) as distance, MAX(scores.hit) as best_hit FROM stories s
        JOIN scores ON s.id = scores.story_id GROUP BY s.id HAVING MAX(scores.hit) >= :min_tokens
        ORDER BY best_hit DESC, distance ASC LIMIT :limit
    """)

    try:
        results = session.execute(keyword_stmt, token_params).fetchall()
        if results:
            return [dict(row._mapping) for row in results]
    except Exception:
        pass

    semantic_stmt = text("""
        SELECT s.*, (s.centroid_vector <=> CAST(:v AS vector)) as distance FROM stories s
        WHERE s.centroid_vector IS NOT NULL AND (s.centroid_vector <=> CAST(:v AS vector)) < 0.75
        ORDER BY distance ASC LIMIT :limit
    """)
    results = session.execute(
        semantic_stmt, {"v": query_vector_str, "limit": limit}).fetchall()
    return [dict(row._mapping) for row in results]


@app.get("/stories/{story_id}")
def get_story(story_id: str, session: Session = Depends(get_db)):
    """Returns details for a specific story cluster and its constituent articles."""
    story = session.get(Story, story_id)
    if not story:
        raise HTTPException(status_code=404, detail="Story not found")

    ready_articles = (
        session.query(RSSArticle).join(StoryArticle).filter(
            StoryArticle.story_id == story_id, RSSArticle.body_fetched == True).all()
    )

    # Recompute live bias distribution to ensure UI reflects newly analyzed articles
    from collections import Counter
    live_bias = Counter(a.bias for a in ready_articles if a.bias)

    return {
        "id": story.id, "title": story.title, "summary": story.summary,
        "article_count": len(ready_articles), "bias_distribution": dict(live_bias),
        "articles": [{"id": a.id, "title": a.title, "outlet": a.outlet, "bias": a.bias} for a in ready_articles]
    }

# ---------------------------------------------------------------------------
# AI Analysis & Deep-Dive Routes
# ---------------------------------------------------------------------------


@app.get("/stories/{story_id}/analysis")
def get_story_analysis(story_id: str, session: Session = Depends(get_db)):
    """
    Triggers the LangGraph agent for deep bias analysis.
    Implements a 24-hour cache to avoid redundant and costly LLM calls.
    """
    from agent import run_agent
    story = session.get(Story, story_id)
    if not story:
        raise HTTPException(status_code=404, detail="Story not found")

    # Check for valid cache
    day_ago = datetime.utcnow() - timedelta(days=1)
    cached = session.query(BiasAnalysisReport).filter_by(
        story_id=story_id).filter(BiasAnalysisReport.created_at >= day_ago).first()
    if cached:
        return cached.raw_result

    articles = (
        session.query(RSSArticle)
        .join(StoryArticle)
        .filter(StoryArticle.story_id == story_id, RSSArticle.body_fetched == True)
        .all()
    )
    if not articles:
        raise HTTPException(
            status_code=400, detail="No readable content to analyze.")

    # Execute Agent
    result = run_agent(topic=story.title, urls=[
                       a.url for a in articles], prefetched_bodies={a.url: a.body for a in articles})

    # Update cache and stats
    report = BiasAnalysisReport(
        story_id=story_id, topic=story.title, raw_result=result, created_at=datetime.utcnow())
    session.add(report)
    session.commit()
    return result


class AnalyzeRequest(BaseModel):
    topic: str
    urls: list[str] = []


@app.post("/analyze")
def trigger_analysis(req: AnalyzeRequest, session: Session = Depends(get_db)):
    """
    Ad-hoc analysis of a custom topic or URL list.
    Saves results as a new 'investigative' story on the dashboard.
    """
    from agent import run_agent
    urls = req.urls
    if not urls and req.topic:
        # Search for likely candidates in our local DB first
        candidates = session.query(RSSArticle).filter(
            RSSArticle.title.ilike(f"%{req.topic}%")).limit(10).all()
        urls = [c.url for c in candidates]

    result = run_agent(topic=req.topic, urls=urls)

    # NOTE: We persist these ad-hoc results as a Story so they are accessible via the homepage.
    story = Story(title=req.topic, summary=result.get(
        "balanced_brief", "")[:500], category="investigative")
    session.add(story)
    session.commit()
    return result

# ---------------------------------------------------------------------------
# Background Ingestion & Maintenance
# ---------------------------------------------------------------------------


@app.post("/fetch/rss")
def trigger_rss_fetch():
    """Manually triggers the global RSS collection pipeline."""
    try:
        return run_rss_collection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/fetch/body")
def trigger_body_fetch():
    """Manually triggers the full-text extraction pipeline for unread articles."""
    try:
        return run_body_fetch()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/logs")
def get_logs(outlet: Optional[str] = None, status: Optional[str] = None, limit: int = 50, session: Session = Depends(get_db)):
    """Retrieves telemetry logs for backend jobs."""
    query = session.query(FetchLog)
    if outlet:
        query = query.filter_by(outlet=outlet)
    if status:
        query = query.filter_by(status=status)
    logs = query.order_by(FetchLog.run_at.desc()).limit(limit).all()
    return [dict(l.__dict__) for l in logs]
