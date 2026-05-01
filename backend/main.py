import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
import warnings

warnings.filterwarnings(
    "ignore",
    message=".*urllib3.*charset_normalizer.*",
)

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session

from body_fetcher import run_body_fetch
from clustering_service import ClusteringService, get_embedding_model
from database import engine, get_db
from models import BiasAnalysisReport, FetchLog, RSSArticle, Story, StoryArticle, init_db
from rss_fetcher import run_rss_collection
from constants import STOP_WORDS, PAGE_SIZE



@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield

app = FastAPI(title="NewsHere API", lifespan=lifespan)

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
    """Return paginated articles with optional search, outlet, bias, country, and date filters."""
    print(f"\n>>> [API] GET /articles | q='{q}' | outlet='{outlet}' <<<")
    query = session.query(
        RSSArticle.id,
        RSSArticle.outlet,
        RSSArticle.bias,
        RSSArticle.country,
        RSSArticle.title,
        RSSArticle.url,
        RSSArticle.summary,
        RSSArticle.published,
        RSSArticle.fetched_at,
        RSSArticle.body_fetched,
    )

    # CORRECTED: Filter at the query level for ready articles
    query = query.filter(
        RSSArticle.body_fetched == True,
        RSSArticle.body.is_not(None),
        RSSArticle.body != ""
    )

    if q and q.strip():
        search_term = f"%{q.strip().lower()}%"
        query = query.filter(
            or_(
                func.lower(RSSArticle.title).like(search_term),
                func.lower(RSSArticle.summary).like(search_term),
            )
        )

    if outlet:
        query = query.filter(RSSArticle.outlet == outlet)
    if bias:
        query = query.filter(RSSArticle.bias == bias)
    if country:
        query = query.filter(RSSArticle.country == country)

    if date_from:
        try:
            date_from_dt = datetime.fromisoformat(date_from)
            query = query.filter(RSSArticle.fetched_at >= date_from_dt)
        except ValueError:
            pass

    if date_to:
        try:
            date_to_dt = datetime.fromisoformat(date_to)
            date_to_end = date_to_dt + timedelta(days=1)
            query = query.filter(RSSArticle.fetched_at < date_to_end)
        except ValueError:
            pass

    rows = (
        query.order_by(RSSArticle.fetched_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return [row._asdict() for row in rows]

@app.get("/articles/{article_id}")
def get_article(article_id: str, session: Session = Depends(get_db)):
    """Return one article with full body and content type."""
    article = session.get(RSSArticle, article_id)
    if not article:
        raise HTTPException(status_code=404, detail="Article not found")

    return {
        "id": article.id,
        "outlet": article.outlet,
        "bias": article.bias,
        "country": article.country,
        "title": article.title,
        "url": article.url,
        "summary": article.summary,
        "published": article.published,
        "fetched_at": article.fetched_at,
        "body": article.body,
        "body_fetched": article.body_fetched,
        "content_type": article.content_type,
    }

@app.get("/outlets")
def get_outlets(session: Session = Depends(get_db)):
    """Return sorted unique outlet names."""
    outlets = (
        session.query(RSSArticle.outlet)
        .distinct()
        .order_by(RSSArticle.outlet)
        .all()
    )
    return [outlet[0] for outlet in outlets]

@app.get("/logs")
def get_logs(
    outlet: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_db)
):
    """Return paginated fetch logs, newest first."""
    query = session.query(FetchLog)

    if outlet:
        query = query.filter(FetchLog.outlet == outlet)
    if status:
        query = query.filter(FetchLog.status == status)

    logs = (
        query.order_by(FetchLog.run_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return [
        {
            "id": log.id,
            "run_id": log.run_id,
            "outlet": log.outlet,
            "run_at": log.run_at,
            "articles_new": log.articles_new,
            "articles_skip": log.articles_skip,
            "status": log.status,
            "error_message": log.error_message,
        }
        for log in logs
    ]

@app.post("/fetch/rss")
def trigger_rss_fetch():
    """Run RSS collection synchronously and return the ingestion summary."""
    try:
        return run_rss_collection()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/fetch/body")
def trigger_body_fetch():
    try:
        return run_body_fetch()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_query_embedding(query: str) -> str:
    vector = [float(x) for x in get_embedding_model().encode(query)]
    return str(vector)

def tokenize_query(query: str) -> list[str]:
    raw_query = query.strip().lower()
    tokens = [t for t in raw_query.split() if len(t) > 2 and t not in STOP_WORDS]
    if not tokens:
        tokens = [t for t in raw_query.split() if len(t) > 2]
    return tokens or [raw_query]

@app.get("/search/stories")
@app.get("/stories")
def get_stories(
    q: Optional[str] = None,
    limit: int = Query(20, ge=1, le=50),
):
    """Return recent stories or search stories with keyword and semantic fallback."""
    with Session(engine) as session:
        # 1. NO QUERY PROVIDED: Default list of recent stories
        if not q or not q.strip():
            # STRICT JOIN: Ensure we only return stories with at least one TRUE, non-empty body
            stories = (
                session.query(Story)
                .join(StoryArticle, StoryArticle.story_id == Story.id)
                .join(RSSArticle, RSSArticle.id == StoryArticle.article_id)
                .filter(
                    RSSArticle.body_fetched == True,
                    RSSArticle.body.is_not(None),
                    func.trim(RSSArticle.body) != "",
                    func.lower(RSSArticle.body) != "null",
                    func.lower(RSSArticle.body) != "none"
                )
                .group_by(Story.id)
                .order_by(Story.updated_at.desc())
                .limit(limit)
                .all()
            )
            
            return [
                {
                    "id": story.id,
                    "title": story.title,
                    "summary": story.summary,
                    "article_count": story.article_count,
                    "bias_distribution": story.bias_distribution,
                    "disagreement_score": story.disagreement_score,
                    "confidence_score": story.confidence_score,
                    "updated_at": story.updated_at,
                    "distance": None,
                    "matched_articles_count": story.article_count,
                }
                for story in stories
            ]

        tokens = tokenize_query(q)
        
        # STRICTER MATCHING: Require all words for 1-2 word queries, allow 1 missing for 3+
        min_required_tokens = len(tokens) if len(tokens) <= 2 else len(tokens) - 1
        query_vector_str = get_query_embedding(q)

        token_match_cases = " + ".join(
            [
                f"(CASE WHEN LOWER(a.title) LIKE :t{i} OR LOWER(a.summary) LIKE :t{i} "
                f"OR LOWER(s.title) LIKE :t{i} THEN 1 ELSE 0 END)"
                for i in range(len(tokens))
            ]
        )
        token_where = " OR ".join(
            [
                f"LOWER(a.title) LIKE :t{i} OR LOWER(a.summary) LIKE :t{i} OR LOWER(s.title) LIKE :t{i}"
                for i in range(len(tokens))
            ]
        )

        token_params = {f"t{i}": f"%{token}%" for i, token in enumerate(tokens)}
        token_params["v"] = query_vector_str
        token_params["limit"] = limit
        token_params["min_tokens"] = min_required_tokens

        # 2. KEYWORD SEARCH: Bulletproof checks for spaces, "null", and "None" strings
        keyword_stmt = text(f"""
            WITH article_scores AS (
                SELECT sa.story_id, sa.article_id,
                       ({token_match_cases}) as tokens_hit
                FROM story_articles sa
                JOIN rss_articles a ON sa.article_id = a.id
                JOIN stories s ON sa.story_id = s.id
                WHERE ({token_where})
                  AND a.body_fetched = TRUE
                  AND a.body IS NOT NULL
                  AND TRIM(a.body) != ''
                  AND LOWER(a.body) NOT IN ('null', 'none')
            )
            SELECT s.id, s.title, s.summary, s.article_count,
                   s.bias_distribution, s.disagreement_score,
                   s.confidence_score, s.updated_at,
                   COUNT(DISTINCT score.article_id) as matched_articles_count,
                   MAX(score.tokens_hit) as best_token_match,
                   (s.centroid_vector <=> CAST(:v AS vector)) as distance
            FROM stories s
            JOIN article_scores score ON s.id = score.story_id
            WHERE s.centroid_vector IS NOT NULL
            GROUP BY s.id
            HAVING MAX(score.tokens_hit) >= :min_tokens
               AND (s.centroid_vector <=> CAST(:v AS vector)) < 0.7
            ORDER BY best_token_match DESC, distance ASC, s.updated_at DESC
            LIMIT :limit
        """)

        try:
            keyword_results = session.execute(keyword_stmt, token_params).fetchall()
            if keyword_results:
                return [dict(row._mapping) for row in keyword_results]
        except Exception as e:
            print(f"Keyword Stage Error: {e}")

        # 3. SEMANTIC FALLBACK: Added bulletproof EXISTS clause 
        try:
            semantic_stmt = text("""
                SELECT s.id, s.title, s.summary, s.article_count, s.bias_distribution,
                       s.disagreement_score, s.confidence_score, s.updated_at,
                       0 as matched_articles_count,
                       (s.centroid_vector <=> CAST(:v AS vector)) as distance
                FROM stories s
                WHERE s.centroid_vector IS NOT NULL
                  AND (s.centroid_vector <=> CAST(:v AS vector)) < 0.75
                  AND EXISTS (
                      SELECT 1 FROM story_articles sa
                      JOIN rss_articles a ON sa.article_id = a.id
                      WHERE sa.story_id = s.id
                        AND a.body_fetched = TRUE
                        AND a.body IS NOT NULL
                        AND TRIM(a.body) != ''
                        AND LOWER(a.body) NOT IN ('null', 'none')
                  )
                ORDER BY distance ASC
                LIMIT :limit
            """)
            results = session.execute(semantic_stmt, {"v": query_vector_str, "limit": limit}).fetchall()
            return [dict(row._mapping) for row in results]
        except Exception as e:
            print(f"Semantic Fallback Error: {e}")
            return []

@app.get("/stories/{story_id}")
def get_story(story_id: str):
    """Return story details and associated articles."""
    with Session(engine) as session:
        story = session.get(Story, story_id)
        if not story:
            raise HTTPException(status_code=404, detail="Story not found")

        ready_articles = (
            session.query(RSSArticle)
            .join(StoryArticle, StoryArticle.article_id == RSSArticle.id)
            .filter(
                StoryArticle.story_id == story_id,
                RSSArticle.body_fetched == True,
                RSSArticle.body.is_not(None),
                RSSArticle.body != "",
                func.trim(RSSArticle.body) != "",
                func.lower(RSSArticle.body).not_in(["null", "none"]),
            )
            .all()
        )

        articles = [
            {
                "id": article.id,
                "title": article.title,
                "outlet": article.outlet,
                "bias": article.bias,
                "url": article.url,
                "published": article.published,
                "ai_summary": article.ai_summary,
            }
            for article in ready_articles
        ]

        # Recompute bias distribution live from only ready articles
        from collections import Counter
        live_bias = Counter(a.bias for a in ready_articles if a.bias)

        return {
            "id": story.id,
            "title": story.title,
            "summary": story.summary,
            "category": story.category,
            "article_count": len(articles),
            "bias_distribution": dict(live_bias),  # Live count, not stale clustered value
            "disagreement_score": story.disagreement_score,
            "confidence_score": story.confidence_score,
            "articles": articles,
        }


@app.get("/stories/{story_id}/analysis")
def get_story_analysis(story_id: str):
    """Run the analysis agent for a story, using a one-day cache."""
    from agent import run_agent

    with Session(engine) as session:
        story = session.get(Story, story_id)
        if not story:
            raise HTTPException(status_code=404, detail="Story not found")

        day_ago = datetime.utcnow() - timedelta(days=1)
        cached = (
            session.query(BiasAnalysisReport)
            .filter(BiasAnalysisReport.topic == story.title)
            .filter(BiasAnalysisReport.created_at >= day_ago)
            .order_by(BiasAnalysisReport.created_at.desc())
            .first()
        )
        if cached and cached.raw_result:
            return cached.raw_result

        ready_articles = (
            session.query(RSSArticle.url, RSSArticle.body, RSSArticle.outlet, RSSArticle.bias)
            .join(StoryArticle, StoryArticle.article_id == RSSArticle.id)
            .filter(
                StoryArticle.story_id == story_id,
                RSSArticle.body_fetched == True,
                RSSArticle.body.is_not(None),
                RSSArticle.body != "",
                func.trim(RSSArticle.body) != "",
                func.lower(RSSArticle.body).not_in(["null", "none"]),
            )
            .all()
        )

        if not ready_articles:
            raise HTTPException(
                status_code=400,
                detail="Story has no fully fetched articles to analyze yet."
            )

        urls = [a.url for a in ready_articles]
        prefetched_bodies = {a.url: a.body for a in ready_articles}

        result = run_agent(topic=story.title, urls=urls, prefetched_bodies=prefetched_bodies)

        report = BiasAnalysisReport(
            topic=story.title,
            balanced_brief=result.get("balanced_brief", ""),
            comparison=result.get("comparison", ""),
            visualization_path=result.get("visualization_path", ""),
            raw_result=result,
            agreement_score=result.get("metrics", {}).get("agreement", 0),
            is_polarized=result.get("metrics", {}).get("is_polarized", False),
        )
        session.add(report)
        session.commit()

        return result

class AnalyzeRequest(BaseModel):
    topic: str
    urls: list[str] = []

@app.post("/analyze")
def trigger_analysis(req: AnalyzeRequest):
    """Run deep analysis for URLs, or discover likely URLs from the topic."""
    print(f"\n>>> [API] POST /analyze | topic='{req.topic}' | urls_count={len(req.urls)} <<<")
    try:
        from agent import run_agent

        urls_to_analyze = req.urls

        if not urls_to_analyze and req.topic:
            with Session(engine) as session:
                search_q = f"%{req.topic.lower()}%"
                
                # CORRECTED: Database query level block on empty articles
                articles = (
                    session.query(RSSArticle)
                    .filter(
                        RSSArticle.body_fetched == True,
                        RSSArticle.body.is_not(None),
                        RSSArticle.body != "",
                        or_(
                            RSSArticle.title.ilike(search_q),
                            RSSArticle.summary.ilike(search_q),
                        )
                    )
                    .order_by(RSSArticle.fetched_at.desc())
                    .limit(10)
                    .all()
                )

                urls_to_analyze = [article.url for article in articles]

        # If no articles found in DB, we still run the agent so it can use GDELT fallback
        return run_agent(topic=req.topic, urls=urls_to_analyze)
    except Exception as e:
        print(f"[API] Analysis trigger failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class ImproveStoriesRequest(BaseModel):
    query: str
    top_k_stories: int = 5
    articles_per_story: int = 5

@app.post("/improve-stories")
def improve_stories(req: ImproveStoriesRequest):
    """Return relevant stories with summary, relevance, weaknesses, and improvement suggestions."""
    if not req.query or not req.query.strip():
        raise HTTPException(status_code=400, detail="Query is required")

    from story_improvement_service import run_improvement_pipeline

    try:
        with Session(engine) as session:
            results = run_improvement_pipeline(
                session=session,
                query=req.query,
                top_k_stories=req.top_k_stories,
                articles_per_story=req.articles_per_story,
            )
            return {
                "query": req.query,
                "stories_found": len(results),
                "results": results,
            }
    except Exception as e:
        print(f"[API] Improve-stories failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))