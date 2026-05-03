"""
clustering_service.py — Semantic grouping of news articles into coherent Stories.

This module provides the core logic for identifying when multiple articles are reporting
on the same real-world event. It uses vector embeddings (all-MiniLM-L6-v2) and 
PostgreSQL's pgvector extension to perform high-performance similarity searches.

Key mechanisms:
- Semantic Embedding: Converts article text into a 384-dimensional dense vector.
- Vector Search: Uses cosine distance (<=>) to find the nearest existing Story.
- Incremental Centroids: Updates story clusters in O(1) time without re-reading all articles.
- Intelligence Triggers: Automatically generates AI summaries when a story reaches specific sizes.
"""

import logging
import uuid
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.orm import Session
import numpy as np
        
from models import RSSArticle, Story, StoryArticle
from Summary import generate_summary

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------

# broad gate for nearest-neighbor search
SIMILARITY_THRESHOLD = 0.50

# strict confirmation gate for assignment
VALIDATION_THRESHOLD = 0.45

# Story sizes at which we trigger the AI summarizer to refresh the story overview
SUMMARY_TRIGGER_COUNTS = {1, 3, 5, 10}

# Global singleton for the embedding model to ensure it's only loaded once
_model = None

# ---------------------------------------------------------------------------
# Model & Vector Utilities
# ---------------------------------------------------------------------------

def get_embedding_model():
    """
    Lazy-loads the SentenceTransformer model. 
    This avoids heavy memory consumption during initial script import.
    """
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model

def _vector_to_db_str(vector) -> str:
    """
    Converts a numpy array into a string format compatible with pgvector: '[v1,v2,...]'.
    """
    return "[" + ",".join(f"{x:.8f}" for x in vector.tolist()) + "]"

# ---------------------------------------------------------------------------
# Clustering Service Implementation
# ---------------------------------------------------------------------------

class ClusteringService:
    """
    Orchestrates the lifecycle of news stories from individual articles.
    """
    def __init__(self, db: Session):
        self.db = db
        self.model = get_embedding_model()

    def get_embedding(self, text: str) -> np.ndarray:
        """Generates a 384-dim vector for the given text."""
        return self.model.encode(text).astype(np.float32)

    def get_or_create_article_embedding(self, article: RSSArticle) -> np.ndarray:
        """
        Retrieves an article's vector from the DB or generates it if missing.
        Combines title and first 1000 chars of body for the embedding context.
        """
        if article.embedding is not None:
            return np.array(article.embedding, dtype=np.float32)

        content = f"{article.title} {article.body[:1000] if article.body else ''}"
        vector = self.get_embedding(content)
        article.embedding = vector.tolist()
        return vector

    # --- Story Assignment ---

    def assign_article(self, article, vector):
        """
        Finds the nearest story cluster using pgvector cosine distance.

        Uses a two-pass validation strategy:
        1. Broad check against SIMILARITY_THRESHOLD.
        2. Strict verification against VALIDATION_THRESHOLD.
        
        Returns: (story_id, distance) or (None, None) if no match found.
        """
        vec_str = _vector_to_db_str(vector)
        
        # Uses the cosine distance operator (<=>) for high-performance ranking in PostgreSQL
        stmt = text("""
            SELECT id, (centroid_vector <=> CAST(:vec AS vector)) AS dist
            FROM stories
            ORDER BY dist ASC
            LIMIT 1
        """)
        result = self.db.execute(stmt, {"vec": vec_str}).first()

        if result and result.dist < SIMILARITY_THRESHOLD:
            if result.dist < VALIDATION_THRESHOLD:
                return result.id, result.dist

        return None, None

    # --- Story Evolution ---

    def create_new_story(self, article: RSSArticle, vector) -> str:
        """Initializes a new story cluster with the first article as its centroid."""
        new_story = Story(
            id=str(uuid.uuid4()),
            title=article.title,
            article_count=1,
            centroid_vector=vector.tolist(),
            bias_distribution={},
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self.db.add(new_story)
        self.db.flush()
        return new_story.id

    def update_centroid(self, story: Story, new_vector: np.ndarray):
        """
        Performs an incremental update of the cluster centroid.
        This calculation is O(1), allowing the system to scale regardless of cluster size.
        """
        current = np.array(story.centroid_vector, dtype=np.float32)
        n = story.article_count
        # Simple weighted average to shift the centroid towards the new article
        story.centroid_vector = ((current * n + new_vector) / (n + 1)).tolist()
        story.updated_at = datetime.now(timezone.utc)

    def update_bias_distribution(self, story: Story, article: RSSArticle):
        """Updates the count of Left/Center/Right articles within the story cluster."""
        bias_key = (article.bias or "unknown").lower()
        dist = story.bias_distribution or {}
        dist[bias_key] = dist.get(bias_key, 0) + 1
        story.bias_distribution = dist

    def link_article(self, story_id: str, article_id: str, distance: float):
        """Creates the formal many-to-one link between an article and its story."""
        link = StoryArticle(
            story_id=story_id,
            article_id=article_id,
            assignment_score=1.0 - (distance or 0.0), # Invert distance to get a 0.0-1.0 similarity score
        )
        self.db.add(link)

    # --- Batch Operations ---

    def process_unassigned_articles(self, limit: int = 50) -> int:
        """
        Polls for articles with fetched bodies that aren't yet assigned to a story.
        This is typically run as a background cron or event-driven task.
        """
        articles = (
            self.db.query(RSSArticle)
            .filter(
                RSSArticle.body_fetched == True,
                RSSArticle.body.isnot(None),
                RSSArticle.body != "",
                ~RSSArticle.id.in_(
                    self.db.query(StoryArticle.article_id)
                ),
            )
            .limit(limit)
            .all()
        )

        # Enforce minimum content length to ensure clustering is based on meaningful data
        articles = [a for a in articles if a.body and len(a.body.split()) >= 50]

        if not articles:
            return 0

        modified_story_ids: set[str] = set()

        for article in articles:
            vector = self.get_or_create_article_embedding(article)
            story_id, distance = self.assign_article(article, vector)

            if story_id:
                story = self.db.get(Story, story_id)
                self.update_centroid(story, vector)
                self.update_bias_distribution(story, article)
                story.article_count += 1
            else:
                story_id = self.create_new_story(article, vector)
                distance = 0.0
                story = self.db.get(Story, story_id)
                self.update_bias_distribution(story, article)

            self.link_article(story_id, article.id, distance)
            modified_story_ids.add(story_id)

        self.db.commit()

        # Update AI-summaries for any clusters that hit growth milestones
        for sid in modified_story_ids:
            self.update_story_intelligence(sid)

        return len(articles)

    # --- Intelligence & Summarization ---

    def update_story_intelligence(self, story_id: str):
        """
        Determines if a story summary needs refreshing.
        Summaries are generated at specific milestones (1, 3, 5, 10 articles)
        to manage LLM costs while keeping content fresh during the first hours of a story.
        """
        story = self.db.get(Story, story_id)
        if not story or story.article_count not in SUMMARY_TRIGGER_COUNTS:
            return

        logger.info(f"Triggering summary for Story {story_id} (count={story.article_count})")

        # Pick the article that is most central to the cluster to use as the summary seed
        representative = (
            self.db.query(RSSArticle)
            .join(StoryArticle, StoryArticle.article_id == RSSArticle.id)
            .filter(StoryArticle.story_id == story_id)
            .filter(RSSArticle.body.isnot(None))
            .order_by(StoryArticle.assignment_score.desc())
            .first()
        )

        if not representative or not representative.body:
            # Fallback title-based summary if no body is readable
            if not story.summary:
                story.summary = f"Coverage of: {story.title}"
                self.db.commit()
            return

        try:
            summary = generate_summary(representative.id, representative.body)
            if summary:
                story.summary = summary
                logger.info(f"Story {story_id} summary updated.")
            elif not story.summary:
                story.summary = f"Coverage of: {story.title}"
        except Exception as exc:
            logger.error(f"Summary generation failed for story {story_id}: {exc}")
            if not story.summary:
                story.summary = f"Coverage of: {story.title}"

        self.db.commit()