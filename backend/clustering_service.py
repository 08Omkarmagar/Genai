import logging
import uuid
from datetime import datetime, timezone
from sqlalchemy import text
from sqlalchemy.orm import Session
import numpy as np

from models import RSSArticle, Story, StoryArticle
from Summary import generate_summary

logger = logging.getLogger(__name__)

# Cosine distance threshold — articles closer than this join an existing story.
SIMILARITY_THRESHOLD = 0.50
# Two-pass stricter gate; must also beat this to confirm assignment.
VALIDATION_THRESHOLD = 0.45
# Story sizes at which we (re-)generate a summary.
SUMMARY_TRIGGER_COUNTS = {1, 3, 5, 10}

_model = None


def get_embedding_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    return _model


def _vector_to_db_str(vector) -> str:
    """Convert numpy array to pgvector-compatible string '[0.1,0.2,...]'."""
    return "[" + ",".join(f"{x:.8f}" for x in vector.tolist()) + "]"


class ClusteringService:
    def __init__(self, db: Session):
        self.db = db
        self.model = get_embedding_model()

    # ------------------------------------------------------------------
    # Embedding helpers
    # ------------------------------------------------------------------

    def get_embedding(self, text: str) -> np.ndarray:
        return self.model.encode(text).astype(np.float32)

    def get_or_create_article_embedding(self, article: RSSArticle) -> np.ndarray:
        if article.embedding is not None:
            return np.array(article.embedding, dtype=np.float32)

        content = f"{article.title} {article.body[:1000] if article.body else ''}"
        vector = self.get_embedding(content)
        article.embedding = vector.tolist()
        return vector

    # ------------------------------------------------------------------
    # Story assignment
    # ------------------------------------------------------------------

    def assign_article(self, article, vector):
        """
        Find the nearest story via pgvector cosine distance.

        Two-pass check:
          pass 1 — within SIMILARITY_THRESHOLD (broad gate)
          pass 2 — within VALIDATION_THRESHOLD  (strict confirmation)

        Returns (story_id, distance) or (None, None).
        """
        vec_str = _vector_to_db_str(vector)
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

    # ------------------------------------------------------------------
    # Story creation / updates
    # ------------------------------------------------------------------

    def create_new_story(self, article: RSSArticle, vector) -> str:
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
        """Incremental centroid update — O(1), no need to reload all articles."""
        current = np.array(story.centroid_vector, dtype=np.float32)
        n = story.article_count
        story.centroid_vector = ((current * n + new_vector) / (n + 1)).tolist()
        story.updated_at = datetime.now(timezone.utc)

    def update_bias_distribution(self, story: Story, article: RSSArticle):
        bias_key = (article.bias or "unknown").lower()
        dist = story.bias_distribution or {}
        dist[bias_key] = dist.get(bias_key, 0) + 1
        story.bias_distribution = dist

    def link_article(self, story_id: str, article_id: str, distance: float):
        link = StoryArticle(
            story_id=story_id,
            article_id=article_id,
            assignment_score=1.0 - (distance or 0.0),
        )
        self.db.add(link)

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------

    def process_unassigned_articles(self, limit: int = 50) -> int:
        """
        Assign up to *limit* unassigned articles to stories.

        Returns the number of articles actually processed so the caller
        can detect a stuck loop.
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

        # Enforce minimum body length *in Python* — avoids a costly DB
        # SPLIT/LENGTH call while still being safe.
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

        # After committing, update intelligence for every touched story.
        for sid in modified_story_ids:
            self.update_story_intelligence(sid)

        return len(articles)

    # ------------------------------------------------------------------
    # Story intelligence (summary generation)
    # ------------------------------------------------------------------

    def update_story_intelligence(self, story_id: str):
        """
        Generate (or refresh) a story-level summary when the article
        count hits one of the trigger thresholds.

        FIX vs original:
          - The original set a hard-coded placeholder string and never
            called generate_summary().
          - We now fetch a representative article body and call the
            real summarizer.  If the API fails we fall back gracefully
            instead of crashing.
        """
        story = self.db.get(Story, story_id)
        if not story:
            return

        if story.article_count not in SUMMARY_TRIGGER_COUNTS:
            return

        logger.info(
            f"Triggering summary for Story {story_id} "
            f"(article_count={story.article_count})"
        )

        # Fetch the body of the most recently linked article as input.
        representative = (
            self.db.query(RSSArticle)
            .join(StoryArticle, StoryArticle.article_id == RSSArticle.id)
            .filter(StoryArticle.story_id == story_id)
            .filter(RSSArticle.body.isnot(None))
            .order_by(StoryArticle.assignment_score.desc())
            .first()
        )

        if not representative or not representative.body:
            logger.warning(
                f"Story {story_id}: no article body available for summarization."
            )
            # Safe fallback — never leave summary as None.
            if not story.summary:
                story.summary = f"Coverage of: {story.title}"
                self.db.commit()
            return

        try:
            summary = generate_summary(representative.id, representative.body)
            if summary:
                story.summary = summary
                logger.info(f"Story {story_id} summary updated.")
            else:
                # generate_summary returned empty — use a sensible default.
                if not story.summary:
                    story.summary = f"Coverage of: {story.title}"
        except Exception as exc:
            logger.error(
                f"Summary generation failed for story {story_id}: {exc}"
            )
            if not story.summary:
                story.summary = f"Coverage of: {story.title}"

        self.db.commit()