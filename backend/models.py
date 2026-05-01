import uuid
from datetime import datetime

from sqlalchemy import Column, String, Text, DateTime, Integer, Boolean, JSON, ForeignKey, Float, Index
from sqlalchemy.orm import DeclarativeBase, relationship
from pgvector.sqlalchemy import Vector

from database import engine


class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    """
    One row per article pulled from an RSS feed.

    fetch_fail_count — incremented each time the body fetch fails.
                       When it reaches 3 the article is flagged in
                       articles_to_remove and skipped on future runs.
    """
    __tablename__ = "rss_articles"

    id = Column(String,  primary_key=True, default=lambda: str(uuid.uuid4()))
    outlet = Column(String,  nullable=False)
    bias = Column(String,  nullable=False)
    country = Column(String,  default="IN")
    title = Column(Text,    nullable=False)
    url = Column(String,  unique=True)
    summary = Column(Text)
    published = Column(String)
    fetched_at = Column(DateTime, default=datetime.utcnow)
    body = Column(Text)
    body_fetched = Column(Boolean,  default=False)
    content_type = Column(Text)
    fetch_fail_count = Column(Integer,  default=0)

    ai_summary = Column(Text)
    bias_score = Column(Integer)
    bias_label = Column(String)
    bias_reasoning = Column(Text)
    confidence_score = Column(Float)
    body_quality = Column(Float, default=0.0)

    embedding = Column(Vector(384))


class ArticleToRemove(Base):
    """
    Holds articles flagged for removal.
    Populated automatically by body_fetcher.py (invalid URL or 3 failures).
    Acted upon only by remove_flagged_articles.py — never auto-deleted.

    reason values:
      "invalid_url"    — URL was null, empty, or malformed
      "failed_3_times" — body fetch failed on 3 separate runs
    """
    __tablename__ = "articles_to_remove"

    id = Column(String,    primary_key=True)
    outlet = Column(String)
    url = Column(String)
    reason = Column(Text)
    flagged_at = Column(DateTime,  default=datetime.utcnow)


class BiasAnalysisReport(Base):
    __tablename__ = "bias_analysis_reports"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    # for cache keying per story
    story_id = Column(String, nullable=True, index=True)
    topic = Column(String, nullable=False)
    balanced_brief = Column(Text)
    comparison = Column(Text)
    visualization_path = Column(String)
    raw_result = Column(JSON)
    agreement_score = Column(Float)
    is_polarized = Column(Boolean)
    ambiguity_notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class FetchLog(Base):
    """
    One row per run of body_fetcher.py (or rss_fetcher.py).
    run_id groups all outlets that ran in the same batch.

    status values: "success" | "partial" | "error"
    """
    __tablename__ = "fetch_logs"

    id = Column(String,  primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(String)
    outlet = Column(String)
    run_at = Column(DateTime, default=datetime.utcnow)
    articles_new = Column(Integer,  default=0)
    articles_skip = Column(Integer,  default=0)
    status = Column(String)
    error_message = Column(Text)


class ArticleRelationship(Base):
    """
    Models the 'Edges' between articles: Supports, Contradicts, etc.
    """
    __tablename__ = "article_relationships"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id = Column(String, ForeignKey(
        "rss_articles.id", ondelete="CASCADE"))
    target_id = Column(String, ForeignKey(
        "rss_articles.id", ondelete="CASCADE"))

    relationship_type = Column(String)
    strength = Column(Float)
    evidence = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)


class Story(Base):
    """
    Groups multiple articles into a single real-world event.
    """
    __tablename__ = "stories"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(Text, nullable=False)
    summary = Column(Text)
    category = Column(String)
    article_count = Column(Integer, default=0)
    bias_distribution = Column(JSON)
    disagreement_score = Column(Float)
    confidence_score = Column(Float)

    centroid_vector = Column(Vector(384))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow,
                        onupdate=datetime.utcnow)

    articles = relationship(
        "RSSArticle", secondary="story_articles", back_populates="stories")


class StoryArticle(Base):
    """
    Many-to-many relationship mapping articles to stories.
    """
    __tablename__ = "story_articles"

    story_id = Column(String, ForeignKey(
        "stories.id", ondelete="CASCADE"), primary_key=True)
    article_id = Column(String, ForeignKey(
        "rss_articles.id", ondelete="CASCADE"), primary_key=True)
    assignment_score = Column(Float)


Index("idx_story_articles_story_id", StoryArticle.story_id)
Index("idx_story_articles_article_id", StoryArticle.article_id)
Index("idx_article_relationships_source", ArticleRelationship.source_id)
Index("idx_article_relationships_target", ArticleRelationship.target_id)

RSSArticle.stories = relationship(
    "Story", secondary="story_articles", back_populates="articles")


def init_db():
    """
    Creates all tables if they don't already exist.
    Safe to call multiple times — will NOT drop or alter existing tables.

    For the new fetch_fail_count column on an existing database,
    run migration.sql manually once.
    """
    Base.metadata.create_all(engine)
