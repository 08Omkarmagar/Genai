"""
models.py
=========
Single source of truth for all SQLAlchemy models.
Imported by save_indian_feeds.py, fetch_full_articles.py, and app.py.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Text, DateTime, Integer, Boolean
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class RSSArticle(Base):
    """
    One row per article fetched from an RSS feed.
    body / body_fetched / content_type are filled by the Body Fetcher Agent.
    """
    __tablename__ = "rss_articles"

    id           = Column(String,   primary_key=True, default=lambda: str(uuid.uuid4()))
    outlet       = Column(String,   nullable=False)
    bias         = Column(String,   nullable=False)
    country      = Column(String,   default="IN")
    title        = Column(Text,     nullable=False)
    url          = Column(String,   unique=True)
    summary      = Column(Text)
    published    = Column(String)
    body         = Column(Text,     nullable=True)
    body_fetched = Column(Boolean,  default=False)   # FIX: was String "false" in feeds, Boolean in fetcher — now unified
    content_type = Column(String,   nullable=True)
    fetched_at   = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class FetchLog(Base):
    """
    Logs every fetch run per outlet.
    Visible in the Flask dashboard at /logs.
    """
    __tablename__ = "fetch_logs"

    id            = Column(String,   primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id        = Column(String)
    outlet        = Column(String)
    run_at        = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    articles_new  = Column(Integer,  default=0)
    articles_skip = Column(Integer,  default=0)
    status        = Column(String)
    error_message = Column(Text)