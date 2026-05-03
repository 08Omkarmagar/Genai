"""
database.py — Database connection and session management.

This module initializes the SQLAlchemy engine and provides a thread-safe session
factory. It relies on environment variables for sensitive connection details.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

load_dotenv()

# Connection parameters sourced from .env
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "news_aggregator")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ---------------------------------------------------------------------------
# Engine & Session Configuration
# ---------------------------------------------------------------------------

# The engine serves as the source of connectivity to the database
engine = create_engine(DATABASE_URL)

# SessionLocal is used for manual session management in background jobs
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency for FastAPI routes to provide a scoped database session.
    Automatically closes the session after the request is finished.
    """
    with Session(engine) as session:
        yield session
