import logging
from sqlalchemy import text
from database import engine
from models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_migration():
    """
    Enables pgvector extension and creates new tables.
    """
    with engine.connect() as conn:
        logger.info("Enabling pgvector extension...")
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            conn.commit()
            logger.info("Extension 'vector' is ready.")
        except Exception as e:
            logger.error(f"Could not enable pgvector extension: {e}")
            logger.info("Note: You may need superuser privileges (postgres) to enable extensions.")
            return

        logger.info("Creating stories and story_articles tables...")
        try:
            Base.metadata.create_all(engine)
            logger.info("Migration successful.")
        except Exception as e:
            logger.error(f"Migration failed during table creation: {e}")

if __name__ == "__main__":
    run_migration()
