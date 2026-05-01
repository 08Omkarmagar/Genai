import logging
from sqlalchemy import text
from database import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_migration():
    """
    Adds the missing story_id column to the bias_analysis_reports table.
    """
    with engine.connect() as conn:
        logger.info("Checking for 'story_id' column in 'bias_analysis_reports'...")
        
        # Check if column exists to avoid errors on multiple runs
        check_col_query = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='bias_analysis_reports' AND column_name='story_id';
        """)
        
        result = conn.execute(check_col_query).fetchone()
        
        if not result:
            logger.info("Adding 'story_id' column...")
            try:
                # Add the column
                conn.execute(text("ALTER TABLE bias_analysis_reports ADD COLUMN story_id VARCHAR;"))
                # Create an index for faster lookups
                conn.execute(text("CREATE INDEX ix_bias_analysis_reports_story_id ON bias_analysis_reports (story_id);"))
                conn.commit()
                logger.info("Column 'story_id' and index added successfully.")
            except Exception as e:
                logger.error(f"Migration failed: {e}")
                conn.rollback()
        else:
            logger.info("Column 'story_id' already exists.")

if __name__ == "__main__":
    run_migration()
