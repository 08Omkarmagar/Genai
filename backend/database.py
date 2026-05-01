
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST",     "localhost")
DB_PORT = os.getenv("DB_PORT",     "5432")
DB_NAME = os.getenv("DB_NAME",     "news_aggregator")
DB_USER = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    with Session(engine) as session:
        yield session
