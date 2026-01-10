from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from dotenv import load_dotenv

load_dotenv()

# Get DB_URL from environment
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("DB_URL environment variable is not set")

# POOL_SIZE is independent of thread count to respect DB limits.
# Supabase free/starter tiers often resolve to ~20 connections max.
# We keep this low (5) and allow some overflow (10) to be safe.
POOL_SIZE = 5

# Create engine with connection pooling
# pool_size: The number of connections to keep open inside the connection pool.
# max_overflow: The number of connections to allow in connection pool "overflow",
#               that is, connections that can be opened above and beyond the
#               pool_size setting.
engine = create_engine(
    DB_URL,
    pool_size=POOL_SIZE,
    max_overflow=0,
    pool_timeout=30,
    pool_pre_ping=True # Verify connection before usage
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db_engine():
    """
    Returns the created engine. Use this to bind models to the engine if needed
    or to perform direct engine operations.
    """
    return engine

from contextlib import contextmanager

@contextmanager
def get_db():
    """
    Context manager for database sessions.
    Ensures session is closed after use.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
