from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from dotenv import load_dotenv
from contextlib import contextmanager
import time
import random
import threading
import sys

load_dotenv()

# Get DB_URL from environment
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise ValueError("DB_URL environment variable is not set")

# POOL_SIZE is independent of thread count to respect DB limits.
# Supabase free/starter tiers typically allow ~15-20 max connections
# Keep pool small to avoid "Max client connections reached"
POOL_SIZE = 5  # Reduced to avoid Supabase limits
MAX_OVERFLOW = 10  # Allow up to 15 total connections

# Create engine with connection pooling optimized for concurrent environments
engine = create_engine(
    DB_URL,
    pool_size=10,  # Reduced to leave room for backend connections
    max_overflow=10,  # Tightened overflow to prevent overloading the DB
    pool_timeout=60,  # Wait longer for a connection if busy
    pool_recycle=1800,  # Reset connections every 30 mins to avoid stale states
    pool_pre_ping=True,  # Check connectivity before usage
    connect_args={
        "connect_timeout": 20,  # Increase OS level connection timeout
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db_engine():
    """
    Returns the created engine. Use this to bind models to the engine if needed
    or to perform direct engine operations.
    """
    return engine


# Global connection failure tracking for adaptive backoff across all threads
_connection_stats = {
    "consecutive_failures": 0,
    "last_success_time": time.time(),
    "lock": threading.Lock(),
}


@contextmanager
def get_db():
    """
    Context manager for database sessions with built-in retry logic
    for transient connection failures (e.g. Supabase pooler issues).
    Retries indefinitely for connection availability issues to handle load spikes.
    """
    attempt = 0

    # Enhanced error detection for database overload conditions
    transient_connection_errors = [
        "connection refused",
        "operationalerror",
        "connection failed",
        "could not receive data",
        "pooler",
        "timeout",
        "connection timed out",
        "too many connections",
        "max client connections reached",
        "connection limit exceeded",
        "server is starting up",
        "database is locked",
    ]

    # Overload-specific errors that indicate the database is under heavy load
    overload_errors = [
        "too many connections",
        "max client connections reached",
        "connection limit exceeded",
        "server is starting up",
        "database system is starting up",
        "database is locked",
        "deadlock detected",
        "resource busy",
        "temporary failure",
    ]

    while True:
        try:
            db = SessionLocal()
            # Test the connection immediately with a ping
            db.execute(text("SELECT 1"))
            try:
                yield db
            finally:
                db.close()

            # Success: reset global failure counter
            with _connection_stats["lock"]:
                _connection_stats["consecutive_failures"] = 0
                _connection_stats["last_success_time"] = time.time()
            break  # Exit loop on success

        except Exception as e:
            attempt += 1
            msg = str(e).lower()

            # Update global failure counter
            with _connection_stats["lock"]:
                _connection_stats["consecutive_failures"] += 1
                consecutive_failures = _connection_stats["consecutive_failures"]

            # Check if it's a transient connection or overload error
            is_transient = any(err in msg for err in transient_connection_errors)
            is_overload = any(err in msg for err in overload_errors)

            if is_transient or is_overload:
                sleep_time = 0.3
                
                # Only log every few attempts to avoid spamming
                if attempt % 5 == 1:
                    error_type = "overloaded" if is_overload else "busy/unavailable"
                    print(
                        f"  [DB Retry] Database {error_type}. Retrying in {sleep_time:.2f}s... (Attempt {attempt}, Consecutive failures: {consecutive_failures})"
                    )

                time.sleep(sleep_time)
                continue

            # If it's not a transient error (e.g. programming error), raise immediately
            raise e
