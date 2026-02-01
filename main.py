from edgar import latest_filings, get_filings, get_current_filings
from datetime import datetime, timedelta
from edgar import Company, set_identity
import pandas as pd
import json
import time
import sys
import math
import re
import signal
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.summarizer import summarize_text
from dotenv import load_dotenv
import os
import random
import requests
import traceback
from sqlalchemy.orm import Session
from sqlalchemy import select, delete, cast, String, Float, func, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from io import BytesIO
from PIL import Image
import boto3
from botocore.exceptions import ClientError

# Global shutdown flags
_shutdown_requested = False
_force_exit = False


def signal_handler(signum, frame):
    global _shutdown_requested, _force_exit
    if _shutdown_requested:
        print(f"\n☢️  FORCED EXIT requested. Terminating immediately...")
        sys.exit(1)
        
    print(f"\n🛑 Received shutdown signal (Ctrl+C). Finishing current tasks and stopping...")
    _shutdown_requested = True


# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def fetch_with_retry(func, *args, **kwargs):
    max_retries = 10
    attempts = 0

    # Enhanced database error patterns
    db_transient_errors = [
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

    db_overload_errors = [
        "too many connections",
        "max client connections reached",
        "connection limit exceeded",
        "server is starting up",
        "database system is starting up",
        "database is locked",
        "deadlock detected",
        "resource busy",
    ]

    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            attempts += 1
            msg = str(e).lower()

            # Rate limits (SEC)
            is_rate_limit = any(
                x in msg for x in ["too many requests", "429", "rate limit"]
            )

            # Database connection issues (Supabase/Postgres)
            is_db_error = any(x in msg for x in db_transient_errors)
            is_db_overload = any(x in msg for x in db_overload_errors)

            if (is_rate_limit or is_db_error) and attempts < max_retries:
                if is_db_overload:
                    # Longer backoff for database overload
                    sleep_time = min(20.0, 3.0 * (1.2**attempts))
                    log_msg = f"[DB Overload] Database overloaded"
                elif is_db_error:
                    # Standard backoff for other DB errors
                    sleep_time = min(10.0, 2.0 * (1.2**attempts))
                    log_msg = f"[DB Connection] Database connection issue"
                else:
                    # Rate limit backoff
                    sleep_time = random.uniform(0.5, 1.5) * attempts
                    log_msg = f"[Rate Limit] API rate limited"

                # Add jitter
                sleep_time += random.uniform(0.0, 1.0)

                # Log every 3rd attempt to avoid spam
                if attempts % 3 == 1:
                    print(
                        f"  {log_msg}. Retrying in {sleep_time:.2f}s... (Attempt {attempts})"
                    )

                time.sleep(sleep_time)
            else:
                raise e


def db_operation_with_retry(db, db_func, *args, **kwargs):
    """
    Retry wrapper specifically for database operations that might fail due to overload.
    Handles both connection issues and transaction conflicts.
    """
    max_retries = 5
    attempts = 0

    # Database overload and transient errors
    db_transient_errors = [
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
        "deadlock detected",
        "resource busy",
        "temporary failure",
        "could not serialize access",
        "serialization failure",
    ]

    while True:
        try:
            return db_func(*args, **kwargs)
        except Exception as e:
            attempts += 1
            msg = str(e).lower()

            # Check if this is a transient database error that should be retried
            is_transient = any(err in msg for err in db_transient_errors)

            if is_transient and attempts < max_retries:
                # Check if it's specifically an overload error
                is_overload = any(
                    err in msg
                    for err in [
                        "too many connections",
                        "max client connections",
                        "connection limit exceeded",
                        "server is starting up",
                        "database system is starting up",
                        "deadlock detected",
                    ]
                )

                sleep_time = 0.3

                print(
                    f"[DB Retry] Operation failed, retrying in {sleep_time:.2f}s... (Attempt {attempts})"
                )
                try:
                    # Clear session state so retry can start fresh (handle PendingRollbackError)
                    db.rollback()
                except:
                    pass
                time.sleep(sleep_time)
                continue

            # If not retryable or max retries reached, raise the error
            raise e


def commit_with_retry(db):
    """
    Retry wrapper specifically for database commit operations.
    Includes a small delay after success to pace DB throughput.
    """
    max_retries = 3
    attempts = 0

    # Commit-specific transient errors
    commit_transient_errors = [
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
        "deadlock detected",
        "resource busy",
        "temporary failure",
        "could not serialize access",
        "serialization failure",
        "commit failed",
        "transaction aborted",
        "duplicate key",
    ]

    while True:
        try:
            db.commit()
            return
        except Exception as e:
            attempts += 1
            msg = str(e).lower()

            # Check if this is a transient commit error that should be retried
            is_transient = any(err in msg for err in commit_transient_errors)

            if is_transient and attempts < max_retries:
                # Check if it's specifically an overload error
                is_overload = any(
                    err in msg
                    for err in [
                        "too many connections",
                        "max client connections",
                        "connection limit exceeded",
                        "server is starting up",
                        "database system is starting up",
                        "deadlock detected",
                    ]
                )

                print(
                    f"[Commit Retry] Commit failed, retrying in {sleep_time:.2f}s... (Attempt {attempts})"
                )
                try:
                    # CRITICAL: If commit fails, the session is poisoned. 
                    # We MUST rollback before retrying even if we think we can salvage the transaction.
                    # In many cases (FlushError), retrying the same commit is impossible, but 
                    # for OperationalError (connectivity), we must reset the state.
                    db.rollback() 
                except:
                    pass
                time.sleep(sleep_time)
                continue

            # If not retryable or max retries reached, rollback and raise the error
            try:
                db.rollback()
            except:
                pass  # Rollback might also fail, ignore it
            raise e


# Database imports
from db.db import init_db_engine, get_db
from db.models import Base, Ticker, CompanyEvent, InsiderTrade

load_dotenv()


def load_env_bool(key):
    return os.environ.get(key, "false").lower() == "true"


GET_8K = load_env_bool("GET_8K")
GET_10K = load_env_bool("GET_10K")
GET_10Q = load_env_bool("GET_10Q")
GET_4 = load_env_bool("GET_4")
GET_COMPANY_INFO = load_env_bool("GET_COMPANY_INFO")

GET_COMPANY_CASHFLOW = load_env_bool("GET_COMPANY_CASHFLOW")
GET_COMPANY_BALANCE_SHEET = load_env_bool("GET_COMPANY_BALANCE_SHEET")
GET_COMPANY_INCOME_STATEMENT = load_env_bool("GET_COMPANY_INCOME_STATEMENT")

GET_LAST_DAYS = int(os.environ.get("GET_LAST_DAYS", 365))
PROCESS_SINCE_LATEST_IN_DB = load_env_bool("PROCESS_SINCE_LATEST_IN_DB")

# Low thread count to prevent overloading database (important for shared/free tiers)
NUM_OF_THREADS = int(os.getenv("NUM_OF_THREADS", 4))

set_identity("PortfolioAdvisorApp/1.0 marcin@example.com")


FIRST_DATE = (datetime.now() - timedelta(days=GET_LAST_DAYS)).strftime("%Y-%m-%d")
DATE_FILTER = f"{FIRST_DATE}:"

FORM_MAPPING = {}
if GET_8K:
    FORM_MAPPING["events_8k"] = ["8-K"]
if GET_10K:
    FORM_MAPPING["annual_financials"] = ["10-K"]
if GET_10Q:
    FORM_MAPPING["quarterly_financials"] = ["10-Q"]
if GET_4:
    FORM_MAPPING["insider_trades"] = ["4"]

ALL_FORMS = [f for forms in FORM_MAPPING.values() for f in forms]

IMPORTANT_8K_ITEMS = {
    "Item 2.01": "Company completed a major acquisition or sold significant assets",
    "Item 2.02": "Company reported earnings or financial results",
    "Item 4.02": "Company can no longer rely on previously issued financial statements",
    "Item 5.02": "Key executive or board member changes",
    "Item 1.01": "Company entered into a significant contract or agreement",
}


def optimize_image(image_bytes):
    try:
        if not image_bytes:
            return None
        img = Image.open(BytesIO(image_bytes))
        output_io = BytesIO()
        # Convert to RGBA if necessary to preserve transparency, though usually logos are fine.
        # If it's already PNG, just optimizing.
        if img.mode != "RGBA" and img.mode != "RGB":
            img = img.convert("RGBA")

        img.save(output_io, format="WEBP", method=6, quality=80)
        return output_io.getvalue()
    except Exception as e:
        print(f"Error optimizing image: {e}")
        return None


def upload_to_s3_with_retry(file_bytes, key, max_retries=3):
    if not file_bytes:
        return None

    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_bucket_url = os.getenv("S3_URL")
    region = os.getenv("REGION")

    bucket_name = s3_bucket_url
    if s3_bucket_url:
        if "://" in s3_bucket_url:
            clean_url = s3_bucket_url.split("://")[-1]
            parts = clean_url.split(".")
            if "s3" in parts:
                bucket_name = parts[0]

    if not bucket_name:
        print("S3_URL or bucket name not configured.")
        return None

    try:
        s3_kwargs = {"service_name": "s3", "region_name": region or "us-east-1"}
        if aws_access_key:
            s3_kwargs["aws_access_key_id"] = aws_access_key
        if aws_secret_key:
            s3_kwargs["aws_secret_access_key"] = aws_secret_key

        s3 = boto3.client(**s3_kwargs)
    except Exception as e:
        print(f"Failed to create boto3 client: {e}")
        return None

    extra_args = {
        "ContentType": "image/webp",
        "CacheControl": "public, max-age=31536000, immutable",
    }

    for attempt in range(max_retries):
        try:
            s3.put_object(Bucket=bucket_name, Key=key, Body=file_bytes, **extra_args)
            # Use the specified region in the URL
            region_str = f"s3.{region}" if region else "s3"
            url = f"https://{bucket_name}.{region_str}.amazonaws.com/{key}"
            return url
        except ClientError as e:
            print(f"S3 Upload ClientError (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2**attempt)  # Exponential backoff: 1s, 2s, 4s...
            else:
                print(f"Failed to upload {key} after {max_retries} attempts.")
        except Exception as e:
            print(f"S3 Upload Error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
            else:
                print(f"Failed to upload {key} after {max_retries} attempts.")
    return None


def clean_for_json(obj):
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(i) for i in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if pd.isna(obj):
        return None
    return obj


def process_eightk(db: Session, cik: str, eightk):
    try:
        if eightk.form != "8-K":
            print(f"Skipping non 8K form inside process_eightk: {eightk.form}")
            return

        important_items_mentioned = False
        for key in IMPORTANT_8K_ITEMS.keys():
            if key in eightk.items:
                important_items_mentioned = True

        if important_items_mentioned:
            summary_text = ""
            if eightk.has_press_release:
                press_releases = eightk.press_releases
                summaries = []
                for pr in press_releases:
                    content = pr.text()
                    if content:
                        s = summarize_text(content)
                        summaries.extend(s)
                summary_text = "\n".join(summaries)

            def add_event():
                event = CompanyEvent(
                    items=clean_for_json(eightk.items),
                    summary=summary_text,
                    date=str(eightk.filing_date),
                    cik=cik,
                )
                db.add(event)

            db_operation_with_retry(db, add_event)

    except Exception as e:
        print(f"Error processing eight k file {e}")


def process_file_object(db: Session, cik: str, obj, accession_no: str):
    try:
        if obj.form == "8-K":
            process_eightk(db, cik, obj)
        elif obj.form == "10-Q" or obj.form == "10-K":
            pass
        elif obj.form == "4":
            # Insider Trade
            try:
                df = obj.to_dataframe()
            except Exception as e:
                print(
                    f"  [Error] Failed to parse Form 4 to dataframe for CIK {cik}: {e}"
                )
                return

            if df is not None and not df.empty:
                try:
                    # Ensure numeric columns
                    for col in ["Shares", "Price", "Value"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

                    # 1. Filter and Calculate Accurate Remaining Shares
                    if "Shares" in df.columns:
                        df["Shares"] = pd.to_numeric(
                            df["Shares"], errors="coerce"
                        ).fillna(0)
                    else:
                        df["Shares"] = 0.0

                    if "Remaining Shares" in df.columns:
                        df["Remaining Shares"] = pd.to_numeric(
                            df["Remaining Shares"], errors="coerce"
                        )

                        final_remaining = df["Remaining Shares"].iloc[-1]
                        if pd.isna(final_remaining):
                            final_remaining = 0

                        calculated_remaining = [0.0] * len(df)
                        current_holding = float(final_remaining)

                        for i in range(len(df) - 1, -1, -1):
                            row = df.iloc[i]
                            txn_code = str(row.get("Code", "")).upper()
                            txn_type = str(row.get("Transaction Type", "")).lower()
                            shares = float(row.get("Shares", 0))

                            is_acquisition = (
                                txn_code in ["P", "A", "M"]
                                or "award" in txn_type
                                or "purchase" in txn_type
                                or "grant" in txn_type
                            )
                            is_disposition = (
                                txn_code in ["S", "F", "D"]
                                or "sale" in txn_type
                                or "disposition" in txn_type
                            )

                            calculated_remaining[i] = current_holding

                            if is_acquisition:
                                current_holding -= shares
                            elif is_disposition:
                                current_holding += shares

                        df["Calculated_Remaining"] = calculated_remaining
                    else:
                        df["Calculated_Remaining"] = 0

                    # 2. Filter for explicitly requested Buys (P) and Sells (S) ONLY
                    if "Code" not in df.columns:
                        print(
                            f"  [Warning] 'Code' column missing in Form 4 for CIK {cik}"
                        )
                        return

                    target_codes = ["P", "S"]
                    df_filtered = df[df["Code"].isin(target_codes)].copy()

                    # 3. Add sequence number to Date to force correct sorting
                    records = df_filtered.to_dict("records")

                    for i, record in enumerate(records):
                        base_date = str(record.get("Date"))
                        sorted_date = f"{base_date}-{accession_no}-{i:02d}"

                        trade_values = {
                            "date": sorted_date,
                            "issuer": record.get("Issuer"),
                            "insider": record.get("Insider"),
                            "position": record.get("Position"),
                            "transaction_type": record.get("Transaction Type"),
                            "code": record.get("Code"),
                            "description": record.get("Description")
                            if "Description" in record
                            else None,
                            "shares": record.get("Shares"),
                            "price": record.get("Price"),
                            "value": record.get("Value"),
                            "remaining_shares": record.get("Calculated_Remaining"),
                            "cik": cik,
                        }

                        def insert_trade():
                            stmt = (
                                pg_insert(InsiderTrade)
                                .values(**trade_values)
                                .on_conflict_do_nothing(index_elements=["date", "cik"])
                            )
                            db.execute(stmt)

                        db_operation_with_retry(db, insert_trade)
                except Exception as e:
                    print(
                        f"  [Error] Failed to process Form 4 dataframe for CIK {cik}: {e}"
                    )
                    # traceback.print_exc() # Uncomment if needed for deep debug

        else:
            pass
    except Exception as e:
        print(f"Error processing file object {e} for form {obj.form}")
        traceback.print_exc()


def process_company(ticker, cik, prefetched_filings=None, ticker_latest_date=None):
    cik = str(cik)

    # Check for shutdown signal early
    if _shutdown_requested:
        print(f"🛑 Skipping {ticker} (CIK {cik}) - shutdown requested")
        return

    should_update_info = (
        GET_COMPANY_INFO
        or GET_COMPANY_CASHFLOW
        or GET_COMPANY_BALANCE_SHEET
        or GET_COMPANY_INCOME_STATEMENT
    )

    # If GET_COMPANY_INFO is ON, we might need to process even if there are no filings
    # to add missing metadata (icon, industry) or create a new Ticker entry.
    if prefetched_filings is not None and not prefetched_filings and not should_update_info:
        return

    print(f"Processing {ticker} (CIK {cik})")

    is_new = False
    needs_metadata = False
    ticker_obj_exists = False

    # PHASE 1: Quick DB Read (Check State)
    # We open/close session quickly just to check what we need to do
    with get_db() as db:
        try:
            stmt = select(Ticker.cik, Ticker.display_name, Ticker.industry).where(Ticker.cik == cik)
            row = db.execute(stmt).first()
            if row:
                ticker_obj_exists = True
                if not row.display_name or not row.industry:
                    needs_metadata = True
            else:
                is_new = True
                
            # If we need to filter filings based on DB state, get the latest date now
            latest_db_date = None
            if PROCESS_SINCE_LATEST_IN_DB and not ticker_latest_date:
                latest_db_date = db.execute(
                    select(InsiderTrade.date)
                    .where(InsiderTrade.cik == cik)
                    .order_by(desc(InsiderTrade.date))
                    .limit(1)
                ).scalar()
        except Exception as e:
            print(f"Error checking state for {ticker}: {e}")
            return

    # PHASE 2: Network I/O (NO DB CONNECTION HELD)
    # We do all slow fetching here without holding a db session
    company_data = None
    files_to_process = []
    
    # Needs updating?
    run_update = is_new or (should_update_info and (needs_metadata or GET_COMPANY_CASHFLOW or GET_COMPANY_BALANCE_SHEET or GET_COMPANY_INCOME_STATEMENT))
    
    if run_update:
        # Fetch company metadata from SEC
        try:
            company = Company(cik)
            # Pre-fetch properties we might need to trigger network calls
            # (We access them here so the edgar lib fetches them)
            pd_name = company.display_name
            pd_ind = company.industry
            
            # Fetch Financials if requested
            pd_cash = clean_for_json(company.cash_flow().to_dataframe().to_dict()) if GET_COMPANY_CASHFLOW else None
            pd_bal = clean_for_json(company.balance_sheet().to_dataframe().to_dict()) if GET_COMPANY_BALANCE_SHEET else None
            pd_inc = clean_for_json(company.income_statement().to_dataframe().to_dict()) if GET_COMPANY_INCOME_STATEMENT else None
            
            # Collect into a struct to pass to DB phase
            company_data = {
                "display_name": pd_name,
                "industry": pd_ind,
                "cash_flow": pd_cash,
                "balance_sheet": pd_bal,
                "income_statement": pd_inc,
                "needs_icon": is_new or needs_metadata
            }
            
            # Icon update happens in network phase (S3 upload)
            if GET_COMPANY_INFO and (is_new or needs_metadata):
                try:
                    raw_icon = company.get_icon()
                    if raw_icon:
                        optimized_icon = optimize_image(raw_icon)
                        if optimized_icon:
                            s3_key = f"icons/tickers/{ticker}.webp"
                            upload_to_s3_with_retry(optimized_icon, s3_key)
                except Exception as e:
                   # print(f"Warning: Could not update icon for {ticker}: {e}")
                   pass
        except Exception as e:
            print(f"Warning: Failed to fetch metadata for {ticker}: {e}")
            
    # Fetch Filings
    if prefetched_filings is not None:
        raw_files = prefetched_filings
    elif ALL_FORMS:
        # Determine date filter
        if ticker_latest_date:
            date_filter = f"{ticker_latest_date[:10]}:"
        elif latest_db_date:
             date_filter = f"{latest_db_date[:10]}:"
        else:
            date_filter = DATE_FILTER
            
        try:
            company_scan = Company(cik) # lightweight if just for ID
            raw_files = fetch_with_retry(
                company_scan.get_filings, filing_date=date_filter, form=ALL_FORMS
            )
        except Exception as e:
            print(f"Error fetching filings list for {ticker}: {e}")
            raw_files = []
    else:
        raw_files = []

    # Download actual filing content (The Slow Part)
    # We buffer these in memory. Assuming reasonable batch size.
    current_batch_data = []
    
    # Limit max batch size to avoid OOM if a ticker has 1000 new filings
    MAX_BATCH = 50 
    
    for i, file in enumerate(raw_files):
        try:
             # Network call to fetch text/html - NO DB LOCK
            obj = fetch_with_retry(file.obj)
            current_batch_data.append((obj, file.accession_no))
            
            # If we hit batch limit, we could flush here. 
            # For simplicity in this refactor, let's just process what we have at the end, 
            # or break if too many? Let's just store specific count.
            if len(current_batch_data) >= MAX_BATCH:
                break 
        except Exception:
            pass

    # PHASE 3: Persist (Write Transaction)
    # Open DB connection again just to write whatever we collected
    with get_db() as db:
        try:
            # 1. Update Ticker / Metadata
            stmt = select(Ticker).where(Ticker.cik == cik)
            t_obj = db.execute(stmt).scalars().first()
            
            if not t_obj:
                t_obj = Ticker(ticker=ticker, cik=cik)
                db.add(t_obj)
                db.flush() # get ID/ensure existence
                
            if company_data:
                if GET_COMPANY_INFO:
                    if not t_obj.display_name and company_data.get("display_name"):
                        t_obj.display_name = company_data["display_name"]
                    if not t_obj.industry and company_data.get("industry"):
                        t_obj.industry = company_data["industry"]
                    t_obj.last_updated = datetime.now()
                
                if company_data.get("cash_flow"):
                    t_obj.cash_flow = company_data["cash_flow"]
                if company_data.get("balance_sheet"):
                    t_obj.balance_sheet = company_data["balance_sheet"]
                if company_data.get("income_statement"):
                    t_obj.income_statement = company_data["income_statement"]
            
            # 2. Process gathered filings
            for obj, acc in current_batch_data:
                try:
                    process_file_object(db, cik, obj, acc)
                except Exception:
                    pass
            
            # Single Commit
            commit_with_retry(db)
            
        except Exception as e:
            db.rollback()
            print(f"Error persisting data for {ticker}: {e}")


def main():
    print("Initializing Database...")
    engine = init_db_engine()
    print("Database Initialized.")

    with get_db() as db:
        # 1. Cleanup old data
        if GET_8K:
            days_ago_8k = os.getenv("DELETE_8K_BEFORE_DAYS_AGO")
            if days_ago_8k:
                try:
                    cutoff_date = (
                        datetime.now() - timedelta(days=int(days_ago_8k))
                    ).strftime("%Y-%m-%d")
                    print(f"Deleting 8-K events older than {cutoff_date}...")
                    stmt = delete(CompanyEvent).where(CompanyEvent.date <= cutoff_date)
                    result = db.execute(stmt)
                    commit_with_retry(db)
                    print(f"Deleted {result.rowcount} old 8-K events.")
                except Exception as e:
                    print(f"Error deleting old 8-K: {e}")
                    db.rollback()

        if GET_4:
            days_ago_4 = os.getenv("DELETE_4_BEFORE_DAYS_AGO")
            if days_ago_4:
                try:
                    cutoff_date = (
                        datetime.now() - timedelta(days=int(days_ago_4))
                    ).strftime("%Y-%m-%d")
                    print(f"Deleting Form 4 trades older than {cutoff_date}...")
                    stmt = delete(InsiderTrade).where(
                        func.substr(InsiderTrade.date, 1, 10) <= cutoff_date
                    )
                    result = db.execute(stmt)
                    commit_with_retry(db)
                    print(f"Deleted {result.rowcount} old Form 4 trades.")
                except Exception as e:
                    print(f"Error deleting old Form 4: {e}")
                    db.rollback()

    # 2. Build Filing Index (OPTIMIZATION)
    active_filings_map = None
    cik_to_latest_trade = {}

    with get_db() as db:
        print("Optimizing process: Fetching market-wide filing index...")

        # Determine starting date
        if PROCESS_SINCE_LATEST_IN_DB:
            # Global latest trade
            global_latest = db.execute(select(func.max(InsiderTrade.date))).scalar()
            global_start_date = (
                global_latest[:10]
                if (global_latest and len(global_latest) >= 10)
                else FIRST_DATE
            )

            # Per-ticker latest trade (for fallback filtering)
            trade_rows = db.execute(
                select(InsiderTrade.cik, func.max(InsiderTrade.date)).group_by(
                    InsiderTrade.cik
                )
            ).all()
            cik_to_latest_trade = {str(row[0]): row[1] for row in trade_rows}
        else:
            global_start_date = FIRST_DATE  # User wants last X days

        # SEC Global Search
        try:
            print(
                f"Searching for {ALL_FORMS} across market since {global_start_date}..."
            )
            all_filings = fetch_with_retry(
                get_filings, filing_date=f"{global_start_date}:", form=ALL_FORMS
            )

            active_filings_map = defaultdict(list)
            for f in all_filings:
                active_filings_map[str(f.cik)].append(f)

            # Real-time capture (SEC Index typically lags 1 day, this fills the gap for today)
            try:
                print("Fetching current real-time filings...")
                current = fetch_with_retry(get_current_filings)
                if current:
                    count = 0
                    for f in current:
                        if f.form in ALL_FORMS:
                            active_filings_map[str(f.cik)].append(f)
                            count += 1
                    print(f"Added {count} real-time filings to the queue.")
            except Exception as e:
                print(f"Warning: Could not fetch real-time filings: {e}")

            print(
                f"Index fetch complete. Found {len(all_filings)} filings across {len(active_filings_map)} companies."
            )
        except Exception as e:
            print(
                f"Warning: Global index fetch failed, falling back to per-ticker search: {e}"
            )
            active_filings_map = None

    # 3. Fetch Ticker List
    cids_raw = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers={"User-Agent": "PortfolioAdvisorApp/1.0 marcin@example.com"},
    ).json()
    cids = [(value["ticker"], value["cik_str"]) for _, value in cids_raw.items()]

    # 4. Filter work list
    if active_filings_map is not None:
        active_ciks = set(active_filings_map.keys())

        # If updating info, also include companies not yet in our Ticker table
        if GET_COMPANY_INFO:
            with get_db() as db:
                db_ciks = {str(c) for c in db.execute(select(Ticker.cik)).scalars()}
            missing_ciks = {str(c) for t, c in cids if str(c) not in db_ciks}
            active_ciks.update(missing_ciks)

        cids = [(t, c) for t, c in cids if str(c) in active_ciks]
        print(f"Filtered to {len(cids)} companies to process based on recent activity.")

    # 5. Execute Threads
    if not cids:
        print("No companies to process in this run.")
        return

    from concurrent.futures import wait, FIRST_COMPLETED

    executor = ThreadPoolExecutor(max_workers=NUM_OF_THREADS)
    active_futures = set()
    future_to_cid = {}
    ticker_iter = iter(cids)
    submitted_count = 0
    total_count = len(cids)

    print(f"Starting streaming extraction for {total_count} companies with {NUM_OF_THREADS} threads...")

    try:
        while True:
            # 1. Fill the executor capacity (only up to NUM_OF_THREADS active tasks)
            while len(active_futures) < NUM_OF_THREADS:
                if _shutdown_requested:
                    break
                try:
                    ticker, cik = next(ticker_iter)
                    submitted_count += 1
                    
                    # Submit only when there is capacity
                    future = executor.submit(
                        process_company,
                        ticker,
                        cik,
                        active_filings_map.get(str(cik)) if active_filings_map is not None else None,
                        cik_to_latest_trade.get(str(cik)),
                    )
                    active_futures.add(future)
                    future_to_cid[future] = (ticker, cik)
                    
                    # Progress log every 100 submissions
                    if submitted_count % 100 == 0:
                        print(f"  [Progress] Streamed {submitted_count}/{total_count} tickers...")
                        
                except StopIteration:
                    break # No more tickers to submit
            
            if not active_futures:
                break # Everything submitted and completed
                
            # 2. Wait for at least one task to complete before submitting more
            # This is the "streaming" part - we don't queue 4000 items at once
            done, active_futures = wait(active_futures, return_when=FIRST_COMPLETED)
            
            for future in done:
                ticker, cik = future_to_cid.pop(future)
                try:
                    future.result()
                    # print(f"✓ Completed {ticker} (CIK {cik})")
                except Exception as exc:
                    # Check if it's a database overload issue or shutdown
                    msg = str(exc).lower()
                    is_db_overload = any(
                        err in msg
                        for err in [
                            "too many connections",
                            "max client connections",
                            "connection limit exceeded",
                            "server is starting up",
                            "database system is starting up",
                            "deadlock detected",
                        ]
                    )
                    is_keyboard_interrupt = "keyboardinterrupt" in msg or _shutdown_requested

                    if is_keyboard_interrupt:
                        print(f"⏹️  {ticker} cancelled due to shutdown")
                    elif is_db_overload:
                        print(f"⚠️  {ticker} failed due to database overload: {exc}")
                    else:
                        print(f"❌ {ticker} failed with error: {exc}")
            
            # Check for shutdown signal to clear remaining
            if _shutdown_requested:
                print("🛑 Shutdown requested, canceling pending jobs...")
                executor.shutdown(wait=False, cancel_futures=True)
                break
                    
    finally:
        # Final cleanup - ensure executor is shut down
        executor.shutdown(wait=True)

    print("\nExtraction complete. Data saved to Database.")


if __name__ == "__main__":
    main()
