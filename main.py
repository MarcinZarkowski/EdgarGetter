from edgar import latest_filings
from datetime import datetime, timedelta
from edgar import Company, set_identity
import pandas as pd
import json
import time
import sys
import math
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.summarizer import summarize_text
from dotenv import load_dotenv
import os
import random
import requests
from sqlalchemy.orm import Session
from sqlalchemy import select, delete, cast, String, func

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

NUM_OF_THREADS = int(os.getenv("NUM_OF_THREADS", 20))

set_identity("PortfolioAdvisorApp/1.0 marcin@example.com")


FIRST_DATE = (datetime.now() - timedelta(days=GET_LAST_DAYS)).strftime('%Y-%m-%d')
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
            
            event = CompanyEvent(
                items=clean_for_json(eightk.items), 
                summary=summary_text,
                date=str(eightk.filing_date),
                cik=cik
            )
            db.add(event)

    except Exception as e:
        print(f"Error processing eight k file {e}")

def process_file_object(db: Session, cik: str, obj):
    try:
        if obj.form == "8-K":
            process_eightk(db, cik, obj)
        elif obj.form == "10-Q" or obj.form == "10-K":
            pass
        elif obj.form == "4":
            # Insider Trade
            df = obj.to_dataframe()
            if df is not None and not df.empty:
                # Ensure numeric columns
                for col in ['Shares', 'Price', 'Value']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        
                # 1. Filter and Calculate Accurate Remaining Shares
                df['Shares'] = pd.to_numeric(df['Shares'], errors='coerce').fillna(0)
                
                if 'Remaining Shares' in df.columns:
                     df['Remaining Shares'] = pd.to_numeric(df['Remaining Shares'], errors='coerce')
                     
                     final_remaining = df['Remaining Shares'].iloc[-1]
                     if pd.isna(final_remaining):
                         final_remaining = 0 
                         
                     calculated_remaining = [0.0] * len(df)
                     current_holding = float(final_remaining)
                     
                     for i in range(len(df) - 1, -1, -1):
                         row = df.iloc[i]
                         txn_code = str(row.get('Code', '')).upper()
                         txn_type = str(row.get('Transaction Type', '')).lower()
                         shares = float(row.get('Shares', 0))
                         
                         is_acquisition = txn_code in ['P', 'A', 'M'] or 'award' in txn_type or 'purchase' in txn_type or 'grant' in txn_type
                         is_disposition = txn_code in ['S', 'F', 'D'] or 'sale' in txn_type or 'disposition' in txn_type

                         calculated_remaining[i] = current_holding
                         
                         if is_acquisition:
                             current_holding -= shares
                         elif is_disposition:
                             current_holding += shares
                             
                     df['Calculated_Remaining'] = calculated_remaining
                else:
                    df['Calculated_Remaining'] = 0

                # 2. Filter for explicitly requested Buys (P) and Sells (S) ONLY
                target_codes = ['P', 'S']
                df_filtered = df[df['Code'].isin(target_codes)].copy()
                
                # 3. Add sequence number to Date to force correct sorting
                import uuid
                file_uuid = str(uuid.uuid4())[:4]
                
                records = df_filtered.to_dict('records')
                
                for i, record in enumerate(records):
                    base_date = str(record.get("Date"))
                    
                    sorted_date = f"{base_date}-{file_uuid}-{i:02d}"
                    
                    trade = InsiderTrade(
                        date=sorted_date,
                        issuer=record.get("Issuer"),
                        insider=record.get("Insider"),
                        position=record.get("Position"),
                        
                        transaction_type=record.get("Transaction Type"),
                        code=record.get("Code"),
                        description=record.get("Description") if "Description" in record else None,
                        shares=record.get("Shares"),
                        price=record.get("Price"),
                        value=record.get("Value"),
                        remaining_shares=record.get("Calculated_Remaining"),
                        
                        cik=cik
                    )
                    db.add(trade)

        elif obj.form == "13F-HR" or obj.form == "13F-HR/A":
            pass
        else:
            pass
    except Exception as e:
        print(f"Error processing file object {e} for form {obj.form}")


def process_company(ticker, cik):
    print(f"\n--- Processing {ticker} ---")
    cik = str(cik)
    
    should_update_info = GET_COMPANY_INFO or GET_COMPANY_CASHFLOW or GET_COMPANY_BALANCE_SHEET or GET_COMPANY_INCOME_STATEMENT
    
    company = None
    if should_update_info or ALL_FORMS:
         company = Company(cik)

    with get_db() as db:
        try:
            if should_update_info:
                stmt = select(Ticker).where(Ticker.cik == cik)
                result = db.execute(stmt).scalars().first()
                if not result:
                    ticker_obj = Ticker(ticker=ticker, cik=cik)
                    db.add(ticker_obj)
                    db.flush() 
                else:
                    ticker_obj = result
                
                if GET_COMPANY_INFO:
                    ticker_obj.display_name = company.display_name
                    ticker_obj.icon = company.get_icon()
                    ticker_obj.industry = company.industry
                    ticker_obj.last_updated = datetime.now()
                
                if GET_COMPANY_CASHFLOW:
                    ticker_obj.cash_flow = clean_for_json(company.cash_flow().to_dataframe().to_dict())
                if GET_COMPANY_BALANCE_SHEET:
                    ticker_obj.balance_sheet = clean_for_json(company.balance_sheet().to_dataframe().to_dict())
                if GET_COMPANY_INCOME_STATEMENT:
                    ticker_obj.income_statement = clean_for_json(company.income_statement().to_dataframe().to_dict())
                
                db.commit() 
            else:
                from sqlalchemy.dialects.postgresql import insert as pg_insert
                stmt = pg_insert(Ticker).values(ticker=ticker, cik=cik).on_conflict_do_nothing(index_elements=['cik'])
                db.execute(stmt)
                db.commit()

        except Exception as e:
            db.rollback()
            print(f"Error updating Ticker {ticker}: {e}")
            return

    def fetch_with_retry(func, *args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if "Too Many Requests" in str(e) or "429" in str(e):
                     sleep_time = random.uniform(0.1, 0.2)
                     time.sleep(sleep_time)
                else:
                    raise e

    if ALL_FORMS:
        try:
            files = fetch_with_retry(company.get_filings, filing_date=DATE_FILTER, form=ALL_FORMS)
        except Exception as e:
            print(f"Error fetching filings for {ticker}: {e}")
            files = []

        for file in files:
            try:
                obj = fetch_with_retry(file.obj)
                
                with get_db() as db:
                    process_file_object(db, cik, obj)
                    db.commit()

            except Exception as e:
                 pass
    
    print(f"Completed processing for {ticker}")

def main():
    print("Initializing Database...")
    engine = init_db_engine()
    print("Database Initialized.")

    with get_db() as db:
        if GET_8K:
            days_ago_8k = os.getenv("DELETE_8K_BEFORE_DAYS_AGO")
            if days_ago_8k:
                try:
                    cutoff_date = (datetime.now() - timedelta(days=int(days_ago_8k))).strftime('%Y-%m-%d')
                    print(f"Deleting 8-K events older than {cutoff_date}...")
                    stmt = delete(CompanyEvent).where(CompanyEvent.date <= cutoff_date)
                    result = db.execute(stmt)
                    print(f"Deleted {result.rowcount} old 8-K events.")
                    db.commit()
                except Exception as e:
                    print(f"Error deleting old 8-K events: {e}")
                    db.rollback()

        if GET_4:
            days_ago_4 = os.getenv("DELETE_4_BEFORE_DAYS_AGO")
            if days_ago_4:
                try:
                    cutoff_date = (datetime.now() - timedelta(days=int(days_ago_4))).strftime('%Y-%m-%d')
                    print(f"Deleting Form 4 trades older than {cutoff_date}...")
                    stmt = delete(InsiderTrade).where(func.substr(InsiderTrade.date, 1, 10) <= cutoff_date)
                    result = db.execute(stmt)
                    print(f"Deleted {result.rowcount} old Form 4 trades.")
                    db.commit()
                except Exception as e:
                    print(f"Error deleting old Form 4 trades: {e}")
                    db.rollback()


    cids = requests.get("https://www.sec.gov/files/company_tickers.json", headers={"User-Agent": "PortfolioAdvisorApp/1.0 marcin@example.com"}).json()
    cids = [ (value["ticker"], value["cik_str"]) for _, value in cids.items()]
    
    with ThreadPoolExecutor(max_workers=NUM_OF_THREADS) as executor:
        future_to_cid = {executor.submit(process_company, ticker, cik): (ticker, cik) for ticker, cik in cids}
        
        for future in as_completed(future_to_cid):
            ticker, cik = future_to_cid[future]
            try:
                future.result()
            except Exception as exc:
                print(f"{ticker} generated an exception: {exc}")
    
    print("\n Extraction complete. Data saved to Database.")

if __name__ == "__main__":
    main()