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

GET_LAST_DAYS = int(os.environ.get("GET_LAST_DAYS", 365))

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
# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

IMPORTANT_8K_ITEMS = {
    "Item 2.01": "Company completed a major acquisition or sold significant assets",
    "Item 2.02": "Company reported earnings or financial results",
    "Item 4.02": "Company can no longer rely on previously issued financial statements",
    "Item 5.02": "Key executive or board member changes",
    "Item 1.01": "Company entered into a significant contract or agreement",
}





def process_eightk(eightk):
    res = {}
    try:
        #res["report_date"] = eightk.

        if eightk.form != "8-K":
             print(f"Skipping non 8K form inside process_eightk: {eightk.form}")
             return res

        print(f"DEBUG: 8K Items: {eightk.items}")
        
        important_items_mentioned = []
        for key in IMPORTANT_8K_ITEMS.keys():
            if key in eightk.items:
                print(f"DEBUG: Match found for {key}")
                important_items_mentioned.append(key)   
        
        if important_items_mentioned:
             res["items"] = important_items_mentioned
             
             if eightk.has_press_release:
                press_releases = eightk.press_releases
                res["summary"] = []
                for pr in press_releases:
                    content = pr.text()
                    if content:
                        summary = summarize_text(content)
                        res["summary"].extend(summary)
                        
                        # Print for verification
                        print(f"\n[Generated Summary - {len(summary)} sentences]")
                        for s in summary:
                            print(f"- {s}")
                            
        return res
    except Exception as e:
        print(f"Error processing eight k file {e}")
        return res

def process_file_object(res, obj):
    try:
        if obj.form == "8-K":
            res["8-K"] = process_eightk(obj)
        elif obj.form == "10-Q" or obj.form == "10-K":
            res[obj.form].append(obj.balance_sheet.to_dataframe().to_dict())
        elif obj.form == "4":
            res[obj.form].append(obj.to_dataframe().to_dict())
    except Exception as e:
        print(f"Error processing file object {e}")
        raise e


def process_company(ticker):
    print(f"\n--- Processing {ticker} ---")
    res = {}
    try:
        company = Company(ticker)

        if GET_COMPANY_INFO:
            res["display_name"] = company.display_name
            res["icon"] = company.get_icon()
            res["tickers"] = company.tickers
            res["industry"] = company.industry

        if GET_COMPANY_CASHFLOW:
            res["cash_flow"] = company.cash_flow().to_dataframe().to_dict()

        if GET_COMPANY_BALANCE_SHEET:
            res["balance_sheet"] = company.balance_sheet().to_dataframe().to_dict()
       
        for t in ALL_FORMS:
            res[t] = []

        if not ALL_FORMS:
            return res

        files = company.get_filings(filing_date=DATE_FILTER, form=ALL_FORMS)

        for file in files:
            while True:
                try:
                    obj = file.obj()
                    process_file_object(res, obj)
                    break
                except Exception as e:
                    if "Too Many Requests" in str(e):
                        print(f"Rate limited for {ticker}, retrying in 1s...")
                        time.sleep(.1)
                        continue
                    else:
                        print(f"Error processing file for {ticker}: {e}")
                        break

        return res

    except Exception as e:
        print(f"CRITICAL ERROR for {ticker}: {e}")
        return res
    
def main():
    tickers = [ "TSLA", "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL", "META"]
    database = {}
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_ticker = {executor.submit(process_company, ticker): ticker for ticker in tickers}
        
        for future in as_completed(future_to_ticker):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                if result:
                    database[ticker] = result
                print(f"Completed processing for {ticker}")
            except Exception as exc:
                print(f"{ticker} generated an exception: {exc}")
        
    with open("market_intel.json", "w") as f:
        json.dump(database, f, indent=2, default=str)
    
    print("\n Extraction complete. Saved to market_intel.json")

if __name__ == "__main__":
    main()