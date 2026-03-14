import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))

# Automatically switch to the virtual environment if not already using it
# (must happen before importing third-party libraries like pandas)
VENV_PYTHON = os.path.join(PROJECT_ROOT, ".venv", "bin", "python3")
if sys.executable != VENV_PYTHON and os.path.exists(VENV_PYTHON):
    print("Auto-activating virtual environment...")
    os.execl(VENV_PYTHON, VENV_PYTHON, *sys.argv)

sys.path.insert(0, PROJECT_ROOT)

import re
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import requests
from datetime import datetime, timedelta

from aws_db_conn import get_athena_client
from aws_db_exec import run_query, fetch_df
from aws_db_creds import DATABASE, S3_STAGING_DIR

# Fix for Netskope/SSL error
os.environ['AWS_CA_BUNDLE'] = '/etc/ssl/cert.pem'

# --- CONFIGURATION ---
REPORT_DATE = "2026-03-08"  # THE ONLY DATE YOU NEED TO CHANGE
# ---------------------

# Dynamic Period Calculation
base_dt = datetime.strptime(REPORT_DATE, "%Y-%m-%d")

# 1. Month-1: Last day of previous month
first_day_curr_month = base_dt.replace(day=1)
last_day_prev_month = (first_day_curr_month - timedelta(days=1)).strftime("%Y-%m-%d")

PERIODS = {
    "M-1 Cumulative (Monthly)": {
        "date": last_day_prev_month,
        "window": 30,
        "label": "Month"
    },
    "W-1 M-0 (Weekly)": {
        "date": (base_dt - timedelta(days=7)).strftime("%Y-%m-%d"),
        "window": 7,
        "label": "Week"
    },
    "W-0 M-0 (Weekly)": {
        "date": REPORT_DATE,
        "window": 7,
        "label": "Week"
    }
}

SPREADSHEET_ID = "1x_dx3SE4btMVduhIdu2vPp7R0JMq2t_SicmEToItLHs"
LOOKUP_SWAPS_THRESHOLD = 5
LOOKUP_DD_THRESHOLD = 0

def categorize(row, window_size, label):
    d = row.get('days_from_last_connected')
    f = row.get('days_from_last_connection_attempt')
    dd = row.get('days_for_soc_depletion')
    swaps = row.get('swaps', 0)
    week_pac = row.get('week_pac', 0)
    
    # Safely handle NaNs for math
    d = float(d) if pd.notnull(d) and d != '' else None
    f = float(f) if pd.notnull(f) and f != '' else None
    dd = float(dd) if pd.notnull(dd) and dd != '' else None
    swaps = float(swaps) if pd.notnull(swaps) and swaps != '' else 0
    week_pac = float(week_pac) if pd.notnull(week_pac) and week_pac != '' else 0

    # Hourly connectivity logic (Denom: 168 for week, 720 for month)
    denom = 24.0 * window_size
    hourly_conn = week_pac / denom

    d_null = d is None
    f_null = f is None

    # Update health check to use the dynamic window (7 or 30)
    limit = window_size

    # 1. Connected within the period
    if not f_null and f <= limit and not d_null and d <= limit:
        if hourly_conn >= 0.75: return "1. Connected - Healthy Connection"
        elif hourly_conn >= 0.30: return "1. Connected - Intermittent Connection"
        else: return "1. Connected - Low connection"

    # 2. Connected to Server but no packets sent
    if not f_null and f <= limit and (d_null or d > limit):
        return "2. Connected to Server but no packets sent"

    # 3. Not connected for > Period (first bracket)
    if not f_null and limit < f <= (limit + 23) and not d_null and limit < d <= (limit + 23):
        if dd is not None and dd <= LOOKUP_DD_THRESHOLD: return "3. Disconnected (Bracket 1) - Potential Deep Discharge"
        if swaps >= LOOKUP_SWAPS_THRESHOLD: return "3. Disconnected (Bracket 1) - Actively Swapping"
        return "3. Disconnected (Bracket 1) - Not Swapping"

    # 4. Long term disconnect (second bracket)
    if not f_null and f > (limit + 23) and not d_null and d > (limit + 23):
        if dd is not None and dd <= LOOKUP_DD_THRESHOLD: return "4. Disconnected (Bracket 2) - Potential Deep Discharge"
        if swaps >= LOOKUP_SWAPS_THRESHOLD: return "4. Disconnected (Bracket 2) - Actively Swapping"
        return "4. Disconnected (Bracket 2) - Not Swapping"

    # 5. Never connected to Server but packets sent
    if f_null and not d_null:
        if swaps > 0: return "5. Never connected to Server but packets sent - Swapped at least once"
        return "5. Never connected to Server but packets sent - Never Swapped"

    # 6. Never connected to server & never sent a packet
    if f_null and d_null:
        if swaps > 0: return "6. Never connected to server & never sent a packet - Swapped at least once"
        return "6. Never connected to server & never sent a packet - Never Swapped"

    return "7. Other / Uncategorized"


if __name__ == "__main__":
    client = get_athena_client()
    with open("connections_vs_packets.sql") as f:
        base_query = f.read()

    all_dfs = []

    # Iterate through periods and execute dynamic queries
    for name, config in PERIODS.items():
        target_date = config['date']
        window = config['window']
        label = config['label']
        
        print(f"\n--- Processing {name} ---")
        print(f"  Target Date: {target_date}")
        print(f"  Health Window: {window} days")
        
        # 1. Inject the modular date
        query_modified = base_query.replace("{{target_date}}", target_date)
        
        # 2. Dynamically update the SQL window from < 7 to < 30 if needed (for week_pac calc)
        if window != 7:
            query_modified = query_modified.replace("<= 7", f"<= {window}")
            query_modified = query_modified.replace("< 7", f"< {window}")
            
        # Print a snippet of the generated SQL for verification
        print("  SQL Snippet (Dates CTE):")
        print("  " + "\n  ".join(query_modified.splitlines()[:3]))
        
        qid = run_query(client, query_modified, DATABASE, S3_STAGING_DIR)
        print("Fetching results...")
        df = fetch_df(client, qid)
        
        print(f"Applying {label}-based Categorization for {len(df)} rows...")
        df['Super Category'] = name
        df['Status'] = df.apply(lambda row: categorize(row, window, label), axis=1)
        all_dfs.append(df)

    final_df = pd.concat(all_dfs, ignore_index=True)

    # Generate Summary Pivot Table
    categories_order = list(PERIODS.keys())
    summary_df = pd.crosstab(final_df['Status'], final_df['Super Category'])
    summary_df = summary_df.reindex(columns=categories_order, fill_value=0)
    summary_df.reset_index(inplace=True)

    print("\n---------- GENERATED SUMMARY ----------")
    print(summary_df.to_string(index=False))
    print("---------------------------------------")

    try:
        print("\nConnecting to Google Sheets...")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        original_request = requests.Session.request
        def new_request(self, method, url, **kwargs):
            kwargs.setdefault('verify', False)
            return original_request(self, method, url, **kwargs)
        requests.Session.request = new_request

        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file('write_ghsteet.json', scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)

        # WRITE SUMMARY ONLY
        print("Uploading Python Auto-Summary to 'Auto Summary' tab...")
        try:
            ws_sum = sh.worksheet("Auto Summary")
            ws_sum.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws_sum = sh.add_worksheet(title="Auto Summary", rows="100", cols="10")
            
        upload_sum = summary_df.astype(object).fillna("")
        ws_sum.update([upload_sum.columns.values.tolist()] + upload_sum.values.tolist(), value_input_option='USER_ENTERED')

        print(f"\n✅ Successfully finished! Summary populated at: {sh.url}")

    except Exception as e:
        print(f"FAILED to upload to Sheets: {e}")
