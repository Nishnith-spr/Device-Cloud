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
REPORT_DATE = "2026-03-15"  # THE ONLY DATE YOU NEED TO CHANGE
# ---------------------

# Dynamic Period Calculation
base_dt = datetime.strptime(REPORT_DATE, "%Y-%m-%d")

# 1. Month-1: Last day of previous month
first_day_curr_month = base_dt.replace(day=1)
last_day_prev_month = (first_day_curr_month - timedelta(days=1)).strftime("%Y-%m-%d")

PERIODS = {
    "W-0 M-0 (Weekly)": {
        "date": REPORT_DATE,
        "window": 7,
        "label": "Week"
    },
    "W-1 M-0 (Weekly)": {
        "date": (base_dt - timedelta(days=7)).strftime("%Y-%m-%d"),
        "window": 7,
        "label": "Week"
    },
    "W-2 M-0 (Weekly)": {
        "date": (base_dt - timedelta(days=14)).strftime("%Y-%m-%d"),
        "window": 7,
        "label": "Week"
    },
    "M-1 Cumulative (Monthly)": {
        "date": last_day_prev_month,
        "window": 30,
        "label": "Month"
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
    if os.path.exists("Iot_but_not_in_crm.sql"):
        with open("Iot_but_not_in_crm.sql") as f:
            iot_gap_query = f.read()
    else:
        iot_gap_query = None

    all_dfs = []
    gap_dfs = []

    # Iterate through periods and execute dynamic queries
    for name, config in PERIODS.items():
        target_date = config['date']
        window = config['window']
        label = config['label']
        
        print(f"\n--- Processing {name} ---")
        
        # 1. Main Connectivity Query
        query_modified = base_query.replace("{{target_date}}", target_date)
        # Use an explicit health window replacement
        query_modified = query_modified.replace("{{health_window}}", str(window))
        
        # Fallback for older versions of the SQL file if placeholders aren't there yet
        if "{{health_window}}" not in base_query:
            if window != 7:
                query_modified = query_modified.replace("<= 7", f"<= {window}").replace("< 7", f"< {window}")

        print(f"  Target: {target_date} | Window: {window} days ({label} View)")
        qid = run_query(client, query_modified, DATABASE, S3_STAGING_DIR)
        print(f"Fetching connectivity results for {target_date}...")
        df = fetch_df(client, qid)
        df['Super Category'] = name
        df['Status'] = df.apply(lambda row: categorize(row, window, label), axis=1)
        all_dfs.append(df)

        # 2. IoT but not in CRM Gap Analysis
        if iot_gap_query:
            print(f"  Calculating IoT/CRM Gap for {target_date}...")
            gap_modified = iot_gap_query.replace("{{target_date}}", target_date)
            qid_gap = run_query(client, gap_modified, DATABASE, S3_STAGING_DIR)
            df_gap = fetch_df(client, qid_gap)
            df_gap['Super Category'] = name
            gap_dfs.append(df_gap)

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df['circulation_batteries'] = pd.to_numeric(final_df['circulation_batteries'], errors='coerce').fillna(0)
    final_gap_df = pd.concat(gap_dfs, ignore_index=True) if gap_dfs else pd.DataFrame()

    try:
        print("\nConnecting to Google Sheets...")
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Robust SSL bypass for corporate proxies
        session = requests.Session()
        session.verify = False
        
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file('write_ghsteet.json', scopes=scopes)
        # Authorize with a session that has verify=False
        gc = gspread.authorize(creds)
        # Note: gspread might still use its own defaults, so we keep the global patch too
        original_request = requests.Session.request
        def new_request(self, method, url, **kwargs):
            kwargs.setdefault('verify', False)
            return original_request(self, method, url, **kwargs)
        requests.Session.request = new_request

        sh = gc.open_by_key(SPREADSHEET_ID)

        print("Preparing 'Analysis' tab...")
        try:
            ws_analysis = sh.worksheet("Analysis")
            ws_analysis.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws_analysis = sh.add_worksheet(title="Analysis", rows="1500", cols="20")

        curr_row = 1
        
        def upload_table(ws, title, df, start_row):
            ws.update(range_name=f"A{start_row}", values=[[title]])
            cols = [df.columns.values.tolist()]
            # Clean formatting for sheets
            def formatter(x):
                if isinstance(x, float):
                    if x > 1.0: return round(x, 0) # Totals or base counts
                    return round(x, 4)
                return x
            vals = df.astype(object).fillna("").map(formatter).values.tolist()
            ws.update(range_name=f"A{start_row+1}", values=cols + vals, value_input_option='USER_ENTERED')
            return start_row + len(df) + 4

        # 0. EXECUTIVE L1 SUMMARY (MATCHING DOC NARRATIVE)
        print("Generating Executive L1 Summary...")
        categories_order = list(PERIODS.keys())
        l1_summary_data = []
        for period in categories_order:
            p_df = final_df[final_df['Super Category'] == period]
            total = len(p_df)
            if total == 0: continue
            
            # 1. Overall Connectivity (Healthy + Intermittent + Low)
            conn_count = len(p_df[p_df['Status'].str.startswith('1.')])
            # 2. > 7 Day Disconnections (Bracket 1)
            disc_count = len(p_df[p_df['Status'].str.startswith('3.')])
            # 3. Never Connected
            never_count = len(p_df[p_df['Status'].str.startswith('5.') | p_df['Status'].str.startswith('6.')])
            # 4. Other Statuses (2., 4., 7.)
            other_count = len(p_df[p_df['Status'].str.startswith('2.') | p_df['Status'].str.startswith('4.') | p_df['Status'].str.startswith('7.')])
            
            l1_summary_data.append({"Metric": "1. Overall Connectivity %", "Period": period, "Value": (conn_count / total)})
            l1_summary_data.append({"Metric": "2. > 7 Day Concern %", "Period": period, "Value": (disc_count / total)})
            l1_summary_data.append({"Metric": "3. Never Connected %", "Period": period, "Value": (never_count / total)})
            l1_summary_data.append({"Metric": "4. Other / Long-term Disconnect %", "Period": period, "Value": (other_count / total)})
            l1_summary_data.append({"Metric": "--- TOTAL ---", "Period": period, "Value": (conn_count + disc_count + never_count + other_count) / total})
            l1_summary_data.append({"Metric": "Total Batteries (Base)", "Period": period, "Value": total})

        # Pivot to wider format for side-by-side view
        if l1_summary_data:
            l1_df = pd.DataFrame(l1_summary_data).pivot(index='Metric', columns='Period', values='Value').reset_index()
            l1_df = l1_df.reindex(columns=['Metric'] + categories_order)
            curr_row = upload_table(ws_analysis, "--- EXECUTIVE L1 SUMMARY (FOR WEEKLY DOC) ---", l1_df, curr_row)

        # 1. CONSOLIDATED STATUS SUMMARY (%)
        print("Generating Consolidated Status Summary (Percentages)...")
        summary_percent = pd.crosstab(final_df['Status'], final_df['Super Category'])
        for col in summary_percent.columns:
            total_col = summary_percent[col].sum()
            if total_col > 0:
                summary_percent[col] = (summary_percent[col] / total_col)
        summary_percent.loc['TOTAL'] = summary_percent.sum()
        summary_percent = summary_percent.reindex(columns=categories_order, fill_value=0).reset_index()
        curr_row = upload_table(ws_analysis, "--- CONSOLIDATED STATUS SUMMARY (%) ---", summary_percent, curr_row)

        # 2. GAP ANALYSIS: IOT HUB BUT NOT IN CRM
        if not final_gap_df.empty:
            print("Generating Gap Analysis tables...")
            gap_summary_country = final_gap_df.pivot_table(index='country', columns='Super Category', values='missing_oems', aggfunc='sum')
            gap_summary_country = gap_summary_country.reindex(columns=categories_order, fill_value=0).reset_index()
            curr_row = upload_table(ws_analysis, "--- GAP BY COUNTRY: IOT HUB BUT NOT IN CRM ---", gap_summary_country, curr_row)

            gap_summary_vendor = final_gap_df.pivot_table(index='oem_prefix', columns='Super Category', values='missing_oems', aggfunc='sum')
            gap_summary_vendor = gap_summary_vendor.reindex(columns=categories_order, fill_value=0).reset_index()
            curr_row = upload_table(ws_analysis, "--- GAP BY VENDOR: IOT HUB BUT NOT IN CRM ---", gap_summary_vendor, curr_row)

        # 3. CONSOLIDATED VENDOR BREAKUP (COUNTS & %)
        print("Generating Consolidated Vendor Breakup...")
        vendor_parts_count = []
        vendor_parts_pct = []
        for period in categories_order:
            p_df = final_df[final_df['Super Category'] == period]
            # Counts
            ct = pd.crosstab(p_df['Status'], p_df['battery_family'])
            ct.columns = [f"{c} ({period[:3]})" for c in ct.columns]
            vendor_parts_count.append(ct)
            # % (Within each vendor's total for that period)
            pct = pd.crosstab(p_df['Status'], p_df['battery_family'])
            pct = pct.div(pct.sum(axis=0), axis=1) # Normalise columns
            pct.loc['TOTAL'] = pct.sum()
            pct.columns = [f"{c} % ({period[:3]})" for c in pct.columns]
            vendor_parts_pct.append(pct)
            
        summary_vendor_count = pd.concat(vendor_parts_count, axis=1).fillna(0).reset_index()
        curr_row = upload_table(ws_analysis, "--- VENDOR BREAKUP (CONSOLIDATED COUNTS) ---", summary_vendor_count, curr_row)

        summary_vendor_pct = pd.concat(vendor_parts_pct, axis=1).fillna(0).reset_index()
        curr_row = upload_table(ws_analysis, "--- VENDOR BREAKUP (CONSOLIDATED %) ---", summary_vendor_pct, curr_row)

        # 4. CONSOLIDATED COUNTRY BREAKUP (COUNTS & %)
        print("Generating Consolidated Country Breakup (COUNTS & %)...")
        country_parts_count = []
        country_parts_pct = []
        for period in categories_order:
            p_df = final_df[final_df['Super Category'] == period]
            # Counts
            ct = pd.crosstab(p_df['Status'], p_df['country_code'])
            ct.columns = [f"{c} ({period[:3]})" for c in ct.columns]
            country_parts_count.append(ct)
            # %
            pct = pd.crosstab(p_df['Status'], p_df['country_code'])
            pct = pct.div(pct.sum(axis=0), axis=1)
            pct.loc['TOTAL'] = pct.sum()
            pct.columns = [f"{c} % ({period[:3]})" for c in pct.columns]
            country_parts_pct.append(pct)
            
        summary_country_count = pd.concat(country_parts_count, axis=1).fillna(0).reset_index()
        curr_row = upload_table(ws_analysis, "--- COUNTRY BREAKUP (CONSOLIDATED COUNTS) ---", summary_country_count, curr_row)

        summary_country_pct = pd.concat(country_parts_pct, axis=1).fillna(0).reset_index()
        curr_row = upload_table(ws_analysis, "--- COUNTRY BREAKUP (CONSOLIDATED %) ---", summary_country_pct, curr_row)

        # 5. ACTIVE CIRCULATION SUMMARY (VENDOR & COUNTRY - COUNTS & %)
        print("Generating Disconnection Reason Analysis for Circulation Batteries...")
        circ_df = final_df[final_df['circulation_batteries'] > 0].copy()
        if not circ_df.empty:
            circ_v_count = []
            circ_v_pct = []
            circ_c_count = []
            circ_c_pct = []
            
            for period in categories_order:
                p_circ = circ_df[circ_df['Super Category'] == period]
                # Vendor Counts & %
                v_ct = pd.crosstab(p_circ['Status'], p_circ['battery_family'])
                v_ct.columns = [f"{c} ({period[:3]})" for c in v_ct.columns]
                circ_v_count.append(v_ct)
                
                v_pct = pd.crosstab(p_circ['Status'], p_circ['battery_family'])
                v_pct = v_pct.div(v_pct.sum(axis=0), axis=1)
                v_pct.columns = [f"{c} % ({period[:3]})" for c in v_pct.columns]
                circ_v_pct.append(v_pct)

                # Country Counts & %
                c_ct = pd.crosstab(p_circ['Status'], p_circ['country_code'])
                c_ct.columns = [f"{c} ({period[:3]})" for c in c_ct.columns]
                circ_c_count.append(c_ct)
                
                c_pct = pd.crosstab(p_circ['Status'], p_circ['country_code'])
                c_pct = c_pct.div(c_pct.sum(axis=0), axis=1)
                c_pct.columns = [f"{c} % ({period[:3]})" for c in c_pct.columns]
                circ_c_pct.append(c_pct)

            summary_circ_vendor = pd.concat(circ_v_count, axis=1).fillna(0).reset_index()
            curr_row = upload_table(ws_analysis, "--- VENDOR BREAKUP (CIRCULATION BATTERIES ONLY - COUNTS) ---", summary_circ_vendor, curr_row)
            
            summary_circ_vendor_pct = pd.concat(circ_v_pct, axis=1).fillna(0).reset_index()
            curr_row = upload_table(ws_analysis, "--- VENDOR BREAKUP (CIRCULATION BATTERIES ONLY - %) ---", summary_circ_vendor_pct, curr_row)

            summary_circ_country = pd.concat(circ_c_count, axis=1).fillna(0).reset_index()
            curr_row = upload_table(ws_analysis, "--- COUNTRY BREAKUP (CIRCULATION BATTERIES ONLY - COUNTS) ---", summary_circ_country, curr_row)
            
            summary_circ_country_pct = pd.concat(circ_c_pct, axis=1).fillna(0).reset_index()
            curr_row = upload_table(ws_analysis, "--- COUNTRY BREAKUP (CIRCULATION BATTERIES ONLY - %) ---", summary_circ_country_pct, curr_row)

        # 6. COUNTRY X VENDOR (Matrix-wise)
        curr_row += 2
        for period in categories_order:
            p_df = final_df[final_df['Super Category'] == period]
            if not p_df.empty:
                cv_summary = pd.crosstab(p_df['country_code'], p_df['battery_family']).reset_index()
                curr_row = upload_table(ws_analysis, f"Country x Vendor Matrix - {period}", cv_summary, curr_row)

        print(f"\n✅ All update-ready tables updated in 'Analysis' tab at: {sh.url}")

    except Exception as e:
        print(f"FAILED to upload to Sheets: {e}")
        import traceback
        traceback.print_exc()
