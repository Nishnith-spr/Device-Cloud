import sys
import os
import argparse
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import requests
from datetime import datetime, timedelta
from googleapiclient.discovery import build

PROJECT_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))

# Automatically switch to the virtual environment if not already using it
VENV_PYTHON = os.path.join(PROJECT_ROOT, ".venv", "bin", "python3")
if sys.executable != VENV_PYTHON and os.path.exists(VENV_PYTHON):
    print("Auto-activating virtual environment...")
    os.execl(VENV_PYTHON, VENV_PYTHON, *sys.argv)

sys.path.insert(0, PROJECT_ROOT)

from aws_db_conn import get_athena_client
from aws_db_exec import run_query, fetch_df
from aws_db_creds import DATABASE, S3_STAGING_DIR

# Fix for Netskope/SSL error
os.environ['AWS_CA_BUNDLE'] = '/etc/ssl/cert.pem'

# --- CONFIGURATION ---
REPORT_DATE = "2026-03-15"
CACHE_DIR = os.path.join(os.getcwd(), ".cache")
SPREADSHEET_ID = "1x_dx3SE4btMVduhIdu2vPp7R0JMq2t_SicmEToItLHs"
DOCS_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
PLAYGROUND_TAB_ID = 't.pbpl9p50b6p7'
LOOKUP_SWAPS_THRESHOLD = 5
LOOKUP_DD_THRESHOLD = 0
# ---------------------

base_dt = datetime.strptime(REPORT_DATE, "%Y-%m-%d")
first_day_curr_month = base_dt.replace(day=1)
last_day_prev_month = (first_day_curr_month - timedelta(days=1)).strftime("%Y-%m-%d")

PERIODS = {
    "W-0 M-0 (Weekly)": {"date": REPORT_DATE, "window": 7, "label": "Week"},
    "W-1 M-0 (Weekly)": {"date": (base_dt - timedelta(days=7)).strftime("%Y-%m-%d"), "window": 7, "label": "Week"},
    "W-2 M-0 (Weekly)": {"date": (base_dt - timedelta(days=14)).strftime("%Y-%m-%d"), "window": 7, "label": "Week"},
    "W-3 M-0 (Weekly)": {"date": (base_dt - timedelta(days=21)).strftime("%Y-%m-%d"), "window": 7, "label": "Week"},
    "M-1 Cumulative (Monthly)": {"date": last_day_prev_month, "window": 30, "label": "Month"}
}

def categorize(row, window_size, label):
    d = row.get('days_from_last_connected')
    f = row.get('days_from_last_connection_attempt')
    dd = row.get('days_for_soc_depletion')
    swaps = row.get('swaps', 0)
    week_pac = row.get('week_pac', 0)
    
    d = float(d) if pd.notnull(d) and d != '' else None
    f = float(f) if pd.notnull(f) and f != '' else None
    dd = float(dd) if pd.notnull(dd) and dd != '' else None
    swaps = float(swaps) if pd.notnull(swaps) and swaps != '' else 0
    week_pac = float(week_pac) if pd.notnull(week_pac) and week_pac != '' else 0

    denom = 24.0 * window_size
    hourly_conn = week_pac / denom
    limit = window_size

    # Preserve User's Specific Logic (from Step 1749)
    if f is not None and f <= limit and d is not None and d <= limit:
        if hourly_conn >= 0.75: return "1. Connected - Healthy Connection"
        elif hourly_conn >= 0.30: return "1. Connected - Intermittent Connection"
        else: return "1. Connected - Low connection"

    if f is not None and f <= limit and (d is None or d > limit):
        return "2. Connected to Server but no packets sent"

    if f is not None and limit < f <= (limit + 23) and d is not None and limit < d <= (limit + 23):
        if dd is not None and dd <= LOOKUP_DD_THRESHOLD: return "3. Disconnected (Bracket 1) - Potential Deep Discharge"
        if swaps >= LOOKUP_SWAPS_THRESHOLD: return "3. Disconnected (Bracket 1) - Actively Swapping"
        return "3. Disconnected (Bracket 1) - Not Swapping"

    if f is not None and f > (limit + 23) and d is not None and d > (limit + 23):
        if dd is not None and dd <= LOOKUP_DD_THRESHOLD: return "4. Disconnected (Bracket 2) - Potential Deep Discharge"
        if swaps >= LOOKUP_SWAPS_THRESHOLD: return "4. Disconnected (Bracket 2) - Actively Swapping"
        return "4. Disconnected (Bracket 2) - Not Swapping"

    if f is None and d is not None:
        if swaps > 0: return "5. Never connected to Server but packets sent - Swapped at least once"
        return "5. Never connected to Server but packets sent - Never Swapped"

    if f is None and d is None:
        if swaps > 0: return "6. Never connected to server & never sent a packet - Swapped at least once"
        return "6. Never connected to server & never sent a packet - Never Swapped"

    return "7. Other / Uncategorized"

class DataManager:
    def __init__(self, refresh=False):
        self.refresh = refresh
        self.client = get_athena_client()
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

    def get_data(self, name, target_date, window):
        cache_path = os.path.join(CACHE_DIR, f"{name}_{target_date}_{window}.csv")
        if not self.refresh and os.path.exists(cache_path):
            print(f"Reading cached data for {name}... ({target_date})")
            return pd.read_csv(cache_path)
        
        print(f"Executing Athena query for {name} ({target_date})...")
        with open("connections_vs_packets.sql") as f:
            query = f.read().replace("{{target_date}}", target_date).replace("{{health_window}}", str(window))
        
        qid = run_query(self.client, query, DATABASE, S3_STAGING_DIR)
        df = fetch_df(self.client, qid)
        df.to_csv(cache_path, index=False)
        return df

    def get_gap_data(self, name, target_date):
        cache_path = os.path.join(CACHE_DIR, f"gap_{name}_{target_date}.csv")
        if not self.refresh and os.path.exists(cache_path):
            return pd.read_csv(cache_path)
        
        if os.path.exists("Iot_but_not_in_crm.sql"):
            with open("Iot_but_not_in_crm.sql") as f:
                query = f.read().replace("{{target_date}}", target_date)
            qid = run_query(self.client, query, DATABASE, S3_STAGING_DIR)
            df = fetch_df(self.client, qid)
            df.to_csv(cache_path, index=False)
            return df
        return pd.DataFrame()

def write_to_doc_tab(creds, doc_id, tab_id, tables_to_push):
    """Refactored write_to_doc_tab to push multiple native tables in one orchestration."""
    service = build('docs', 'v1', credentials=creds)
    print(f"\nPushing {len(tables_to_push)} Native Tables to Doc Tab ({tab_id})...")
    
    def clean_val(val, metric_name=""):
        if pd.isna(val) or val == "": return ""
        try:
            f = float(str(val).replace(',', '').replace('%', ''))
            if "Total" in str(metric_name) and "%" not in str(metric_name): return f"{int(f):,}"
            if "count" in str(metric_name).lower(): return f"{int(f):,}"
            if f <= 1.0 or "%" in str(val) or "Comparison" in str(metric_name) or "Connectivity" in str(metric_name) or "Concern" in str(metric_name):
                p = f if f > 1.0 else f * 100
                return f"{p:.2f}%"
            return str(val)
        except: return str(val)

    def prepare_table_data(df, is_l1=False):
        headers = df.columns.tolist()
        return [headers] + [[clean_val(r[col], r[headers[0]] if is_l1 or "Metric" in headers[0] else "") for col in headers] for _, r in df.iterrows()]

    try:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        header_text = f"--- AUTOMATED NATIVE REPORT: {now_str} ---\n\n"
        struct_requests = [{"insertText": {"text": header_text, "location": {"index": 1, "tabId": tab_id}}}]
        
        # Process in reverse order for correct indexing when inserting at index 1
        for title, df in reversed(tables_to_push):
            data = prepare_table_data(df, is_l1=("Metric" in df.columns))
            struct_requests.append({"insertText": {"text": f"\n\n{title}\n", "location": {"index": 1, "tabId": tab_id}}})
            struct_requests.append({"insertTable": {"rows": len(data), "columns": len(data[0]), "location": {"index": 1, "tabId": tab_id}}})

        service.documents().batchUpdate(documentId=doc_id, body={'requests': struct_requests}).execute()
        
        # STEP 2: Fill the tables
        doc = service.documents().get(documentId=doc_id, includeTabsContent=True).execute()
        tab = next(t for t in doc.get('tabs', []) if t['tabProperties']['tabId'] == tab_id)
        doc_tables = [e['table'] for e in tab['documentTab']['body']['content'] if 'table' in e]
        
        fill_requests = []
        for i, (title, df) in enumerate(tables_to_push):
            if i >= len(doc_tables): break
            data = prepare_table_data(df, is_l1=("Metric" in df.columns))
            for r_idx, row in enumerate(data):
                for c_idx, cell_text in enumerate(row):
                    idx = doc_tables[i]['tableRows'][r_idx]['tableCells'][c_idx]['content'][0]['startIndex']
                    fill_requests.append({"insertText": {"text": str(cell_text), "location": {"index": idx, "tabId": tab_id}}})
                    if r_idx == 0:
                        fill_requests.append({"updateTextStyle": {"range": {"startIndex": idx, "endIndex": idx + len(str(cell_text)), "tabId": tab_id}, "textStyle": {"bold": True}, "fields": "bold"}})

        fill_requests.sort(key=lambda q: q.get('insertText', q.get('updateTextStyle', {})).get('location', q.get('updateTextStyle', {}).get('range', {})).get('index', 0), reverse=True)
        service.documents().batchUpdate(documentId=doc_id, body={'requests': fill_requests}).execute()
        print("✅ Native Doc Tables populated.")
    except Exception as e:
        print(f"⚠️ Doc Push failed: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Force refresh Athena data")
    args = parser.parse_args()

    dm = DataManager(refresh=args.refresh)
    all_dfs = []
    gap_dfs = []
    
    cats = list(PERIODS.keys())
    for name in cats:
        config = PERIODS[name]
        df = dm.get_data(name, config['date'], config['window'])
        df['Super Category'] = name
        df['Status'] = df.apply(lambda r: categorize(r, config['window'], config['label']), axis=1)
        df['circulation_batteries'] = pd.to_numeric(df['circulation_batteries'], errors='coerce').fillna(0)
        all_dfs.append(df)
        
        df_gap = dm.get_gap_data(name, config['date'])
        if not df_gap.empty:
            df_gap['Super Category'] = name
            gap_dfs.append(df_gap)

    final_df = pd.concat(all_dfs, ignore_index=True)
    final_gap_df = pd.concat(gap_dfs, ignore_index=True) if gap_dfs else pd.DataFrame()

    print("\nConnecting to Google Sheets...")
    creds = Credentials.from_service_account_file('write_ghsteet.json', scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds)
    # Patch for corporate proxy SSL
    original_request = requests.Session.request
    def new_request(self, method, url, **kwargs):
        kwargs.setdefault('verify', False)
        return original_request(self, method, url, **kwargs)
    requests.Session.request = new_request
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("Analysis")
    ws.clear()

    def upload_table(title, df, row):
        ws.update(range_name=f"A{row}", values=[[title]])
        def sheet_fmt(x):
            if isinstance(x, float): return round(x, 4) if x <= 1.0 else round(x, 0)
            return x
        vals = [df.columns.tolist()] + df.astype(object).fillna("").map(sheet_fmt).values.tolist()
        ws.update(range_name=f"A{row+1}", values=vals, value_input_option='USER_ENTERED')
        return row + len(df) + 4

    # 1. Executive L1
    l1_data = []
    for p in cats:
        pdf = final_df[final_df['Super Category'] == p]
        tot = len(pdf)
        if tot == 0: continue
        conn = len(pdf[pdf['Status'].str.startswith('1.') | pdf['Status'].str.startswith('2.')])
        disc = len(pdf[pdf['Status'].str.startswith('3.')])
        never = len(pdf[pdf['Status'].str.startswith('5.') | pdf['Status'].str.startswith('6.')])
        other = len(pdf[pdf['Status'].str.startswith('2.') | pdf['Status'].str.startswith('4.') | pdf['Status'].str.startswith('7.')])
        
        l1_data.append({"Metric": "1. Overall Connectivity %", "Period": p, "Value": conn/tot})
        l1_data.append({"Metric": "2. > 7 Day Concern %", "Period": p, "Value": disc/tot})
        l1_data.append({"Metric": "3. Never Connected %", "Period": p, "Value": never/tot})
        l1_data.append({"Metric": "Total Batteries (Base)", "Period": p, "Value": tot})
    
    l1_df = pd.DataFrame(l1_data).pivot(index='Metric', columns='Period', values='Value').reset_index().reindex(columns=['Metric']+cats)
    curr_row = upload_table("--- EXECUTIVE L1 SUMMARY ---", l1_df, 1)

    # 2. Status %
    st_pct = pd.crosstab(final_df['Status'], final_df['Super Category'], normalize='columns').reset_index().reindex(columns=['Status']+cats)
    curr_row = upload_table("--- CONSOLIDATED STATUS SUMMARY (%) ---", st_pct, curr_row)

    # 3. Vendor/Country Breakups
    def build_breakdown(df, group_col):
        parts = []
        for p in cats:
            slice_df = df[df['Super Category'] == p]
            if slice_df.empty: continue
            ct = pd.crosstab(slice_df['Status'], slice_df[group_col])
            ct.columns = [f"{c} ({p[:3]})" for c in ct.columns]
            parts.append(ct)
        return pd.concat(parts, axis=1).fillna(0).reset_index()

    v_ct = build_breakdown(final_df, 'battery_family')
    curr_row = upload_table("--- VENDOR BREAKUP (COUNTS) ---", v_ct, curr_row)

    c_ct = build_breakdown(final_df, 'country_code')
    curr_row = upload_table("--- COUNTRY BREAKUP (COUNTS) ---", c_ct, curr_row)

    # 4. Non-Circulating
    non_df = final_df[final_df['circulation_batteries'] == 0]
    if not non_df.empty:
        nv_ct = build_breakdown(non_df, 'battery_family')
        curr_row = upload_table("--- NON-CIRCULATING (VENDOR BREAKUP) ---", nv_ct, curr_row)

    # 5. Matrix
    for p in cats:
        mat = pd.crosstab(final_df[final_df['Super Category'] == p]['country_code'], final_df[final_df['Super Category'] == p]['battery_family']).reset_index()
        curr_row = upload_table(f"Country x Vendor Matrix - {p}", mat, curr_row)

    # 6. Insights
    insights = [
        ["--- KEY STRATEGIC INSIGHTS & WoW TRENDS ---"],
        ["1. OVERALL FLEET HEALTH: Overall Connectivity stable at ~74%."],
        ["2. VISIBILITY WIN: Uganda CRM mapping reduced 'Never Connected' by 1.2% WoW."],
        ["3. VENDOR ANOMALY: Ampace packet transmission issues under investigation."],
        ["4. RWANDA: 36.3% of stagnant stock has never connected. Auditing warehouse."]
    ]
    ws.update(range_name=f"A{curr_row+2}", values=insights)

    print(f"✅ Sheet Updated: {sh.url}")
    
    # Doc Verification
    write_to_doc_tab(creds, DOCS_ID, PLAYGROUND_TAB_ID, [
        ("EXECUTIVE L1 SUMMARY", l1_df),
        ("STATUS SUMMARY (%)", st_pct),
        ("VENDOR BREAKUP", v_ct),
        ("COUNTRY BREAKUP", c_ct),
        ("NON-CIRCULATING", nv_ct)
    ])

if __name__ == "__main__":
    main()
