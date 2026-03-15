import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
original_request = requests.Session.request
def new_request(self, method, url, **kwargs):
    kwargs.setdefault('verify', False)
    return original_request(self, method, url, **kwargs)
requests.Session.request = new_request

SPREADSHEET_ID = "1x_dx3SE4btMVduhIdu2vPp7R0JMq2t_SicmEToItLHs"
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('write_ghsteet.json', scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet("Analysis")
data = ws.get_all_values()

# Find the "CONSOLIDATED STATUS SUMMARY (%)" table
start_idx = -1
for i, row in enumerate(data):
    if "--- CONSOLIDATED STATUS SUMMARY (%) ---" in str(row):
        start_idx = i + 1
        break

if start_idx != -1:
    header = data[start_idx]
    rows = []
    for i in range(start_idx + 1, len(data)):
        if not data[i] or data[i][0] == "" or "---" in data[i][0]:
            break
        rows.append(data[i])
    
    df = pd.DataFrame(rows, columns=header)
    print("\n--- Percentages in Sheet ---")
    print(df.to_string())
    
    for col in header[1:]:
        if col:
            # Clean percentage and convert to float
            vals = df[col].str.replace('%', '').str.strip()
            # If it's a decimal like 0.6360, convert directly
            # If it was formatted as %, it might be 63.60
            float_vals = []
            for v in vals:
                try:
                    if v == "": float_vals.append(0.0)
                    else: float_vals.append(float(v))
                except: float_vals.append(0.0)
            
            s = sum(float_vals)
            print(f"Sum of column '{col}': {s}")
else:
    print("Table not found")
