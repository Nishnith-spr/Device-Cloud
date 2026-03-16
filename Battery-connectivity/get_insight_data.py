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
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'

def get_insights():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("Analysis")
    data = ws.get_all_values()

    def extract_table(marker):
        start = -1
        for i, row in enumerate(data):
            if marker in str(row):
                start = i + 1
                break
        if start == -1: return None
        header = data[start]
        rows = []
        for i in range(start + 1, len(data)):
            if not data[i] or data[i][0] == "" or "---" in data[i][0]: break
            rows.append(data[i])
        return pd.DataFrame(rows, columns=header)

    l1 = extract_table("EXECUTIVE L1 SUMMARY")
    status = extract_table("CONSOLIDATED STATUS SUMMARY")
    gap_country = extract_table("GAP BY COUNTRY")
    vendor_pct = extract_table("VENDOR BREAKUP (CONSOLIDATED %)")

    print("\n--- RAW DATA FOR ANALYSIS ---")
    if l1 is not None: print("\nL1 Summary:\n", l1.to_string())
    if status is not None: print("\nStatus Breakdown:\n", status.to_string())
    if gap_country is not None: print("\nGap by Country:\n", gap_country.to_string())
    if vendor_pct is not None: print("\nVendor Breakdown (%):\n", vendor_pct.to_string())

if __name__ == "__main__":
    get_insights()
