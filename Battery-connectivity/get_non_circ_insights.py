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

def get_deep_insights():
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

    non_v = extract_table("VENDOR BREAKUP (NON-CIRCULATING ONLY - %)")
    non_c = extract_table("COUNTRY BREAKUP (NON-CIRCULATING ONLY - %)")

    print("\n--- NON-CIRCULATING INSIGHTS DATA ---")
    if non_v is not None: print("\nNon-Circulating Vendor (%):\n", non_v.to_string())
    if non_c is not None: print("\nNon-Circulating Country (%):\n", non_c.to_string())

if __name__ == "__main__":
    get_deep_insights()
