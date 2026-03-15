import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Robust SSL bypass
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

try:
    ws = sh.worksheet("Analysis")
    data = ws.get_all_values()
    df = pd.DataFrame(data)
    print("--- FIRST 40 ROWS OF ANALYSIS TAB ---")
    print(df.head(40).to_string(index=False, header=False))
except Exception as e:
    print(f"Error: {e}")
