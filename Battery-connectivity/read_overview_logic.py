import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json

# Fix for SSL
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
for tab_name in ["Vendor Breakup", "Country X Vendor", "Country Breakup"]:
    print(f"\n--- {tab_name} ---")
    ws = sh.worksheet(tab_name)
    data = ws.get('A1:J20')
    for i, row in enumerate(data):
        print(f"Row {i+1}: {row}")
