import gspread
from google.oauth2.service_account import Credentials
import os
import urllib3
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
original_request = requests.Session.request
def new_request(self, method, url, **kwargs):
    kwargs.setdefault('verify', False)
    return original_request(self, method, url, **kwargs)
requests.Session.request = new_request

# Use the reference sheet ID
SPREADSHEET_ID = "1xvLIfYKhE0JY0_p4Q57aACdpdjHdHtdvzVhdlbF7p-E"

scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('write_ghsteet.json', scopes=scopes)
gc = gspread.authorize(creds)

sh = gc.open_by_key(SPREADSHEET_ID)

# Check "Vendor Breakup" or "Connectivity consolidated" or "Report"
# Let's try to find an "Active" or "Circulation" row/column
for tab in ["Report", "Connectivity consolidated"]:
    try:
        ws = sh.worksheet(tab)
        print(f"\n--- {tab} Sample ---")
        data = ws.get('A1:Z50', value_render_option='FORMULA')
        for i, row in enumerate(data):
            if any("ACTIVE" in str(cell).upper() or "CIRCULATION" in str(cell).upper() for cell in row):
                print(f"Row {i+1}: {row}")
    except:
        pass
