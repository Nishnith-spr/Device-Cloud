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

def push_latest_insights():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("Analysis")
    
    # Find the bottom
    all_vals = ws.get_all_values()
    start_row = len(all_vals) + 2
    
    insights = [
        ["--- KEY STRATEGIC INSIGHTS & WoW TRENDS ---"],
        ["1. OVERALL FLEET HEALTH: Overall Connectivity rose from 73.18% to 74.02% over 2 weeks (+0.84%)."],
        ["2. VISIBILITY WIN: The 'Never Connected' bracket shrank by 1.20% WoW, driven by successful CRM mapping in Uganda."],
        ["3. VENDOR ANOMALY (AMPACE): Ampace Healthy connections dropped from 59.7% -> 53.6%."],
        ["   Hypothesis: Rise in 'Connected to Server but no packets sent' suggests a BMS firmware handshake issue during use."],
        ["4. COUNTRY SPOTLIGHT (UGANDA): Uganda CRM gap crashed from 591 to 106 batteries (massive operational cleanup)."],
        ["5. COUNTRY SPOTLIGHT (KENYA): Seeing a rise in idle capacity; 45% of stagnant stock is healthy but not being swapped."],
        [""],
        ["--- NON-CIRCULATING BATTERY ANALYSIS (STAGNANT FLEET) ---"],
        ["1. RWANDA 'GHOST STOCK': 36.3% of stagnant stock has never connected or sent a packet. Auditing physical warehouse vs CRM required."],
        ["2. GREENWAY-1 DEFECTS: 31.4% of stagnant Greenway-1 units are silent disconnections (Bracket 2). High probability of hardware failure."],
        ["3. UNIQUE FAMILY DARK POOL: 30.2% of non-circulating Unique units have never connected. Confirms Unique as the primary stagnant stock."],
        ["4. UGANDA DISCONNECTS: 18.2% of stagnant Uganda stock are in Status 4 (dropped off just after use). Potential lost/defective field units."],
        [""],
        ["--- STRATEGIC RECOMMENDATIONS FOR MAR 17th ---"],
        ["- Deep dive into Ampace packet transmission handshake (handheld field check)."],
        ["- Redeploy Kenyan 'Idle Spares' to higher-demand stations to improve ROI."],
        ["- Hardware recall evaluation for Greenway-1 units in Status 4."],
        ["- Finalize Cameroon metadata cleanup before next week's automated sync."]
    ]
    
    ws.update(range_name=f"A{start_row}", values=insights)
    print(f"✅ Strategic Insights pushed to Analysis tab at row {start_row}")

if __name__ == "__main__":
    push_latest_insights()
