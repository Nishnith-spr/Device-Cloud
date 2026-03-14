import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import ssl
import requests
import urllib3

# Handle SSL certificates for corporate environments/macOS
try:
    # Suppress insecure request warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Monkeypatch requests to disable SSL verification globally in this script
    original_request = requests.Session.request
    def new_request(self, method, url, **kwargs):
        kwargs.setdefault('verify', False)
        return original_request(self, method, url, **kwargs)
    requests.Session.request = new_request
except Exception:
    pass

def get_gsheet_data(spreadsheet_id, sheet_name, credentials_path=None, read_formulas=False):
    """
    Reads data from a Google Sheet and returns it as a pandas DataFrame.
    
    Args:
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        sheet_name (str): The name of the worksheet to read.
        credentials_path (str, optional): Path to the service account credentials JSON.
        read_formulas (bool): If True, reads cell formulas instead of calculated values.
        
    Returns:
        pd.DataFrame: The data from the Google Sheet.
    """
    if credentials_path is None:
        # Try to find credentials in common locations
        possible_paths = [
            "credentials.json",
            "../Ameyo Metrics/credentials.json",
            os.path.expanduser("~/Desktop/Codes/Ameyo Metrics/credentials.json")
        ]
        for p in possible_paths:
            if os.path.exists(p):
                credentials_path = p
                break
    
    if not credentials_path or not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found at {credentials_path}. Please provide a valid path.")

    # Define scopes
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    
    # Authenticate
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    client = gspread.authorize(creds)
    
    # Open spreadsheet and worksheet
    sh = client.open_by_key(spreadsheet_id)
    worksheet = sh.worksheet(sheet_name)
    
    # Get all values
    # If read_formulas is True, use 'FORMULA' option
    render_option = 'FORMULA' if read_formulas else 'FORMATTED_VALUE'
    
    data = worksheet.get_all_values(value_render_option=render_option)
    
    if not data:
        return pd.DataFrame()
        
    # Convert to DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

if __name__ == "__main__":
    # ==========================================
    # ==========================================
    # ==========================================
    SPREADSHEET_ID = "1xvLIfYKhE0JY0_p4Q57aACdpdjHdHtdvzVhdlbF7p-E"
    
    try:
        print("Loading raw data from Google Sheets...")
        df_raw = get_gsheet_data(SPREADSHEET_ID, "Last connected with packet received - raw data")
        
        # Filter for Target Weeks
        TARGET_WEEKS = ["week8", "week9"]
        df = df_raw[df_raw['week'].isin(TARGET_WEEKS)].copy()
        
        # Convert numeric columns for logical comparisons
        cols_to_fix = ['days_from_last_connected', 'days_from_last_connection_attempt', 'Hourly connectivity', 'Potential DD', 'swaps']
        for col in cols_to_fix:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert flags to boolean
        df['flag'] = df['flag'].astype(str).str.upper() == 'TRUE'

        def categorize(row):
            d_con = row['days_from_last_connected'] # Last Data Packet
            d_att = row['days_from_last_connection_attempt'] # Last Server Connection
            h_con = row['Hourly connectivity']
            p_dd = row['Potential DD']
            swaps = row['swaps']
            flag = row['flag']

            # 1. Connected within a week
            if pd.notnull(d_con) and pd.notnull(d_att) and d_con <= 7 and d_att <= 7:
                wk_label = row['week']
                if h_con >= 0.75: return f"Connected within a week ({wk_label}) - Healthy Connection"
                if h_con >= 0.3:  return f"Connected within a week ({wk_label}) - Intermittent Connection"
                return f"Connected within a week ({wk_label}) - Low connection"
            
            # 2. Connected to Server but no packets sent
            if flag:
                return "Connected to Server but no packets sent"
            
            # 3. Not connected for > 7 days (7-30)
            if pd.notnull(d_con) and pd.notnull(d_att) and 7 < d_con <= 30 and d_att > 7:
                if p_dd <= 0: return "Not connected for > 7 days - Potential Deep Discharge"
                if swaps > 5: return "Not connected for > 7 days - Actively Swapping"
                return "Not connected for > 7 days - Not Swapping"
            
            # 4. Not connected for > 30 days
            if pd.notnull(d_con) and pd.notnull(d_att) and d_con > 30 and d_att > 30:
                if p_dd <= 0: return "Not connected for > 30 days - Potential Deep Discharge"
                if swaps > 5: return "Not connected for > 30 days - Actively Swapping"
                return "Not connected for > 30 days - Not Swapping"
            
            # 5. Never connected to Server but packets sent ($D<>"", $F="")
            if pd.notnull(d_con) and pd.isnull(d_att):
                if swaps > 0: return "Never connected - Server but packets sent - Swapped atleast once"
                return "Never connected - Server but packets sent - Never Swapped"
            
            # 6. Never connected to server & never sent a packet ($D="", $F="")
            if pd.isnull(d_con) and pd.isnull(d_att):
                if swaps > 0: return "Never connected - server & packet - Swapped atleast once"
                return "Never connected - server & packet - Never Swapped"
            
            return "Other / Uncategorized"

        df['Category'] = df.apply(categorize, axis=1)
        
        print(f"\n--- Categorization Results for {', '.join(TARGET_WEEKS)} ---")
        summary = df['Category'].value_counts().sort_index()
        for cat, count in summary.items():
            print(f"{cat:<70}: {count:,}")
        
        print("-" * 80)
        print(f"{'Total Records Processing':<70}: {len(df):,}")
        print(f"{'Total Categorized':<70}: {summary.sum():,}")

        # Save to CSV
        output_file = f"Categorized_Raw_Data_MultipleWeeks.csv"
        df.to_csv(output_file, index=False)
        print(f"\nSuccessfully generated: {os.path.abspath(output_file)}")
        
    except Exception as e:
        print(f"Error: {e}")