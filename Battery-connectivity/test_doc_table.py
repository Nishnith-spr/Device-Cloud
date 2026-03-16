from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'
TAB_ID = 't.pbpl9p50b6p7'

def test_insert_table():
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Testing real Table insertion in tab {TAB_ID}...")
    
    # 1. Insert a 3x3 table at the start of the tab
    requests = [
        {
            "insertTable": {
                "rows": 3,
                "columns": 3,
                "location": {
                    "index": 1,
                    "tabId": TAB_ID
                }
            }
        }
    ]
    
    try:
        response = service.documents().batchUpdate(documentId=DOCUMENT_ID, body={'requests': requests}).execute()
        print("Table inserted successfully!")
        
        # Now let's try to find where it was inserted to fill it
        # The reply should contain the table's start index
        # But cell filling is another batchUpdate
        
    except Exception as e:
        print(f"Failed to insert table: {e}")

if __name__ == "__main__":
    test_insert_table()
