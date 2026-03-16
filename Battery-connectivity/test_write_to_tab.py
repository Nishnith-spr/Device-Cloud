from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'
TEST_TAB_ID = 't.pbpl9p50b6p7' # "Copy of 10th March"

def write_to_tab():
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Writing test text to tab {TEST_TAB_ID}...")
    
    requests = [
        {
            "insertText": {
                "text": "\n--- AUTOMATED TEST WRITING ---\n",
                "location": {
                    "index": 1,
                    "tabId": TEST_TAB_ID
                }
            }
        }
    ]
    
    try:
        result = service.documents().batchUpdate(documentId=DOCUMENT_ID, body={'requests': requests}).execute()
        print("Success! Check the 'Copy of 10th March' tab.")
    except Exception as e:
        print(f"Failed to write to tab: {e}")

if __name__ == "__main__":
    write_to_tab()
