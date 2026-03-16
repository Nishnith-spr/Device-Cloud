from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'

def create_tab():
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Creating new tab in document {DOCUMENT_ID}...")
    
    # Check if insertTab is available in the API client
    # Note: If the client library is old, it might not have the tab features.
    # We can try a generic batchUpdate.
    
    requests = [
        {
            "insertTab": {
                "tabProperties": {
                    "title": "March 15th - Automation"
                }
            }
        }
    ]
    
    try:
        result = service.documents().batchUpdate(documentId=DOCUMENT_ID, body={'requests': requests}).execute()
        new_tab_id = result.get('replies')[0].get('insertTab').get('tabId')
        print(f"Success! Created tab with ID: {new_tab_id}")
        return new_tab_id
    except Exception as e:
        print(f"Failed to create tab: {e}")
        # If insertTab fails, it might be due to API version or permissions
        return None

if __name__ == "__main__":
    create_tab()
