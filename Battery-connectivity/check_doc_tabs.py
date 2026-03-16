from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'

def get_doc_structure():
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Fetching document {DOCUMENT_ID} structure...")
    
    try:
        # Some API clients might not support the includeTabsContent parameter yet in the helper method
        # but we can try passing it as a kwarg
        doc = service.documents().get(documentId=DOCUMENT_ID, includeTabsContent=True).execute()
        
        print(f"Title: {doc.get('title')}")
        tabs = doc.get('tabs')
        if tabs:
            print(f"Found {len(tabs)} tabs:")
            for tab in tabs:
                props = tab.get('tabProperties', {})
                print(f" - {props.get('title')} (ID: {props.get('tabId')})")
        else:
            print("No tabs found in document. Metadata keys: " + ", ".join(doc.keys()))
            
    except Exception as e:
        print(f"Failed to get doc: {e}")

if __name__ == "__main__":
    get_doc_structure()
