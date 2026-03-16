from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'
TAB_ID = 't.j7wgibilod55' # "17th March"

def read_tab_content():
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Reading content from tab {TAB_ID}...")
    
    try:
        doc = service.documents().get(documentId=DOCUMENT_ID, includeTabsContent=True).execute()
        
        target_tab = None
        for tab in doc.get('tabs', []):
            if tab.get('tabProperties', {}).get('tabId') == TAB_ID:
                target_tab = tab
                break
        
        if target_tab:
            content = target_tab.get('documentTab', {}).get('body', {}).get('content', [])
            # Find the total length (end index)
            end_index = content[-1].get('endIndex') if content else 1
            print(f"Tab {TAB_ID} has content. End index: {end_index}")
            
            # Print first few lines of content for context
            text = ""
            for element in content:
                if 'paragraph' in element:
                    for run in element['paragraph']['elements']:
                        if 'textRun' in run:
                            text += run['textRun']['content']
            
            print("--- Tab Content Preview ---")
            print(text[:500] + "...")
            return end_index
        else:
            print("Tab not found")
            
    except Exception as e:
        print(f"Failed to read tab: {e}")

if __name__ == "__main__":
    read_tab_content()
