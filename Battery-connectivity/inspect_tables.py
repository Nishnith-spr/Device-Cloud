from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'
TAB_ID = 't.j7wgibilod55' # "17th March"

def inspect_tab_structure():
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Inspecting structure of tab {TAB_ID}...")
    
    try:
        doc = service.documents().get(documentId=DOCUMENT_ID, includeTabsContent=True).execute()
        
        target_tab = None
        for tab in doc.get('tabs', []):
            if tab.get('tabProperties', {}).get('tabId') == TAB_ID:
                target_tab = tab
                break
        
        if target_tab:
            content = target_tab.get('documentTab', {}).get('body', {}).get('content', [])
            found_tables = 0
            for element in content:
                if 'table' in element:
                    found_tables += 1
                    table = element['table']
                    print(f"\n[Table {found_tables}] at index {element['startIndex']}")
                    print(f"Rows: {table['rows']}, Columns: {table['columns']}")
                    # Peek at first cell content
                    try:
                        first_cell = table['tableRows'][0]['tableCells'][0]
                        cell_text = ""
                        for item in first_cell['content']:
                            if 'paragraph' in item:
                                for run in item['paragraph']['elements']:
                                    if 'textRun' in run: cell_text += run['textRun']['content']
                        print(f"First cell text: '{cell_text.strip()}'")
                    except: pass
            
            if found_tables == 0:
                print("No tables found in this tab.")
        else:
            print("Tab not found")
            
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    inspect_tab_structure()
