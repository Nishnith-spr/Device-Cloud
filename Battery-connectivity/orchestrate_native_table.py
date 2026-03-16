from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import json

DOCUMENT_ID = '1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw'
SERVICE_ACCOUNT_FILE = 'write_ghsteet.json'
TAB_ID = 't.pbpl9p50b6p7'

def create_and_fill_table(l1_data):
    scopes = ['https://www.googleapis.com/auth/documents']
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)

    print(f"Orchestrating Native Table for L1 Summary in tab {TAB_ID}...")
    
    # Step 1: Insert Empty Table at index 1
    insert_req = {
        "insertTable": {
            "rows": len(l1_data),
            "columns": len(l1_data[0]),
            "location": {"index": 1, "tabId": TAB_ID}
        }
    }
    
    try:
        response = service.documents().batchUpdate(documentId=DOCUMENT_ID, body={'requests': [insert_req]}).execute()
        # Find the inserted table from the updated doc
        doc = service.documents().get(documentId=DOCUMENT_ID, includeTabsContent=True).execute()
        
        target_tab = None
        for tab in doc.get('tabs', []):
            if tab.get('tabProperties', {}).get('tabId') == TAB_ID:
                target_tab = tab
                break
        
        if not target_tab: return
        
        # The table should be at the very beginning (index 1)
        # Structural elements are in target_tab['documentTab']['body']['content']
        table_element = None
        for element in target_tab['documentTab']['body']['content']:
            if 'table' in element: # Taking the first one
                table_element = element['table']
                break
        
        if not table_element:
            print("Could not find the table we just inserted.")
            return

        # Step 2: Prepare InsertText requests using cell start indices
        fill_requests = []
        for r_idx, row in enumerate(l1_data):
            for c_idx, cell_text in enumerate(row):
                # Table structure: tableRows -> tableCells -> content -> startIndex
                cell = table_element['tableRows'][r_idx]['tableCells'][c_idx]
                cell_start_index = cell['content'][0]['startIndex']
                
                fill_requests.append({
                    "insertText": {
                        "text": str(cell_text),
                        "location": {"index": cell_start_index, "tabId": TAB_ID}
                    }
                })
                
                # Bold the headers
                if r_idx == 0:
                    fill_requests.append({
                        "updateTextStyle": {
                            "range": {
                                "startIndex": cell_start_index,
                                "endIndex": cell_start_index + len(str(cell_text)),
                                "tabId": TAB_ID
                            },
                            "textStyle": {"bold": True},
                            "fields": "bold"
                        }
                    })

        # Send fill requests in reverse order to keep indices stable?
        # Actually, since these are in separate cells, the start indices of cells are fixed *relative* to the table start!
        # wait, NO. As you insert text in Cell 1, the doc index of Cell 2 shifts forward.
        # So we MUST process fill_requests in REVERSE order of startIndex.
        
        fill_requests.sort(key=lambda x: x.get('insertText', {}).get('location', {}).get('index', 0), reverse=True)
        # Note: updateTextStyle also needs to be sorted if it's interleaved.
        # Better: Sort all requests by startIndex DESC.
        
        def get_idx(req):
            if 'insertText' in req: return req['insertText']['location']['index']
            if 'updateTextStyle' in req: return req['updateTextStyle']['range']['startIndex']
            return 0
            
        fill_requests.sort(key=get_idx, reverse=True)

        service.documents().batchUpdate(documentId=DOCUMENT_ID, body={'requests': fill_requests}).execute()
        print("✅ Native Table filled successfully.")
        
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    # Mock data
    l1_mock = [
        ["Metric", "W-0", "W-1"],
        ["Connectivity %", "74.1%", "73.7%"],
        ["Total", "110,044", "109,311"]
    ]
    create_and_fill_table(l1_mock)
