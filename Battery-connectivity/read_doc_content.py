import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

def read_google_doc(doc_id, credentials_path):
    scopes = ['https://www.googleapis.com/auth/documents.readonly']
    creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scopes)
    service = build('docs', 'v1', credentials=creds)
    
    try:
        doc = service.documents().get(documentId=doc_id).execute()
        title = doc.get('title')
        print(f"Document Title: {title}\n")
        
        content = doc.get('body').get('content')
        full_text = ""
        for element in content:
            if 'paragraph' in element:
                elements = element.get('paragraph').get('elements')
                for leaf in elements:
                    if 'textRun' in leaf:
                        full_text += leaf.get('textRun').get('content')
        return full_text
    except Exception as e:
        return f"Error reading document: {e}"

if __name__ == "__main__":
    DOCUMENT_ID = "1jM8GgVmT8BjalEege-u4Ufe3OjZwtWM5HPoVryWWpZw"
    CRED_PATH = "write_ghsteet.json"
    
    text = read_google_doc(DOCUMENT_ID, CRED_PATH)
    print("--- Document Content ---")
    print(text)
    print("------------------------")
