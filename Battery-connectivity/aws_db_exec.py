import time
import pandas as pd
import boto3
import io
from aws_db_creds import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

# Global S3 client to avoid re-creation overhead
_S3_CLIENT = None

def get_s3_client():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        _S3_CLIENT = boto3.client(
            's3', 
            aws_access_key_id=AWS_ACCESS_KEY_ID, 
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
            region_name=AWS_REGION,
            verify="/etc/ssl/cert.pem"
        )
    return _S3_CLIENT

def run_query(client, query, database, s3_output, poll_interval=1, timeout=300):
    """
    Starts an Athena query and polls for completion.
    """
    resp = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output},
    )

    qid = resp["QueryExecutionId"]
    start_time = time.time()

    while True:
        status_resp = client.get_query_execution(QueryExecutionId=qid)
        state = status_resp["QueryExecution"]["Status"]["State"]
        
        if state == "SUCCEEDED":
            print(f"\nQuery {qid} succeeded.")
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get("StateChangeReason", "No reason provided")
            print(f"\nQuery {qid} {state}")
            raise RuntimeError(f"Athena query {state}: {reason}")
        
        if time.time() - start_time > timeout:
            client.stop_query_execution(QueryExecutionId=qid)
            print(f"\nQuery {qid} TIMEOUT")
            raise RuntimeError(f"Athena query TIMEOUT after {timeout} seconds")
            
        print(f"Query is {state:10} ({int(time.time() - start_time)}s elapsed)...", end="\r")
        time.sleep(poll_interval)

    return qid

def fetch_df(client, qid):
    """
    FAST PATH: Downloads the result CSV directly from S3.
    Avoids the slow row-by-row pagination of the Athena API.
    """
    # 1. Get the S3 result location
    res = client.get_query_execution(QueryExecutionId=qid)
    s3_path = res['QueryExecution']['ResultConfiguration']['OutputLocation']
    
    # 2. Extract Bucket and Key
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1]
    
    # 3. Download using optimized S3 client
    print(f"Fetching results from S3...")
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    
    # 4. Read directly into pandas (fastest method)
    return pd.read_csv(obj['Body'], low_memory=False)

