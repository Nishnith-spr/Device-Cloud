import boto3
from aws_db_creds import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

def get_athena_client():
    return boto3.client(
        "athena",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        verify="/etc/ssl/cert.pem"
    )
