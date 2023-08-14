import io

import boto3
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd

load_dotenv()

CSV_FILE_NAME_GENERATED_TODAY = f"rates-{datetime.today().date()}.csv".replace("-", "_")
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')


s3_client: boto3.client = boto3.client('s3',
                                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                        region_name=AWS_REGION_NAME)

def upload_file_to_s3(file_buffer: io.StringIO, key: str):
    try:
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET_NAME, Key=key, Body=file_buffer.getvalue()
        )
    except Exception as e:
        print(e)

def load_data(dataframe: pd.DataFrame, key= CSV_FILE_NAME_GENERATED_TODAY):
    with io.StringIO() as csv_buffer:
        dataframe.to_csv(csv_buffer, index=True)
        upload_file_to_s3(csv_buffer, key)

# Default: get file generated today
def get_file(file_name: str = CSV_FILE_NAME_GENERATED_TODAY) -> io.StringIO:
    try:
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET_NAME, Key=file_name)
        csv_content = response['Body'].read().decode('utf-8')
        return io.StringIO(csv_content)
    except Exception as e:
        print(e)
        return None
