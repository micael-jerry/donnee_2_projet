import io

import boto3
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

CSV_FILE_NAME_GENERATED_TODAY = f"rates-{datetime.today().date()}.csv".replace("-", "_")
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')


def upload_file_to_s3(file_buffer: io.StringIO):
    s3_client: boto3.client = boto3.client('s3',
                                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                           region_name=AWS_REGION_NAME)

    try:
        s3_client.put_object(
            Bucket=AWS_S3_BUCKET_NAME, Key=CSV_FILE_NAME_GENERATED_TODAY, Body=file_buffer.getvalue()
        )
    except Exception as e:
        print(e)
