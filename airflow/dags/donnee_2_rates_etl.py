import boto3
import requests as rq
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

CSV_FILE_NAME_GENERATED_TODAY = f"rates-{datetime.today().date()}.csv".replace("-", "_")


def extract_transform_data():
    BASE_URL = os.getenv('BASE_URL')
    API_KEY = os.getenv('API_KEY')
    url = f'{BASE_URL}/fixer'
    requests_headers = {
        'apikey': API_KEY
    }

    get_symbols_response: rq.Response = rq.get(f'{url}/symbols', headers=requests_headers)
    symbols: dict = get_symbols_response.json()['symbols']
    df_symbols: pd.DataFrame = pd.DataFrame(symbols.items(), columns=['symbols', 'countries'])

    base_request_param = 'EUR'
    get_rates_response: rq.Response = rq.get(f'{url}/latest?base={base_request_param}', headers=requests_headers)

    rates_response_json = get_rates_response.json()
    rates = rates_response_json['rates']

    df_rates: pd.DataFrame = pd.DataFrame(list(rates.items()), columns=['symbols', 'rates'])

    df: pd.DataFrame = pd.merge(df_rates, df_symbols, how='left')

    df.to_csv(f"data/rates/{CSV_FILE_NAME_GENERATED_TODAY}")


def load_data():
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
    AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')

    s3_client: boto3.client = boto3.client('s3',
                                           aws_access_key_id=AWS_ACCESS_KEY_ID,
                                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                           region_name=AWS_REGION_NAME)

    try:
        s3_client.upload_file(f"data/rates/{CSV_FILE_NAME_GENERATED_TODAY}", AWS_S3_BUCKET_NAME,
                              CSV_FILE_NAME_GENERATED_TODAY)
    except Exception as e:
        print(e)
