import io

import requests as rq
import pandas as pd
import os

from s3_service import upload_file_to_s3


def extract_transform_load_data():
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

    load_data(df)


def load_data(dataframe: pd.DataFrame):
    with io.StringIO() as csv_buffer:
        dataframe.to_csv(csv_buffer, index=True)
        upload_file_to_s3(csv_buffer)