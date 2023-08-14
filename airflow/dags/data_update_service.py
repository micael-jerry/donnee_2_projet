import pandas as pd
from s3_service import get_file
from s3_service import load_data
from datetime import datetime
import io


def update_data():
    column_name: str = f"{datetime.today().date()}"
    rates_data: io.StringIO = get_file("data.csv")
    rate_today: io.StringIO = get_file()

    rates_data_df = None
    rate_today_df: pd.DataFrame = pd.read_csv(rate_today, index_col=0, header=0)

    if rates_data is None:
        rates_data_df = pd.DataFrame(rate_today_df.rename(columns={'rates': column_name}))
    else:
        rates_data_df = pd.read_csv(rates_data, index_col=0, header=0)
        rates_data_df[column_name] = rate_today_df['rates']

    load_data(
        rates_data_df,
        add_index=True,
        key="data.csv"
    )
