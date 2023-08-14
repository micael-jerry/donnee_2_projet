import io
import pandas as pd
from s3_service import get_file

def get_data_to_be_analyzed():
    rates_data:io.StringIO = get_file("data.csv")
    rates_data_df = pd.read_csv(rates_data, index_col=0, header=0)
    
    rates_data_transformed = []
    
    date_columns = rates_data_df.columns[2:]
    
    for index, row in rates_data_df.iterrows():
        symbols = row['symbols']
        countries = row['countries']

        for date_col in date_columns:
            date = date_col
            value = row[date_col]
        
            new_row = [symbols, countries, date, value]
        
            rates_data_transformed.append(new_row)
    
    rates_data_transformed_df: pd.DataFrame = pd.DataFrame(rates_data_transformed, columns=['symbols', 'countries', 'date', 'value'])
    
    rates_data_transformed_df.to_csv("data/rates_data_transformed_df.csv", index=True, index_label="id")