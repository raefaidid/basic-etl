import pandas as pd
from datetime import datetime

master_df = pd.DataFrame()
offset_year = int(datetime.now().strftime('%y'))

for i in range(offset_year+1):
    if i < 10:
        URL_DATA = f'https://storage.data.gov.my/transportation/cars_200{i}.parquet'
    else:
        URL_DATA = f'https://storage.data.gov.my/transportation/cars_20{i}.parquet'
    df = pd.read_parquet(URL_DATA)
    master_df = pd.concat([master_df, df])

if 'date' in master_df.columns: master_df['date'] = pd.to_datetime(master_df['date'])    


change_dtypes = {
    "type" : "category"
}

master_df = master_df.astype(change_dtypes)

if 'date_reg' in master_df.columns: master_df['date_reg'] = pd.to_datetime(master_df['date_reg'])

master_df = master_df.assign(
    year = master_df['date_reg'].dt.year,
    month = master_df['date_reg'].dt.month,
    day = master_df['date_reg'].dt.day
)
