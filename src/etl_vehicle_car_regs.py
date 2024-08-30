import pandas as pd
from datetime import datetime
import logging 

logging.basicConfig(
    filename="status.log", format="%(asctime)s %(message)s", filemode="w"
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_data():
    df = pd.DataFrame()
    offset_year = int(datetime.now().strftime('%y'))
    for i in range(offset_year+1):
        if i < 10:
            URL_DATA = f'https://storage.data.gov.my/transportation/cars_200{i}.parquet'
        else:
            URL_DATA = f'https://storage.data.gov.my/transportation/cars_20{i}.parquet'
        partial_df = pd.read_parquet(URL_DATA)
        df = pd.concat([df, partial_df])
        logger.info(f"Extracted data from {URL_DATA}")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    change_dtypes = {
        "type" : "category"
    }
    df = df.astype(change_dtypes)
    if 'date_reg' in df.columns: df['date_reg'] = pd.to_datetime(df['date_reg'])
    
    df = df.assign(
    year = df['date_reg'].dt.year,
    month = df['date_reg'].dt.month,
    day = df['date_reg'].dt.day
    )
    logger.info(f"Transformed data")
    return df

def load_data(df: pd.DataFrame) -> None:
    df.to_parquet('../data/vehicle_car_regs_2000_to_now.parquet')
    logger.info(f"Loaded data to parquet")
    logger.info(f"Dataframe rows: {df.shape[0]}, columns: {df.shape[1]}")
    
def main():
    master_df = extract_data()
    master_df = transform_data(master_df)
    load_data(master_df)

if __name__ == '__main__':
    main()
    print("ETL process completed")
