import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(
    filename="status.log", format="%(asctime)s %(message)s", filemode="w"
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_data():
    """
    Extract data from the given URL and return a pandas DataFrame.
    """
    df = pd.DataFrame()
    offset_year = int(datetime.now().strftime("%y")) #get the current year in 2 digits
    for i in range(offset_year + 1): #loop through the years from 2000 to the current year
        if i < 10:
            URL_DATA = f"https://storage.data.gov.my/transportation/cars_200{i}.parquet"
        else:
            URL_DATA = f"https://storage.data.gov.my/transportation/cars_20{i}.parquet"
        partial_df = pd.read_parquet(URL_DATA)
        df = pd.concat([df, partial_df]) #concatenate the partial dataframe to the master dataframe
        logger.info(f"Extracted data from {URL_DATA}")
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the data by changing the type column to category and converting the date_reg column to a datetime object.
    """
    change_dtypes = {"type": "category"}
    df = df.astype(change_dtypes) #change the type column to category
    if "date_reg" in df.columns:
        df["date_reg"] = pd.to_datetime(df["date_reg"]) #convert the date_reg column to a datetime object

    df = df.assign(
        year=df["date_reg"].dt.year, #create a new column for the year
        month=df["date_reg"].dt.month, #create a new column for the month
        day=df["date_reg"].dt.day, #create a new column for the day
    )
    logger.info(f"Transformed data")
    return df


def load_data(df: pd.DataFrame) -> None:
    """
    Load the data to a parquet file and store it in the data folder.
    """
    df.to_parquet("../data/vehicle_car_regs_2000_to_now.parquet") 
    logger.info(f"Loaded data to parquet")
    logger.info(f"Dataframe rows: {df.shape[0]}, columns: {df.shape[1]}") #log the number of rows and columns in the dataframe


def main():
    """
    Main function to execute the ETL process.
    """
    master_df = extract_data()
    master_df = transform_data(master_df)
    load_data(master_df)


if __name__ == "__main__":
    main()
    print("ETL process completed")
