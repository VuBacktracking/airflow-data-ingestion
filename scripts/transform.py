import os
import boto3
import s3fs
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

# .env configuration
BUCKET_NAME = os.getenv('AWS_S3_BUCKET')
TRANSFORMED_PREFIX = os.getenv('AWS_S3_TRANSFORMED_PREFIX', 'transformed')
RAW_PREFIX = os.getenv('AWS_S3_RAW_PREFIX', 'raw')

# Parameters & Arguments
DATA_PATH = "data"
TAXI_LOOKUP_PATH = os.path.join(os.path.dirname(__file__), "data_lookup", "taxi_lookup.csv")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
)
s3_fs = s3fs.S3FileSystem()

# Process data
def drop_column(df, file):
    """
        Drop columns 'store_and_fwd_flag'
    """
    if "store_and_fwd_flag" in df.columns:
        df = df.drop(columns=["store_and_fwd_flag"])
        print("Dropped column store_and_fwd_flag from file: " + file)
    else:
        print("Column store_and_fwd_flag not found in file: " + file)

    return df


def merge_taxi_zone(df, file):
    """
        Merge dataset with taxi zone lookup
    """
    df_lookup = pd.read_csv(TAXI_LOOKUP_PATH)

    def merge_and_rename(df, location_id, lat_col, long_col):
        df = df.merge(df_lookup, left_on=location_id, right_on="LocationID")
        df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
        df = df.rename(columns={
            "latitude" : lat_col,
            "longitude" : long_col
        })
        return df

    if "pickup_latitude" not in df.columns:
        df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")
        
    if "dropoff_latitude" not in df.columns:
        df = merge_and_rename(df, "dolocationid", "dropoff_latitude", "dropoff_longitude")

    # df = df.drop(columns=[col for col in df.columns if "Unnamed" in col], errors='ignore').dropna()

    print("Merged data from file: " + file)

    return df


def process(df, file):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime, ehail_fee
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """
    
    if file.startswith("green"):
        # rename columns
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "ehail_fee": "fee"
            },
            inplace=True
        )

        # drop column
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)

    elif file.startswith("yellow"):
        # rename columns
        df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "airport_fee": "fee"
            },
            inplace=True
        )

    # fix data type in columns 'payment_type', 'dolocationid', 'pulocationid', 'vendorid'
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].astype(int)
    if "dolocationid" in df.columns:
        df["dolocationid"] = df["dolocationid"].astype(int)
    if "pulocationid" in df.columns:
        df["pulocationid"] = df["pulocationid"].astype(int)
    if "vendorid" in df.columns:
        df["vendorid"] = df["vendorid"].astype(int)

    # drop column 'fee'
    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
                
    # Remove missing data
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)
    
    print("Transformed data from file: " + file)

    return df

def processing_dataframe(df: pd.DataFrame, file_path: str) -> pd.DataFrame:
    """
    Additional aggregation and transformation on the cleaned DataFrame using pandas.
    """
    # Create time features
    df["year"] = pd.to_datetime(df["pickup_datetime"]).dt.year
    df["month"] = pd.to_datetime(df["pickup_datetime"]).dt.strftime("%B")
    df["dow"] = pd.to_datetime(df["pickup_datetime"]).dt.strftime("%A")

    # Assign service type based on file path
    if "yellow" in file_path.lower():
        df["service_type"] = 1
    elif "green" in file_path.lower():
        df["service_type"] = 2
    else:
        df["service_type"] = 0

    # Group and aggregate
    df_grouped = df.groupby([
        "year", "month", "dow",
        "vendorid", "ratecodeid",
        "pulocationid", "dolocationid",
        "payment_type", "pickup_datetime", "dropoff_datetime",
        "pickup_latitude", "pickup_longitude",
        "dropoff_latitude", "dropoff_longitude",
        "service_type"
    ], dropna=False).agg({
        "passenger_count": "sum",
        "trip_distance": "sum",
        "extra": "sum",
        "mta_tax": "sum",
        "fare_amount": "sum",
        "tip_amount": "sum",
        "tolls_amount": "sum",
        "total_amount": "sum",
        "improvement_surcharge": "sum",
        "congestion_surcharge": "sum"
    }).reset_index()

    return df_grouped

def transform_from_s3(taxi_type: str):
    """
    Transform data from S3 bucket and save to transformed bucket
    Args:
        taxi_type (str): Type of taxi data to process (e.g., "yellow", "green").
    """
    s3_input_prefix = f"{RAW_PREFIX}/{taxi_type}"
    s3_output_prefix = f"{TRANSFORMED_PREFIX}/{taxi_type}"

    # List all files in the S3 bucket
    files = s3_fs.glob(f"{BUCKET_NAME}/{s3_input_prefix}/{taxi_type}_tripdata_*.parquet")

    if not files:
        print(f"No files found in s3://{BUCKET_NAME}/{s3_input_prefix}")
        return

    for s3_file_path in files:
        file_name = os.path.basename(s3_file_path)
        print(f"Processing file: {s3_file_path}")

        try:
            # Read file from S3
            df = pd.read_parquet(f"s3://{s3_file_path}", filesystem=s3_fs, engine='pyarrow')
            df.columns = map(str.lower, df.columns)

            # Transform
            df = drop_column(df, file_name)
            df = merge_taxi_zone(df, file_name)
            df = process(df, file_name)
            df = processing_dataframe(df, file_name)

            # Save transformed DataFrame to S3
            s3_output_path = f"s3://{BUCKET_NAME}/{s3_output_prefix}/{file_name}"
            df.to_parquet(s3_output_path, index=False, filesystem=s3_fs, engine='pyarrow')
            print(f"Saved transformed file to: {s3_output_path}")

        except Exception as e:
            print(f"Failed processing file {file_name}: {e}")