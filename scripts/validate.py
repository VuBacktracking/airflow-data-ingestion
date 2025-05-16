import os
from airflow.exceptions import AirflowSkipException

DATA_DIR = "data" 

def validate_file_exists(taxi_type, execution_date):
    year = execution_date.year
    month = execution_date.month
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    file_path = os.path.join(DATA_DIR, taxi_type, file_name)

    if not os.path.exists(file_path):
        raise AirflowSkipException(f"⚠️ Skipped: file not found: {file_path}")
    
    print(f"✅ File validated: {file_path}")