from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Import hàm download
from scripts.extract import download_taxi_data
from scripts.validate import validate_file_exists
from scripts.load import upload_folder_to_s3

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="taxi_etl_dag",
    start_date=datetime(2024, 1, 1), # Ngày bắt đầu chạy DAG và tương ứng sẽ download dữ liệu tháng đó
    end_date=datetime(2024, 2, 1),  # Để chạy cho Jan và Feb 2024
    schedule_interval="@monthly",
    catchup=True,
    default_args=default_args,
    tags=["etl", "taxi", "draft"],
) as dag:
    
    start = EmptyOperator(task_id="start")

    # Nhánh 1: Yellow Taxi
    extract_yellow_taxi = PythonOperator(
        task_id="extract_yellow_taxi",
        python_callable=download_taxi_data,
        op_kwargs={"taxi_type": "yellow"},
        provide_context=True,
    )

    validate_yellow_taxi = PythonOperator(
        task_id="validate_yellow_taxi",
        python_callable=validate_file_exists,
        op_kwargs={"taxi_type": "yellow"},
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE  
    )
    
    # Nhánh 2: Green Taxi
    extract_green_taxi = PythonOperator(
        task_id="extract_green_taxi",
        python_callable=download_taxi_data,
        op_kwargs={"taxi_type": "green"},
        provide_context=True,
    )

    validate_green_taxi = PythonOperator(
        task_id="validate_green_taxi",
        python_callable=validate_file_exists,
        op_kwargs={"taxi_type": "green"},
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    load_raw_yellow_to_s3 = PythonOperator(
        task_id="load_raw_yellow_to_s3",
        python_callable=upload_folder_to_s3,
        op_kwargs={"local_folder": "yellow", "s3_subfolder": "yellow"},
        provide_context=True,
    )
    
    load_raw_green_to_s3 = PythonOperator(
        task_id="load_raw_green_to_s3",
        python_callable=upload_folder_to_s3,
        op_kwargs={"local_folder": "green", "s3_subfolder": "green"},
        provide_context=True,
    )
    
    join = EmptyOperator(task_id="join")
    end = EmptyOperator(task_id="end")

    # Thiết lập flow
    start >> [extract_yellow_taxi, extract_green_taxi]
    extract_yellow_taxi >> validate_yellow_taxi >> load_raw_yellow_to_s3 >> join
    extract_green_taxi >> validate_green_taxi >> load_raw_green_to_s3 >> join
    join >> end
