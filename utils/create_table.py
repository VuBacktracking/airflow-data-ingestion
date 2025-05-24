
import os
import logging
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient
from pathlib import Path

# Load environment variables
dotenv_path = Path(__file__).resolve().parent / ".env"
load_dotenv(".env")

def main():
    # Initialize PostgresSQLClient with credentials from environment variables
    try:
        pc = PostgresSQLClient(
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        logging.info("PostgreSQL client initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize PostgreSQL client: {e}")
        return

    # SQL queries to create tables
    create_table_staging = """
        DROP TABLE IF EXISTS staging.nyc_taxi;
        CREATE TABLE IF NOT EXISTS staging.nyc_taxi (
            year                    VARCHAR,
            month                   VARCHAR,
            dow                     VARCHAR,
            vendor_id               INT, 
            rate_code_id            FLOAT, 
            pickup_location_id      INT, 
            dropoff_location_id     INT, 
            payment_type_id         INT, 
            pickup_datetime         TIMESTAMP WITHOUT TIME ZONE, 
            dropoff_datetime        TIMESTAMP WITHOUT TIME ZONE, 
            pickup_latitude         FLOAT,
            pickup_longitude        FLOAT,
            dropoff_latitude        FLOAT,
            dropoff_longitude       FLOAT,
            passenger_count         FLOAT, 
            trip_distance           FLOAT,
            extra                   FLOAT, 
            mta_tax                 FLOAT, 
            fare_amount             FLOAT, 
            tip_amount              FLOAT, 
            tolls_amount            FLOAT, 
            total_amount            FLOAT, 
            improvement_surcharge   FLOAT, 
            congestion_surcharge    FLOAT,
            service_type            INT
        );
    """

    # Execute queries with error handling
    try:
        pc.execute_query(create_table_staging)
        logging.info("Table 'staging.nyc_taxi' created successfully or already exists.")
    except Exception as e:
        logging.error(f"Failed to create table 'staging.nyc_taxi': {e}")

if __name__ == "__main__":
    main()
