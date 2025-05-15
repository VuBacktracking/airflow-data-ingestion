# Airflow Data Insgestion Pipeline

## Architecture


## Tasks
- [x] Design overall data pipeline architecture (Docker, EC2, S3, Delta Lake, Trino)
- [x] Define and configure Airflow DAG for orchestration
    - [x] Extract and load raw taxi data (yellow/green) to Amazon S3
    - [ ] Transform raw data into structured format
    - [x] Convert transformed data to Delta format
- [ ] Configure Trino to connect to Delta Lake on S3
- [ ] Enable data scientists to query data using Trino
- [ ] Manage infrastructure with Terraform modules
    - [x] Provision Amazon S3 bucket
    - [ ] Provision EC2 instance
- [ ] Set up CI for Pull Requests (e.g., GitHub Actions)

## Pipeline

## Troubeshoot

```bash
    - ./.env:/opt/airflow/.env
    ~> dotenv_path = Path(__file__).resolve().parent.parent.parent / ".env"
```