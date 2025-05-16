import os
import boto3
from dotenv import load_dotenv
from pathlib import Path
from botocore.exceptions import NoCredentialsError

# Load environment variables from .env file
dotenv_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path)

# .env configuration
BUCKET_NAME = os.getenv('AWS_S3_BUCKET')
S3_BASE_PREFIX = os.getenv('AWS_S3_RAW_PREFIX', 'raw')  # raw for default

LOCAL_BASE_DIR = os.path.join('data')

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
)
# Initialize S3 client
s3_client = boto3.client('s3')

def upload_file(local_path, bucket, s3_key):
    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        print(f"Uploaded: {local_path} -> s3://{bucket}/{s3_key}")
    except FileNotFoundError:
        print(f"File not found: {local_path}")
    except NoCredentialsError:
        print("AWS credentials not available.")
    except Exception as e:
        print(f"Upload failed for {local_path}: {e}")

def upload_folder_to_s3(local_folder, s3_subfolder):
    folder_path = os.path.join(LOCAL_BASE_DIR, local_folder)
    for file_name in os.listdir(folder_path):
        local_file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(local_file_path):
            s3_key = f"{S3_BASE_PREFIX}/{s3_subfolder}/{file_name}"
            upload_file(local_file_path, BUCKET_NAME, s3_key)

# Upload từng thư mục
# for subfolder in ['yellow', 'green']:
#     upload_folder_to_s3(local_folder=subfolder, s3_subfolder=subfolder)