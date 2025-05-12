import os
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = "data"  

def download_taxi_data(taxi_type, execution_date):
    year = execution_date.year
    month = execution_date.month
    file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{file_name}"
    save_dir = os.path.join(DATA_DIR, taxi_type)
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, file_name)

    if os.path.exists(save_path):
        print(f"✅ File already exists: {save_path}")
        return

    # HEAD check
    try:
        head = requests.head(url, timeout=10)
        if head.status_code != 200:
            print(f"❌ File not available (HTTP {head.status_code}): {url}")
            return
    except Exception as e:
        print(f"❌ HEAD request failed: {url} - {e}")
        return

    # Download
    try:
        print(f"⬇️  Downloading: {url}")
        response = requests.get(url, stream=True, timeout=60)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Download completed: {save_path}")
        else:
            print(f"❌ Download failed: HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ Download error: {url} - {e}")
