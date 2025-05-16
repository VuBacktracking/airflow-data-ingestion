FROM apache/airflow:2.9.2

# Switch to root to install system dependencies
USER root

# Install apt packages
RUN apt-get update \
    && apt-get install -y gosu \
    && apt-get install -y git \
    && apt-get install -y libgomp1 \
    && apt-get install -y --no-install-recommends vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies
COPY requirements.txt requirements.txt
# Switch back to airflow user
USER airflow
# Install Python dependencies 
RUN pip install --upgrade pip && pip install --upgrade --no-cache-dir -r requirements.txt

