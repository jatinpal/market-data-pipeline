FROM apache/airflow:2.7.3

# Install Python packages needed for pipeline
RUN pip install --no-cache-dir \
    yfinance \
    google-cloud-storage \
    pandas \
    dbt-bigquery

# Set working directory
WORKDIR /opt/airflow