# Start from official Airflow image
FROM apache/airflow:2.10.2-python3.10

USER root

# Install system dependencies if needed (for dbt)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install dbt 
RUN pip install --no-cache-dir \
    dbt-core==1.9.0 \
    dbt-redshift==1.9.5 \
    redshift-connector==2.1.8

