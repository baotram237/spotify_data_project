from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import io

# Add your project path for custom extractor
sys.path.append('/opt/airflow/dags/src')

from extraction.websources.api_extraction import categoriesExtraction
from load.s3_handler import s3Handler
from load.redshift_handler import redshiftHandler

# Default arguments
default_args = {
    "owner": "tramntb",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 14),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

# Config
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_KEY_PREFIX = 'SPOTIFY/CATEGORIES'
DATA_SOURCE = 'categories'
AWS_CONN_ID = 'aws_conn'
REDSHIFT_CONN_ID = 'redshift_conn'
REDSHIFT_TABLE = 'tramntb_dev.bronze_spotify_categories'
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

def extract_categories_to_s3(**context):
    """
    Extract categories data from Spotify API and write directly to S3 (Parquet)
    """
    try:
        # Extract data from Spotify API
        with categoriesExtraction() as extractor:
            df = extractor.extract_categories_data(limit=50, max_pages=None)

        # Validate extracted data
        if df is None or df.empty:
            logging.warning("No categories data extracted from Spotify API")
            return None

        # Add metadata columns
        df["extracted_at"] = datetime.now()
        df["extraction_date"] = context["ds"]

        # Generate timestamp for filename
        execution_date = context["execution_date"]
        timestamp = execution_date.strftime("%Y%m%d_%H%M%S")

        logging.info(f"Extracted {len(df)} records from Spotify Categories API")

        # S3 object key (Parquet)
        s3_key = (
            f"{S3_KEY_PREFIX}/year={execution_date.year}/month={execution_date.month:02d}/"
            f"day={execution_date.day:02d}/{DATA_SOURCE}_{timestamp}.parquet"
        )
        # Upload to S3
        s3_handler = s3Handler(AWS_CONN_ID)
        metadata = s3_handler.upload_df_to_s3(df, S3_BUCKET_NAME, s3_key, context)
        
        logging.info(f"Successfully uploaded categories data to S3: {s3_key}")
        return metadata

    except Exception as e:
        logging.error(f"Error extracting/uploading Spotify categories: {str(e)}")
        raise

def load_to_redshift(**context):
    """
    Full load: Truncate table and copy all data from S3 to Redshift
    """
    try:
        redshift_handler = redshiftHandler(
            REDSHIFT_CONN_ID
        )

        task_instance = context['task_instance']
        file_info = task_instance.xcom_pull(task_ids='extract_categories_to_s3')

        if not file_info:
            logging.error("No file info received from extraction task")
            raise ValueError("No S3 file information available")
        
        s3_key = file_info['s3_key']
        record_count = file_info['record_count']

        logging.info(f"Full load: Truncating table and loading {record_count} records from s3://{S3_BUCKET_NAME}/{s3_key}")
        
        redshift_handler.full_load_to_redshift(
            aws_conn_id = AWS_CONN_ID,
            s3_bucket = S3_BUCKET_NAME,
            s3_key = s3_key,
            redshift_table = REDSHIFT_TABLE
        )
        
    except Exception as e:
        logging.error(f"Error during full load to Redshift: {str(e)}")
        raise

def dbt_success_callback(context):
    """Callback for successful dbt runs"""
    task_instance = context.get('ti')
    logging.info(f"dbt transformation succeeded for task: {task_instance.task_id}, DAG: {context['dag'].dag_id}")

def dbt_failure_callback(context):
    """Callback for failed dbt runs"""
    task_instance = context.get('ti')
    logging.error(f"dbt transformation FAILED for task: {task_instance.task_id}, DAG: {context['dag'].dag_id}")

# DAG definition
with DAG(
    "categories_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=["spotify", "categories", "full_load"],
) as dag:

    # Task definitions
    extract_to_s3 = PythonOperator(
        task_id="extract_categories_to_s3",
        python_callable=extract_categories_to_s3,
        provide_context=True,
    )

    load_to_redshift = PythonOperator(
        task_id="load_categories_to_redshift",
        python_callable=load_to_redshift,
        provide_context=True,
    )

# 2. dbt Transformation Task Group
    with TaskGroup("dbt_transformations", tooltip="Run dbt models") as dbt_group:
        
        # dbt deps - install packages
        dbt_deps = BashOperator(
            task_id='dbt_deps',
            bash_command=f'export PATH=$PATH:/home/airflow/.local/bin && cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}',
            env={
                'DBT_PROFILES_DIR': DBT_PROFILES_DIR,
                'DBT_PROJECT_DIR': DBT_PROJECT_DIR,
            }
        )
        
        # dbt run mart models
        dbt_run_mart = BashOperator(
            task_id='dbt_run_mart',
            bash_command=f'export PATH=$PATH:/home/airflow/.local/bin && cd {DBT_PROJECT_DIR} && dbt run -m spotify_mrt__categories --profiles-dir {DBT_PROFILES_DIR}',
            on_success_callback=dbt_success_callback,
            on_failure_callback=dbt_failure_callback,
            env={
            'DBT_PROFILES_DIR': DBT_PROFILES_DIR,
            'DBT_PROJECT_DIR': DBT_PROJECT_DIR,
            }
        )
        
        # Task dependencies within dbt group
        dbt_deps>> dbt_run_mart

    # Task dependencies
    extract_to_s3 >> load_to_redshift >> dbt_group