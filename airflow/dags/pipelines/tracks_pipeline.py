from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import io

# Add your project path for custom extractor
sys.path.append('/opt/airflow/dags/src')
from extraction.websources.api_extraction import albumTrackExtraction
from load.s3_handler import s3Handler
from load.redshift_handler import redshiftHandler

albums_dataset = Dataset("redshift://tramntb_dev.bronze_spotify_new_albums")

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
S3_KEY_PREFIX = 'SPOTIFY/TRACKS'
DATA_SOURCE = 'tracks'
AWS_CONN_ID = 'aws_conn'
REDSHIFT_CONN_ID = 'redshift_conn'
REDSHIFT_TABLE = 'tramntb_dev.bronze_spotify_tracks'
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

def process_album_ids(**context):
    sql = """
        SELECT album_id
        FROM tramntb_dev.bronze_spotify_new_albums
    """
    try:
        redshift_handler = redshiftHandler(
            REDSHIFT_CONN_ID
        )
        df = redshift_handler.execute_query(sql)

        album_ids = df["album_id"].tolist()
        logging.info(f"Get {len(album_ids)} album ids from Redshift")
        return album_ids

    except Exception as e:
            logging.error(f"Error during query data from Redshift: {str(e)}")
            raise

def extract_tracks_to_s3(**context):
    """
    Extract tracks data from Spotify API and write directly to S3 (Parquet)
    """
    try:
        task_instance = context['task_instance']
        album_ids = task_instance.xcom_pull(task_ids='process_album_ids')

        all_dfs = []
        with albumTrackExtraction() as extractor:
            for i, album_id in enumerate(album_ids):
                print(f"Processing album {i+1}/{len(album_ids)}: {album_id}")
                try:
                    df = extractor.extract_album_track(album_id, market="US")
                    if not df.empty:
                        all_dfs.append(df)
                        print(f"Added {len(df)} tracks")
                    else:
                        print(f"No tracks found")
                except Exception as e:
                    print(f"Error: {str(e)}")

            if all_dfs:
                final_df = pd.concat(all_dfs, ignore_index=True)
                # Add metadata
                final_df["extracted_at"] = datetime.now()
                final_df["extraction_date"] = context["ds"]

                execution_date = context["execution_date"]
                timestamp = execution_date.strftime("%Y%m%d_%H%M%S")

                logging.info(f"Extracted data from Spotify track API: {len(final_df)} records")

                # S3 object key (Parquet)
                s3_key = (
                    f"{S3_KEY_PREFIX}/year={execution_date.year}/month={execution_date.month:02d}/"
                    f"day={execution_date.day:02d}/{DATA_SOURCE}_{timestamp}.parquet"
                )
                # Upload to S3
                s3_handler = s3Handler(AWS_CONN_ID)
                metadata = s3_handler.upload_df_to_s3(final_df, S3_BUCKET_NAME, s3_key, context)
                
                logging.info(f"Successfully uploaded tracks data to S3: {s3_key}")
                return metadata

    except Exception as e:
        logging.error(f"Error extracting/uploading Spotify tracks: {str(e)}")
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
        file_info = task_instance.xcom_pull(task_ids='extract_tracks_to_s3')

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
            redshift_table = REDSHIFT_TABLE,
            serialize = True
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
    "tracks_pipeline",
    default_args=default_args,
    schedule=[albums_dataset], 
    max_active_runs=1,
    tags=["spotify", "tracks", "full_load"],
) as dag:

    # Task definitions
    process_album_ids = PythonOperator(
        task_id="process_album_ids",
        python_callable=process_album_ids,
        provide_context=True,
    )

    extract_to_s3 = PythonOperator(
        task_id="extract_tracks_to_s3",
        python_callable=extract_tracks_to_s3,
        provide_context=True,
    )

    load_to_redshift = PythonOperator(
        task_id="load_tracks_to_redshift",
        python_callable=load_to_redshift,
        provide_context=True,
    )

    #dbt Transformation Task Group
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
            bash_command=f'export PATH=$PATH:/home/airflow/.local/bin && cd {DBT_PROJECT_DIR} && dbt run -m spotify_mrt__tracks --profiles-dir {DBT_PROFILES_DIR}',
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
    process_album_ids >> extract_to_s3 >> load_to_redshift >> dbt_group