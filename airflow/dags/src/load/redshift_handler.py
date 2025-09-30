import pandas as pd
import io
import logging
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

class redshiftHandler:
    """Class for handling Redshift operations."""
    def __init__(self, redshift_conn_id='redshift_conn'):
        self.redshift_conn_id = redshift_conn_id
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_query(self, query):
        """Execute a query in Redshift"""
        try:           
            logging.info(f"Execute query in Redshift table")
            
            # Get Redshift connection
            redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            conn = redshift_hook.get_conn()
            df = pd.read_sql(query, conn)
            print(f"Fetched {len(df)} albums from Redshift")
            return df
        
        except Exception as e:
            logging.error(f"Error during full load to Redshift: {str(e)}")
            raise


    def full_load_to_redshift(self, aws_conn_id, s3_bucket, s3_key, redshift_table, serialize: bool = False):
        """
        Full load from S3 to Redshift (truncate and load)
        """
        try:           
            logging.info(f"Full load: Loading records from s3://{s3_bucket}/{s3_key} to {redshift_table}")
            
            # Get Redshift connection
            redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            
            # Full load approach: Truncate entire table
            truncate_sql = f"TRUNCATE TABLE {redshift_table}"
            redshift_hook.run(truncate_sql)
            logging.info(f"Table {redshift_table} truncated for full load")
            
            # Copy all data from S3 to Redshift
            s3_hook = S3Hook(aws_conn_id=aws_conn_id)
            credentials = s3_hook.get_credentials()

            if serialize:
                copy_sql = f"""
                COPY {redshift_table}
                FROM 's3://{s3_bucket}/{s3_key}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                FORMAT AS PARQUET
                SERIALIZETOJSON;
                """
            else:
                copy_sql = f"""
                COPY {redshift_table}
                FROM 's3://{s3_bucket}/{s3_key}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                FORMAT AS PARQUET;
                """                

            redshift_hook.run(copy_sql)
            logging.info("Data copied successfully to Redshift")
            
            # Verify data was loaded
            count_sql = f"SELECT COUNT(*) FROM {redshift_table}"
            loaded_count = redshift_hook.get_first(count_sql)[0]
            
            logging.info(f"Full load completed: {loaded_count} records loaded to {redshift_table}")
            
            if loaded_count == 0:
                raise ValueError("No records were loaded to Redshift")
            
            return {
                "redshift_table": redshift_table,
                "records_loaded": loaded_count,
                "load_type": "full_load"
            }
            
        except Exception as e:
            logging.error(f"Error during full load to Redshift: {str(e)}")
            raise