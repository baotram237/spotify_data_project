import pandas as pd
import io
import logging
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class s3Handler:
    """Class for handling S3 operations."""
    def __init__(self, aws_conn_id='aws_conn'):
        self.aws_conn_id = aws_conn_id
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @staticmethod
    def ensure_compatible_dtypes(df):
        """Ensure compatible data types in pyarrow table for the DataFrame."""
        df.reset_index(drop=True, inplace=True)
        for col in df.columns:
            try:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
                elif pd.api.types.is_integer_dtype(df[col]):
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                elif pd.api.types.is_float_dtype(df[col]):
                    df[col] = df[col].astype('float32')
                elif pd.api.types.is_bool_dtype(df[col]):
                    df[col] = df[col].astype('bool')
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif pd.api.types.is_string_dtype(df[col]):
                    df[col] = df[col].astype(str)
                return df
            except Exception as e:
                logging.error(f"Error converting column {col}: {e}")
                raise

    def upload_df_to_s3(self, df, bucket_name, s3_key, context=None):
        """
        Upload DataFrame to S3 as Parquet file
        """
        try:
            if df is None or df.empty:
                logging.warning("DataFrame is empty, skipping upload")
                return None
            
            # Add metadata if context is provided
            if context:
                df["extracted_at"] = datetime.now()
                df["extraction_date"] = context.get("ds")
            df = self.ensure_compatible_dtypes(df)
            
            # Save Parquet to buffer
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine="pyarrow", compression="snappy", index=False)
            parquet_buffer.seek(0)
            
            # Upload to S3
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
            s3_hook.load_bytes(
                bytes_data=parquet_buffer.getvalue(),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True,
            )
            
            logging.info(f"Uploaded Parquet file to s3://{bucket_name}/{s3_key}")
            
            return {
                "s3_key": s3_key,
                "s3_path": f"s3://{bucket_name}/{s3_key}",
                "record_count": len(df),
                "file_format": "parquet",
            }
            
        except Exception as e:
            logging.error(f"Error uploading to S3: {str(e)}")
            raise
    