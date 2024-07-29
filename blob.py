import pandas as pd
from azure.storage.blob import ContainerClient
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
azure_logger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azure_logger.setLevel(logging.ERROR)

# Load environment variables from .env file
load_dotenv()

# Database connection details from .env
DB_CONN_STR = (
    f"mssql+pyodbc://{os.getenv('MSSQL_USERNAME')}:{os.getenv('MSSQL_PASSWORD')}@"
    f"{os.getenv('MSSQL_SERVER')}/{os.getenv('MSSQL_DATABASE')}?"
    "driver=ODBC+Driver+17+for+SQL+Server"
)

# Azure Blob Storage connection details from .env
ACCOUNT_URL = os.getenv('ACCOUNT_URL')
CREDENTIAL = os.getenv('CREDENTIAL')
CONTAINER_NAME = os.getenv('BLOB_CONTAINER_NAME')

def get_table_columns(table_name):
    try:
        engine = create_engine(DB_CONN_STR)
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
        df = pd.read_sql(query, engine)
        return df['COLUMN_NAME'].tolist()
    except SQLAlchemyError as e:
        logger.error(f"Error retrieving table columns: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def export_data_to_csv(table_name):
    try:
        engine = create_engine(DB_CONN_STR)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, engine)
        
        csv_file = f"{table_name.lower()}.csv"  # Dynamically create CSV filename
        df.to_csv(csv_file, index=False)
        logger.info("Data exported to CSV successfully.")
        
        return csv_file
    except SQLAlchemyError as e:
        logger.error(f"Error exporting data to CSV: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def upload_to_azure(csv_file, blob_name):
    try:
        client = ContainerClient(account_url=ACCOUNT_URL, container_name=CONTAINER_NAME, credential=CREDENTIAL)
        
        # List existing blobs before upload
        logger.info("Existing blobs before upload:")
        for blob in client.list_blobs():
            logger.info(blob.name)
        
        # Upload the file
        with open(csv_file, "rb") as data:
            client.upload_blob(name=blob_name, data=data, overwrite=True)
        logger.info(f"File '{blob_name}' uploaded to Azure Blob Storage successfully.")
        
        # List existing blobs after upload
        logger.info("Existing blobs after upload:")
        for blob in client.list_blobs():
            logger.info(blob.name)
    except Exception as e:
        logger.error(f"Error uploading file to Azure: {e}")
        raise

if __name__ == "__main__":
    try:
        table_name = 'DHL_SCHEMA'
        
        columns = get_table_columns(table_name)
        
        # Export and upload to Azure Blob Storage
        csv_file = export_data_to_csv(table_name)
        blob_name = f"{table_name.lower()}.csv"  # Use table name as blob name
        upload_to_azure(csv_file, blob_name)
        
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
