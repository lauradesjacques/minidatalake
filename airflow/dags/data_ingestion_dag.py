from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Add the project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(project_root)

# Import ingestion scripts
from src.ingestion.ingest_corona import main as ingest_corona
from src.ingestion.ingest_youtube import main as ingest_youtube

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Define the DAG
dag = DAG(
    'data_lake_ingestion',
    default_args=default_args,
    description='Hourly data ingestion for COVID-19 and YouTube data',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['data_lake', 'ingestion'],
)

# Task to ingest COVID-19 data
def corona_ingestion_task():
    try:
        logger.info("Starting COVID-19 data ingestion")
        ingest_corona()
        logger.info("COVID-19 data ingestion completed successfully")
    except Exception as e:
        logger.error(f"Error in COVID-19 data ingestion: {str(e)}")
        raise

# Task to ingest YouTube data
def youtube_ingestion_task():
    try:
        logger.info("Starting YouTube data ingestion")
        ingest_youtube()
        logger.info("YouTube data ingestion completed successfully")
    except Exception as e:
        logger.error(f"Error in YouTube data ingestion: {str(e)}")
        raise

# Define the tasks
corona_task = PythonOperator(
    task_id='ingest_corona_data',
    python_callable=corona_ingestion_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=5),
)

youtube_task = PythonOperator(
    task_id='ingest_youtube_data',
    python_callable=youtube_ingestion_task,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=5),
)

# Set task dependencies
# We can run them in parallel since they're independent
corona_task >> youtube_task  # Run YouTube ingestion after COVID-19 ingestion
# If you want them to run in parallel, remove the >> operator 