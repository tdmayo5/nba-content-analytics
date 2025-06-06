"""
NBA Content Analytics Daily Pipeline
Orchestrates data extraction, transformation, and loading for NBA content analysis

This DAG demonstrates:
- External API integration (NBA Stats)
- Data simulation for social media and ratings
- Error handling and data quality checks
- dbt integration for transformations
- Modular, testable code structure
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import logging
import sys
import os

# Add project paths to Python path for imports
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/data_generators')

from data_generators.nba_api import NBADataExtractor
from data_generators.social_simulator import SocialMediaSimulator  
from data_generators.ratings_simulator import TVRatingsSimulator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'nba_daily_analytics_pipeline',
    default_args=default_args,
    description='Daily NBA content analytics data pipeline',
    schedule_interval='0 3 * * *',  # Run at 3 AM daily
    max_active_runs=1,
    tags=['nba', 'analytics', 'daily', 'content']
)

def get_db_connection():
    """
    Get PostgreSQL database connection
    
    Returns:
        psycopg2 connection object
    """
    try:
        return psycopg2.connect(
            host='postgres',
            database='nba_analytics', 
            user='airflow',
            password='airflow',
            connect_timeout=10
        )
    except Exception as e:
        logger.error()
