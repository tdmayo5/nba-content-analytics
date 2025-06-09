"""
Simple NBA Analytics DAG for troubleshooting
This basic DAG helps identify and fix common setup issues
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'nba_simple_test',
    default_args=default_args,
    description='Simple test DAG for NBA analytics setup',
    schedule_interval=None,  # Manual trigger only
    max_active_runs=1,
    tags=['nba', 'test', 'troubleshooting']
)

def test_python_environment(**context):
    """Test basic Python functionality"""
    import sys
    import os
    
    logger.info("=== Python Environment Test ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info("Python path:")
    for path in sys.path:
        logger.info(f"  {path}")
    
    # Test basic imports
    try:
        import pandas as pd
        logger.info(f"✅ Pandas version: {pd.__version__}")
    except ImportError as e:
        logger.error(f"❌ Pandas import failed: {e}")
    
    try:
        import requests
        logger.info(f"✅ Requests available")
    except ImportError as e:
        logger.error(f"❌ Requests import failed: {e}")
    
    return "Python environment test completed"

def test_database_connection(**context):
    """Test database connectivity"""
    import psycopg2
    
    logger.info("=== Database Connection Test ===")
    
    try:
        # Try to connect to the database
        conn = psycopg2.connect(
            host='postgres',
            database='nba_analytics',
            user='airflow',
            password='airflow',
            connect_timeout=10
        )
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"✅ Database connected successfully")
        logger.info(f"PostgreSQL version: {version[0]}")
        
        # Check if our schemas exist
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('bronze_nba', 'silver_nba', 'gold_nba')
        """)
        schemas = cursor.fetchall()
        logger.info(f"NBA schemas found: {[s[0] for s in schemas]}")
        
        cursor.close()
        conn.close()
        
        return "Database connection test passed"
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

def test_data_generation(**context):
    """Test basic data generation without external dependencies"""
    import pandas as pd
    import random
    from datetime import date
    
    logger.info("=== Data Generation Test ===")
    
    # Generate simple test data
    teams = ['LAL', 'GSW', 'BOS', 'MIL', 'PHX', 'BRK', 'MIA', 'PHI']
    
    # Create test games data
    games_data = []
    for i in range(5):
        home_team = random.choice(teams)
        away_team = random.choice([t for t in teams if t != home_team])
        
        games_data.append({
            'game_id': f"TEST{date.today().strftime('%Y%m%d')}{i:02d}",
            'game_date': date.today(),
            'home_team_id': home_team,
            'visitor_team_id': away_team,
            'home_team_score': random.randint(95, 125),
            'visitor_team_score': random.randint(95, 125),
            'game_status': 'Final'
        })
    
    games_df = pd.DataFrame(games_data)
    logger.info(f"✅ Generated {len(games_df)} test games")
    logger.info(f"Sample data:\n{games_df.head().to_string()}")
    
    return f"Generated {len(games_df)} test games"

# Define tasks
test_python_task = PythonOperator(
    task_id='test_python_environment',
    python_callable=test_python_environment,
    dag=dag,
)

test_db_task = PythonOperator(
    task_id='test_database_connection',
    python_callable=test_database_connection,
    dag=dag,
)

test_data_task = PythonOperator(
    task_id='test_data_generation',
    python_callable=test_data_generation,
    dag=dag,
)

test_bash_task = BashOperator(
    task_id='test_bash_commands',
    bash_command='''
    echo "=== Bash Environment Test ==="
    echo "Current user: $(whoami)"
    echo "Current directory: $(pwd)"
    echo "Available disk space:"
    df -h /
    echo "Memory usage:"
    free -h
    echo "Docker containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || echo "Docker not available in container"
    ''',
    dag=dag,
)

# Set dependencies
test_python_task >> test_db_task >> test_data_task >> test_bash_task

# Add documentation
dag.doc_md = """
# NBA Analytics - Troubleshooting DAG

This simple DAG helps diagnose common setup issues:

## Tasks:
1. **Python Environment Test**: Verifies Python setup and imports
2. **Database Connection Test**: Tests PostgreSQL connectivity  
3. **Data Generation Test**: Creates sample data without external APIs
4. **Bash Commands Test**: Verifies system environment

## Usage:
- Trigger this DAG manually to test your setup
- Check task logs for detailed diagnostic information
- Use this to verify everything works before running the main pipeline

## Troubleshooting:
- If Python test fails: Check Docker container setup
- If database test fails: Verify PostgreSQL container is running
- If data test fails: Check pandas installation
- If bash test fails: Check container permissions
"""