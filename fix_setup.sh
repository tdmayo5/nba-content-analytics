#!/bin/bash

echo "🔧 Fixing NBA Analytics Setup"
echo "============================="

# Function to create directory if it doesn't exist
create_dir() {
    if [ ! -d "$1" ]; then
        echo "📁 Creating directory: $1"
        mkdir -p "$1"
    else
        echo "✅ Directory exists: $1"
    fi
}

# Create required directories
echo "📂 Creating required directories..."
create_dir "airflow"
create_dir "airflow/dags" 
create_dir "airflow/plugins"
create_dir "airflow/config"
create_dir "data_generators"
create_dir "dbt"
create_dir "dbt/models"
create_dir "dbt/models/bronze"
create_dir "dbt/models/silver" 
create_dir "dbt/models/gold"
create_dir "dashboard"
create_dir "scripts"
create_dir "data"

# Create __init__.py files for Python modules
echo "🐍 Creating Python module files..."
touch data_generators/__init__.py
touch airflow/__init__.py
touch airflow/dags/__init__.py

# Create simple requirements.txt if it doesn't exist
if [ ! -f "requirements.txt" ]; then
    echo "📝 Creating requirements.txt..."
    cat > requirements.txt << 'EOF'
# Core data processing
pandas==2.0.3
numpy==1.24.3
requests==2.31.0

# Database connections  
psycopg2-binary==2.9.7
sqlalchemy==2.0.19

# Airflow
apache-airflow[postgres]==2.7.1

# dbt
dbt-postgres==1.6.0

# Dashboard
streamlit==1.28.0
plotly==5.15.0

# Data science
scikit-learn==1.3.0
textblob==0.17.1

# Development tools
jupyter==1.0.0
black==23.7.0
pylint==2.17.5
EOF
else
    echo "✅ requirements.txt already exists"
fi

# Create basic docker-compose.yml if it doesn't exist
if [ ! -f "docker-compose.yml" ]; then
    echo "🐳 Creating docker-compose.yml..."
    cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  # PostgreSQL Database
  postgres:
    image: postgres:13
    container_name: nba_postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts:/docker-entrypoint-initdb.d/
    ports:
      - "5432:5432"
    networks:
      - nba_network
    restart: unless-stopped

  # Redis for Airflow
  redis:
    image: redis:7-alpine
    container_name: nba_redis
    networks:
      - nba_network
    restart: unless-stopped

  # Airflow Webserver
  airflow-webserver:
    image: apache/airflow:2.7.1-python3.9
    container_name: nba_airflow_webserver
    command: webserver
    depends_on:
      - postgres
      - redis
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: false
      AIRFLOW__CORE__LOAD_EXAMPLES: false
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: true
      PYTHONPATH: /opt/airflow:/opt/airflow/data_generators
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./data_generators:/opt/airflow/data_generators
      - ./data:/opt/airflow/data
      - ./requirements.txt:/requirements.txt
    ports:
      - "8080:8080"
    networks:
      - nba_network
    restart: unless-stopped
    user: "50000:0"

  # Airflow Scheduler  
  airflow-scheduler:
    image: apache/airflow:2.7.1-python3.9
    container_name: nba_airflow_scheduler
    command: scheduler
    depends_on:
      - postgres
      - redis
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      PYTHONPATH: /opt/airflow:/opt/airflow/data_generators
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./data_generators:/opt/airflow/data_generators  
      - ./data:/opt/airflow/data
      - ./requirements.txt:/requirements.txt
    networks:
      - nba_network
    restart: unless-stopped
    user: "50000:0"

  # Streamlit Dashboard
  dashboard:
    image: python:3.9-slim
    container_name: nba_dashboard
    depends_on:
      - postgres
    volumes:
      - ./dashboard:/app
      - ./data:/app/data
    ports:
      - "8501:8501"
    environment:
      - POSTGRES_HOST=postgres
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=nba_analytics
    networks:
      - nba_network
    restart: unless-stopped
    working_dir: /app
    command: >
      bash -c "
        pip install streamlit plotly pandas psycopg2-binary &&
        streamlit run nba_dashboard.py --server.port=8501 --server.address=0.0.0.0
      "

volumes:
  postgres_data:

networks:
  nba_network:
    driver: bridge
EOF
else
    echo "✅ docker-compose.yml already exists"
fi

# Create database initialization script
if [ ! -f "scripts/init_db.sql" ]; then
    echo "🗄️ Creating database initialization script..."
    create_dir "scripts"
    cat > scripts/init_db.sql << 'EOF'
-- Create NBA Analytics database
CREATE DATABASE IF NOT EXISTS nba_analytics;

-- Connect to NBA Analytics database  
\c nba_analytics;

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze_nba;
CREATE SCHEMA IF NOT EXISTS silver_nba;
CREATE SCHEMA IF NOT EXISTS gold_nba;

-- Create bronze layer tables
CREATE TABLE IF NOT EXISTS bronze_nba.raw_games (
    game_id VARCHAR(20) PRIMARY KEY,
    game_date DATE NOT NULL,
    home_team_id VARCHAR(3) NOT NULL,
    visitor_team_id VARCHAR(3) NOT NULL,
    home_team_score INTEGER,
    visitor_team_score INTEGER,
    game_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_nba.raw_social_data (
    tweet_id BIGINT PRIMARY KEY,
    game_hashtag VARCHAR(50),
    text TEXT,
    created_at TIMESTAMP,
    user_followers INTEGER,
    retweet_count INTEGER,
    favorite_count INTEGER,
    sentiment_polarity DECIMAL(3,2),
    sentiment_subjectivity DECIMAL(3,2)
);

CREATE TABLE IF NOT EXISTS bronze_nba.raw_tv_ratings (
    rating_id SERIAL PRIMARY KEY,
    game_date DATE NOT NULL,
    home_team VARCHAR(3) NOT NULL,
    away_team VARCHAR(3) NOT NULL,
    tv_rating DECIMAL(4,2),
    estimated_viewers DECIMAL(5,2),
    is_weekend BOOLEAN,
    is_primetime BOOLEAN,
    day_of_week VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold_nba TO airflow;
EOF
else
    echo "✅ Database init script already exists"
fi

echo ""
echo "🎯 Next steps:"
echo "1. Save the simple DAG to airflow/dags/nba_simple_test.py"
echo "2. Stop current containers: docker-compose down"
echo "3. Restart with: docker-compose up -d"
echo "4. Wait 30 seconds for containers to start"
echo "5. Check Airflow at http://localhost:8080"
echo "6. Look for the 'nba_simple_test' DAG"
echo ""
echo "📋 If issues persist, run the troubleshooting script first!"