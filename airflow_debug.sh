#!/bin/bash

echo "🔍 NBA Analytics Airflow Troubleshooting"
echo "========================================"

# Check if we're in the right directory
echo "📁 Current directory:"
pwd
echo ""

# Check project structure
echo "📂 Project structure:"
find . -name "*.py" -path "*/airflow/dags/*" 2>/dev/null | head -10
echo ""

# Check if DAG files exist
echo "🐍 DAG files in airflow/dags:"
ls -la airflow/dags/ 2>/dev/null || echo "❌ airflow/dags directory not found"
echo ""

# Check Docker containers
echo "🐳 Docker containers status:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

# Check Airflow container logs for errors
echo "📋 Recent Airflow scheduler logs:"
docker logs nba_airflow_scheduler --tail 20 2>/dev/null || echo "❌ Scheduler container not found"
echo ""

# Check if data generators directory exists
echo "📊 Data generators:"
ls -la data_generators/ 2>/dev/null || echo "❌ data_generators directory not found"
echo ""

# Test database connection
echo "🗄️ Testing database connection:"
docker exec nba_postgres psql -U airflow -d nba_analytics -c "\dt bronze_nba.*" 2>/dev/null || echo "❌ Cannot connect to database"
echo ""

# Check Python path issues
echo "🐍 Python import test:"
docker exec nba_airflow_scheduler python -c "
import sys
print('Python path:')
for p in sys.path:
    print(f'  {p}')
    
try:
    import data_generators.nba_api
    print('✅ data_generators.nba_api imported successfully')
except ImportError as e:
    print(f'❌ Import error: {e}')
" 2>/dev/null || echo "❌ Cannot test imports"