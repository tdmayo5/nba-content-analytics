#!/bin/bash

echo "🏀 Setting up NBA Analytics Dashboard"
echo "===================================="

# Create dashboard directory if it doesn't exist
if [ ! -d "dashboard" ]; then
    echo "📁 Creating dashboard directory..."
    mkdir dashboard
fi

# Create dashboard requirements
echo "📝 Creating dashboard requirements..."
cat > dashboard/requirements.txt << 'EOF'
streamlit==1.28.0
plotly==5.15.0
pandas==2.0.3
psycopg2-binary==2.9.7
numpy==1.24.3
EOF

# Create simple dashboard launcher script
cat > run_dashboard.sh << 'EOF'
#!/bin/bash
echo "🚀 Starting NBA Analytics Dashboard..."
echo "Dashboard will be available at: http://localhost:8501"
echo "Press Ctrl+C to stop"
echo ""

# Check if running in Docker
if [ -f /.dockerenv ]; then
    # Running inside Docker container
    cd /app
    pip install -r requirements.txt
    streamlit run nba_dashboard.py --server.port=8501 --server.address=0.0.0.0
else
    # Running locally
    cd dashboard
    pip install -r requirements.txt
    streamlit run nba_dashboard.py --server.port=8501 --server.address=0.0.0.0
fi
EOF

chmod +x run_dashboard.sh

echo ""
echo "✅ Dashboard setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Save the NBA Dashboard code to: dashboard/nba_dashboard.py"
echo "2. Run the dashboard with: ./run_dashboard.sh"
echo "3. Or use Docker: docker-compose up dashboard"
echo "4. Open browser to: http://localhost:8501"
echo ""
echo "🎯 You should see:"
echo "   • Interactive charts and graphs"
echo "   • TV ratings analysis"
echo "   • Social media metrics"
echo "   • Content recommendations"
echo "   • Team performance analytics"