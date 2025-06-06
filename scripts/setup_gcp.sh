#!/bin/bash

# GCP Project Setup Script
echo "🚀 Setting up GCP project for NBA Analytics Platform..."

# Variables
PROJECT_ID="nba-analytics-free"
REGION="us-central1"
ZONE="us-central1-a"

# Login and create project
echo "📝 Creating GCP project..."
gcloud auth login
gcloud projects create $PROJECT_ID --name="NBA Analytics Platform"
gcloud config set project $PROJECT_ID

# Enable billing (required for some services)
echo "💳 Note: Enable billing in console for BigQuery and other services"

# Enable required APIs
echo "🔧 Enabling required APIs..."
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable compute.googleapis.com

# Create service account
echo "👤 Creating service account..."
gcloud iam service-accounts create nba-analytics \
    --description="NBA Analytics Service Account" \
    --display-name="NBA Analytics"

# Grant necessary permissions
echo "🔑 Granting permissions..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:nba-analytics@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:nba-analytics@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Download service account key
gcloud iam service-accounts keys create ~/nba-analytics-key.json \
    --iam-account=nba-analytics@$PROJECT_ID.iam.gserviceaccount.com

echo "✅ GCP setup complete!"
echo "📁 Service account key saved to: ~/nba-analytics-key.json"
echo "🌐 Next: Set up BigQuery datasets and Cloud Storage bucket"
