#!/bin/bash
# Script to deploy the MCP server to Google Cloud Run

# Exit on error
set -e

# Check if dataset and table IDs are provided
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <dataset_id> <table_id> [service_account]"
  echo "Example: $0 reporting_dataset actions_table"
  exit 1
fi

# Set variables
DATASET_ID=$1
TABLE_ID=$2
SERVICE_ACCOUNT=${3:-""}  # Optional service account

# Build the Docker image
echo "Building Docker image..."
IMAGE_NAME="reporting-mcp:$(date +%Y%m%d-%H%M%S)"
docker build -t $IMAGE_NAME -f cloudbuild.Dockerfile .

# Get the current project ID
PROJECT_ID=$(gcloud config get-value project)
if [ -z "$PROJECT_ID" ]; then
  echo "No Google Cloud project is set. Please run 'gcloud config set project YOUR_PROJECT_ID'"
  exit 1
fi

# Tag the image for Google Container Registry
GCR_IMAGE="gcr.io/$PROJECT_ID/$IMAGE_NAME"
docker tag $IMAGE_NAME $GCR_IMAGE

# Push the image to Google Container Registry
echo "Pushing image to Google Container Registry..."
docker push $GCR_IMAGE

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
DEPLOY_CMD="gcloud run deploy reporting-mcp \
  --image $GCR_IMAGE \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT_ID=$PROJECT_ID,BIGQUERY_DATASET_ID=$DATASET_ID,BIGQUERY_TABLE_ID=$TABLE_ID"

# Add service account if provided
if [ ! -z "$SERVICE_ACCOUNT" ]; then
  DEPLOY_CMD="$DEPLOY_CMD --service-account $SERVICE_ACCOUNT"
fi

# Execute the deployment command
eval $DEPLOY_CMD

echo "Deployment complete! Your MCP server is now running on Cloud Run."
echo "You can access it at the URL provided above."
