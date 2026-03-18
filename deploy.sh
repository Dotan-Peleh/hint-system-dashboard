#!/bin/bash
set -e

DASHBOARD_NAME="hint-system-ab-test"
REGION="us-central1"
PROJECT="yotam-395120"
PROJECT_NUMBER="57935720907"

# Ensure we deploy from the script's directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "Deploying: $DASHBOARD_NAME"
echo ""

gcloud beta run deploy $DASHBOARD_NAME \
  --source "$SCRIPT_DIR" \
  --region $REGION \
  --project $PROJECT \
  --no-allow-unauthenticated \
  --iap \
  --memory 1Gi \
  --cpu 1 \
  --timeout 300 \
  --max-instances 3 \
  --service-account 57935720907-compute@developer.gserviceaccount.com \
  --set-env-vars "CLOUD_RUN=true" \
  --quiet

SERVICE_URL=$(gcloud run services describe $DASHBOARD_NAME --region=$REGION --project=$PROJECT --format="value(status.url)")

echo "Configuring IAP access..."
gcloud run services add-iam-policy-binding $DASHBOARD_NAME \
  --region=$REGION \
  --project=$PROJECT \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-iap.iam.gserviceaccount.com" \
  --role="roles/run.invoker" \
  --quiet 2>/dev/null || true

gcloud beta iap web add-iam-policy-binding \
  --member="domain:peerplay.com" \
  --role="roles/iap.httpsResourceAccessor" \
  --region=$REGION \
  --resource-type=cloud-run \
  --service=$DASHBOARD_NAME \
  --project=$PROJECT \
  --quiet 2>/dev/null || true

gcloud beta iap web add-iam-policy-binding \
  --member="domain:peerplay.io" \
  --role="roles/iap.httpsResourceAccessor" \
  --region=$REGION \
  --resource-type=cloud-run \
  --service=$DASHBOARD_NAME \
  --project=$PROJECT \
  --quiet 2>/dev/null || true

echo ""
echo "========================================================================"
echo "DEPLOYMENT SUCCESSFUL!"
echo "========================================================================"
echo "Dashboard URL: $SERVICE_URL"
echo ""
