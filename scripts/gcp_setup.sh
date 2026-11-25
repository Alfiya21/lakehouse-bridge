#!/usr/bin/env bash
# NOT RUN in the project's CI. Creates the cloud resources for the GCP path. Uses the free tier where possible.
set -euo pipefail
: "${PROJECT:?set PROJECT}"; REGION="${REGION:-us-central1}"; BUCKET="${BUCKET:-${PROJECT}-lakehouse-bridge}"
gcloud config set project "$PROJECT"
gcloud services enable bigquery.googleapis.com dataproc.googleapis.com storage.googleapis.com aiplatform.googleapis.com composer.googleapis.com
gsutil mb -l "$REGION" "gs://$BUCKET" || true
bq --location=US mk -d "${PROJECT}:lakehouse_bridge" || true
echo "Landing data -> gs://$BUCKET/landing ; lakehouse -> gs://$BUCKET/lakehouse"
