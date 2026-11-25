#!/usr/bin/env bash
# NOT RUN in the project's CI. Runs the PySpark batch job on Dataproc Serverless with Delta Lake tables.
set -euo pipefail
: "${PROJECT:?}"; : "${BUCKET:?}"; REGION="${REGION:-us-central1}"
zip -qr /tmp/lakehouse_bridge.zip src/lakehouse_bridge
gsutil cp /tmp/lakehouse_bridge.zip "gs://$BUCKET/code/"
gcloud dataproc batches submit pyspark src/lakehouse_bridge/pipelines/batch.py \
  --region "$REGION" --deps-bucket "$BUCKET" --py-files "gs://$BUCKET/code/lakehouse_bridge.zip" \
  --properties "spark.jars.packages=io.delta:delta-spark_2.12:3.1.0" \
  --version 2.2 -- 
# Environment for the job: LB_FORMAT=delta LB_DATA=gs://$BUCKET  (see README "Cloud mode")
