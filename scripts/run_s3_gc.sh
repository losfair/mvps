#!/bin/bash

set -e

export RUST_LOG=info

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export S3_ENDPOINT=http://localhost:8355

cargo run -p mvps-s3-gc -- \
  --bucket mvps-image-store --prefix test/ \
  --threshold-inactive-days 0
