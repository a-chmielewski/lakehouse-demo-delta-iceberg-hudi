#!/usr/bin/env bash
set -euo pipefail
source .env
aws s3 sync /tmp/tlc/ s3://$S3_BUCKET/$RAW_PREFIX/ --profile $AWS_PROFILE
