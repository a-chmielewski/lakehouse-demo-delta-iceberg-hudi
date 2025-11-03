#!/usr/bin/env bash
source .env
aws s3 ls s3://$S3_BUCKET/ --recursive --human-readable --summarize --profile $AWS_PROFILE
