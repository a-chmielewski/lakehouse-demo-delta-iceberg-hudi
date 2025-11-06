aws iam put-role-policy `
  --role-name emr-serverless-demo-exec `
  --policy-name emr-serverless-exec-inline `
  --policy-document file://infra/emr-serverless-exec-policy.json
