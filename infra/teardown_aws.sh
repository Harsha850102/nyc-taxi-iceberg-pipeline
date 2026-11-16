#!/usr/bin/env bash
# Deletes EVERYTHING setup_aws.sh created, including all data in the bucket.
# Run only after the reviewers have seen your work:
#     AWS_PROFILE=my-admin bash infra/teardown_aws.sh
set -euo pipefail
: "${S3_BUCKET:?Set S3_BUCKET}"

read -r -p "This permanently deletes s3://${S3_BUCKET} and the taxi-* IAM users/roles. Type the bucket name to confirm: " answer
[[ "$answer" == "$S3_BUCKET" ]] || { echo "Aborted."; exit 1; }

aws s3 rm "s3://${S3_BUCKET}" --recursive || true
aws s3api delete-bucket --bucket "$S3_BUCKET" || true

for user in taxi-pipeline-user taxi-polaris-service-user; do
  for key in $(aws iam list-access-keys --user-name "$user" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do
    aws iam delete-access-key --user-name "$user" --access-key-id "$key"
  done
  for pol in $(aws iam list-user-policies --user-name "$user" --query 'PolicyNames' --output text 2>/dev/null); do
    aws iam delete-user-policy --user-name "$user" --policy-name "$pol"
  done
  aws iam delete-user --user-name "$user" 2>/dev/null && echo "Deleted user $user" || true
done

for role in taxi-polaris-storage-role taxi-snowflake-read-role; do
  for pol in $(aws iam list-role-policies --role-name "$role" --query 'PolicyNames' --output text 2>/dev/null); do
    aws iam delete-role-policy --role-name "$role" --policy-name "$pol"
  done
  aws iam delete-role --role-name "$role" 2>/dev/null && echo "Deleted role $role" || true
done

# Glue fallback objects (only exist if you used CATALOG_TYPE=glue)
aws glue delete-database --name nyc_taxi 2>/dev/null && echo "Deleted Glue database nyc_taxi" || true

echo "AWS teardown complete. In Snowflake run:  DROP DATABASE TAXI_DB; DROP EXTERNAL VOLUME taxi_s3_vol;"
echo "DROP CATALOG INTEGRATION taxi_iceberg_files; DROP WAREHOUSE TAXI_WH;"
