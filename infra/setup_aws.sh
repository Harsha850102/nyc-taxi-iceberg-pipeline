#!/usr/bin/env bash
# Creates the AWS resources for the pipeline. Safe to re-run.
#
# Run it with an ADMIN profile (an IAM Identity Center / IAM admin user),
# NEVER with root credentials:
#     AWS_PROFILE=my-admin bash infra/setup_aws.sh
#
# Creates:
#   * private, encrypted S3 bucket            ${S3_BUCKET}
#   * IAM user taxi-pipeline-user             -> used by ingest + Spark (S3 raw/ + warehouse/ only)
#   * IAM user taxi-polaris-service-user      -> used by the Polaris container; can ONLY assume the role below
#   * IAM role taxi-polaris-storage-role      -> what Polaris uses to write Iceberg metadata in warehouse/
#   * IAM role taxi-snowflake-read-role       -> read-only on warehouse/ for Snowflake (trust fixed later)
#
# It does NOT create access keys (so no secret ever lands in a log). Create them
# yourself afterwards - see docs/RUNBOOK.md step 3.
set -euo pipefail

cd "$(dirname "$0")"
: "${S3_BUCKET:?Set S3_BUCKET (e.g. in .env)}"
: "${AWS_REGION:?Set AWS_REGION (e.g. us-east-1)}"
CATALOG_TYPE="${CATALOG_TYPE:-polaris}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
CALLER=$(aws sts get-caller-identity --query Arn --output text)
if [[ "$CALLER" == *":root" ]]; then
  echo "Refusing to run as the root user. Use an IAM admin user or SSO role." >&2
  exit 1
fi
echo "Account: $ACCOUNT_ID   Caller: $CALLER   Region: $AWS_REGION   Bucket: $S3_BUCKET"

render() {  # fill ${VARS} in a policy template
  sed -e "s|\${S3_BUCKET}|${S3_BUCKET}|g" \
      -e "s|\${ACCOUNT_ID}|${ACCOUNT_ID}|g" \
      -e "s|\${AWS_REGION}|${AWS_REGION}|g" \
      -e "s|\${SNOWFLAKE_IAM_USER_ARN}|${SNOWFLAKE_IAM_USER_ARN:-arn:aws:iam::${ACCOUNT_ID}:root}|g" \
      -e "s|\${SNOWFLAKE_EXTERNAL_ID}|${SNOWFLAKE_EXTERNAL_ID:-placeholder-until-snowflake-step}|g" \
      "policies/$1"
}

# ---------------------------------------------------------------- S3 bucket
if aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
  echo "Bucket exists: $S3_BUCKET"
else
  if [[ "$AWS_REGION" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION"
  else
    aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION" \
      --create-bucket-configuration "LocationConstraint=$AWS_REGION"
  fi
  echo "Created bucket: $S3_BUCKET"
fi
aws s3api put-public-access-block --bucket "$S3_BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$S3_BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
echo "Bucket is private (public access blocked) and encrypted (SSE-S3)."

# ---------------------------------------------------------------- IAM users
ensure_user() {
  aws iam get-user --user-name "$1" >/dev/null 2>&1 || aws iam create-user --user-name "$1" >/dev/null
  echo "IAM user ready: $1"
}
ensure_user taxi-pipeline-user
ensure_user taxi-polaris-service-user

aws iam put-user-policy --user-name taxi-pipeline-user --policy-name taxi-pipeline-s3 \
  --policy-document "$(render pipeline-user-policy.json)"
if [[ "$CATALOG_TYPE" == "glue" ]]; then
  aws iam put-user-policy --user-name taxi-pipeline-user --policy-name taxi-pipeline-glue \
    --policy-document "$(render glue-catalog-policy.json)"
  echo "Attached Glue Data Catalog permissions (CATALOG_TYPE=glue)."
fi
aws iam put-user-policy --user-name taxi-polaris-service-user --policy-name assume-polaris-storage-role \
  --policy-document "$(render polaris-service-user-policy.json)"

# ---------------------------------------------------------------- IAM roles
ensure_role() {  # name trust-file policy-file
  if aws iam get-role --role-name "$1" >/dev/null 2>&1; then
    aws iam update-assume-role-policy --role-name "$1" --policy-document "$(render "$2")"
  else
    sleep 10  # IAM is eventually consistent: a brand-new user may not be usable in a trust policy yet
    aws iam create-role --role-name "$1" --assume-role-policy-document "$(render "$2")" >/dev/null
  fi
  aws iam put-role-policy --role-name "$1" --policy-name "${1}-s3" --policy-document "$(render "$3")"
  echo "IAM role ready: arn:aws:iam::${ACCOUNT_ID}:role/$1"
}
ensure_role taxi-polaris-storage-role polaris-role-trust.json polaris-role-policy.json
ensure_role taxi-snowflake-read-role snowflake-role-trust.json snowflake-role-policy.json

cat <<EOF

Done. Put these in your .env:
  POLARIS_STORAGE_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/taxi-polaris-storage-role
  SNOWFLAKE_ROLE_ARN=arn:aws:iam::${ACCOUNT_ID}:role/taxi-snowflake-read-role

Next: create access keys for taxi-pipeline-user and taxi-polaris-service-user (docs/RUNBOOK.md step 3).
After the Snowflake DESC EXTERNAL VOLUME step, run:
  SNOWFLAKE_IAM_USER_ARN=... SNOWFLAKE_EXTERNAL_ID=... bash infra/setup_aws.sh
to lock the Snowflake role's trust policy to Snowflake's IAM user.
EOF
