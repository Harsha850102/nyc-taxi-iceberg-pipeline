#!/usr/bin/env bash
# One-time setup of the local Polaris server (re-runnable):
#   1. get an OAuth token for the root principal
#   2. create catalog "$POLARIS_CATALOG" whose storage is s3://$S3_BUCKET/warehouse
#   3. give the catalog_admin role permission to manage tables (granted to service_admin -> root)
#
# Note: the default Polaris Docker image keeps its catalog in memory. If you restart
# the container, run this script again and re-run the Spark jobs (the data in S3 is safe).
set -euo pipefail
: "${S3_BUCKET:?}" "${AWS_REGION:?}" "${POLARIS_STORAGE_ROLE_ARN:?}" "${POLARIS_ROOT_CLIENT_ID:?}" "${POLARIS_ROOT_CLIENT_SECRET:?}"
POLARIS_HOST="${POLARIS_HOST:-http://localhost:8181}"
CATALOG="${POLARIS_CATALOG:-taxi_catalog}"

echo "Waiting for Polaris at $POLARIS_HOST ..."
for i in $(seq 1 30); do
  curl -sf -o /dev/null "http://localhost:8182/q/health" && break
  sleep 2
done

TOKEN=$(curl -sf -X POST "$POLARIS_HOST/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id="$POLARIS_ROOT_CLIENT_ID" \
  -d client_secret="$POLARIS_ROOT_CLIENT_SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

code=$(curl -s -o /dev/null -w "%{http_code}" "${AUTH[@]}" "$POLARIS_HOST/api/management/v1/catalogs/$CATALOG")
if [[ "$code" == "200" ]]; then
  echo "Catalog $CATALOG already exists"
else
  curl -sf -X POST "${AUTH[@]}" "$POLARIS_HOST/api/management/v1/catalogs" -d @- <<JSON
{
  "catalog": {
    "name": "$CATALOG",
    "type": "INTERNAL",
    "properties": {"default-base-location": "s3://$S3_BUCKET/warehouse"},
    "storageConfigInfo": {
      "storageType": "S3",
      "roleArn": "$POLARIS_STORAGE_ROLE_ARN",
      "region": "$AWS_REGION",
      "allowedLocations": ["s3://$S3_BUCKET/warehouse"]
    }
  }
}
JSON
  echo "Created catalog $CATALOG -> s3://$S3_BUCKET/warehouse"
fi

curl -sf -X PUT "${AUTH[@]}" \
  "$POLARIS_HOST/api/management/v1/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" \
  -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}'
echo "Granted CATALOG_MANAGE_CONTENT on $CATALOG to catalog_admin"

echo "Catalogs:"
curl -sf "${AUTH[@]}" "$POLARIS_HOST/api/management/v1/catalogs" | python3 -m json.tool
