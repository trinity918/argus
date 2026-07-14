#!/usr/bin/env bash
# Deploy the Argus stack to Azure Container Apps.
#
# Usage:
#   ./deploy.sh <resource-group> [location] [feedMode]
#     resource-group  target resource group (created if missing)
#     location        Azure region              (default: eastus)
#     feedMode        demo | live               (default: demo)
#
# Requires: az CLI, logged in (az login), subscription selected.
# Runs in two phases because the app images live in the ACR this deployment
# creates: infra first, then `az acr build` (cloud-side build — no local
# Docker needed), then the container apps.
set -euo pipefail

RG="${1:?usage: deploy.sh <resource-group> [location] [feedMode]}"
LOCATION="${2:-eastus}"
FEED_MODE="${3:-demo}"
TAG="${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}"
BICEP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$BICEP_DIR/../.." && pwd)"

echo "==> resource group: $RG ($LOCATION), feed mode: $FEED_MODE, image tag: $TAG"

echo "==> [1/4] ensuring resource group"
az group create --name "$RG" --location "$LOCATION" --output none

echo "==> [2/4] deploying infrastructure (registry, environment, storage, identity)"
ACR_NAME=$(az deployment group create \
  --resource-group "$RG" \
  --template-file "$BICEP_DIR/main.bicep" \
  --parameters deployApps=false \
  --query 'properties.outputs.acrName.value' -o tsv)
echo "    registry: $ACR_NAME"

echo "==> [3/4] building images in ACR (cloud-side; no local docker required)"
az acr build --registry "$ACR_NAME" --image "argus:$TAG" "$REPO_ROOT" --output none
az acr build --registry "$ACR_NAME" --image "argus-ml:$TAG" "$REPO_ROOT/ml" --output none

echo "==> [4/4] deploying container apps"
DASHBOARD=$(az deployment group create \
  --resource-group "$RG" \
  --template-file "$BICEP_DIR/main.bicep" \
  --parameters deployApps=true imageTag="$TAG" feedMode="$FEED_MODE" \
  --query 'properties.outputs.dashboardUrl.value' -o tsv)

echo ""
echo "✓ Argus deployed."
echo "  dashboard: $DASHBOARD"
echo "  teardown:  az group delete --name $RG --yes --no-wait"
