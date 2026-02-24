#!/bin/bash
set -e

# Setup Azure Terraform Backend Storage
# This script creates a Resource Group and Storage Account for Terraform state if they don't exist.

if [ -z "$PREFIX" ] || [ -z "$LOCATION" ] || [ -z "$ARM_SUBSCRIPTION_ID" ]; then
    echo "Missing required environment variables (PREFIX, LOCATION, ARM_SUBSCRIPTION_ID)"
    exit 1
fi

ENVIRONMENT="${ENVIRONMENT:-dev}"

# Login using Service Principal
echo "Logging into Azure..."
az login --service-principal -u "$ARM_CLIENT_ID" -p "$ARM_CLIENT_SECRET" --tenant "$ARM_TENANT_ID" > /dev/null

RG_NAME="rg-${PREFIX}-terraform-state"
# Use a short hash of the subscription ID to ensure global uniqueness
SUB_HASH=$(echo -n "$ARM_SUBSCRIPTION_ID" | sha256sum | cut -c 1-6)
STORAGE_ACCOUNT_NAME="sttfstate${PREFIX}${SUB_HASH}"
CONTAINER_NAME="tfstate"

echo "Configuring Terraform Backend..."
echo "Environment: $ENVIRONMENT"
echo "Resource Group: $RG_NAME"
echo "Storage Account: $STORAGE_ACCOUNT_NAME"
echo "Container: $CONTAINER_NAME"

# Create Resource Group
az group create --name "$RG_NAME" --location "$LOCATION" -o none || true

# Create Storage Account
az storage account create --name "$STORAGE_ACCOUNT_NAME" --resource-group "$RG_NAME" --location "$LOCATION" --sku Standard_LRS --encryption-services blob -o none || true

# Create Blob Container
az storage container create --name "$CONTAINER_NAME" --account-name "$STORAGE_ACCOUNT_NAME" --auth-mode login -o none || true

# Output the configuration for Terraform to use
mkdir -p backend_config
cat > backend_config/backend.hcl << EOF
resource_group_name  = "${RG_NAME}"
storage_account_name = "${STORAGE_ACCOUNT_NAME}"
container_name       = "${CONTAINER_NAME}"
key                  = "${ENVIRONMENT}.terraform.tfstate"
EOF

echo "Backend configuration generated at backend_config/backend.hcl"
