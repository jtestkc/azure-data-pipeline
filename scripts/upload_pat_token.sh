#!/bin/bash

# =============================================================================
# Upload DATABRICKS_TOKEN to GitHub Secrets
# Run this after terraform apply completes
# =============================================================================

REPO_OWNER="jtestkc"
REPO_NAME="azure-data-pipeline"
SECRET_NAME="DATABRICKS_TOKEN"

echo "========================================================="
echo "  Uploading DATABRICKS_TOKEN to GitHub Secrets         "
echo "========================================================="

# Check if GitHub CLI is installed
if ! command -v gh &> /dev/null; then
    echo "GitHub CLI not found. Installing..."
    # Install based on OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install gh
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
        sudo apt update
        sudo apt install gh
    fi
fi

# Check if logged in to GitHub
echo ""
echo "Checking GitHub authentication..."
gh auth status
if [ $? -ne 0 ]; then
    echo "Not logged in to GitHub. Please run 'gh auth login' first."
    exit 1
fi

# Get PAT token from terraform output
echo ""
echo "Getting PAT token from Terraform output..."
cd terraform

# Check if terraform is initialized
if [ ! -d ".terraform" ]; then
    echo "Running terraform init..."
    terraform init
fi

# Get the PAT token
PAT_TOKEN=$(terraform output -raw databricks_pat_token 2>/dev/null)
if [ -z "$PAT_TOKEN" ]; then
    echo "Failed to get PAT token from Terraform output."
    echo "Make sure Terraform apply has completed successfully."
    cd ..
    exit 1
fi

cd ..

# Upload to GitHub secrets
echo ""
echo "Uploading $SECRET_NAME to GitHub secrets..."

echo "$PAT_TOKEN" | gh secret set $SECRET_NAME --repo $REPO_OWNER/$REPO_NAME

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================================="
    echo "  SUCCESS! $SECRET_NAME uploaded to GitHub Secrets      "
    echo "========================================================="
    echo ""
    echo "You can now run the workflow with 'run-pipeline' or 'full-cycle'"
    echo "to automatically trigger the Databricks pipeline."
else
    echo "Failed to upload secret to GitHub."
    exit 1
fi
