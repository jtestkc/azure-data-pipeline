# deploy.ps1
# Master Orchestration Script for 1-Click Azure Databricks Pipeline Deployment

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "  Azure Databricks Sales Analytics + GenAI Deployment  " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# 1. Initialize and Apply Terraform
Write-Host "`n[1/4] Provisioning Infrastructure with Terraform..." -ForegroundColor Yellow
Set-Location terraform
terraform init
terraform apply -auto-approve

if ($LASTEXITCODE -ne 0) {
    Write-Host "Terraform Apply failed! Exiting..." -ForegroundColor Red
    exit $LASTEXITCODE
}

# 2. Extract Outputs
Write-Host "`n[2/4] Extracting Infrastructure Details..." -ForegroundColor Yellow
$outputs = terraform output -json | ConvertFrom-Json

$sql_server = $outputs.sql_server_fqdn.value
$sql_db = $outputs.sql_database_name.value
$storage_account = $outputs.storage_account_name.value
$storage_key = $outputs.storage_primary_access_key.value
$container = $outputs.storage_container_name.value

Set-Location ..

# 3. Generate Fake Data
Write-Host "`n[3/4] Generating Synthetic Datasets..." -ForegroundColor Yellow
python scripts/fake_data_generator.py

# 4. Seed Data to Azure
Write-Host "`n[4/4] Seeding SQL and ADLS Storage..." -ForegroundColor Yellow
# Note: we pass these via environment variables or direct arguments
# For this demo, we'll assume the Python script handles env injection or manual run
python scripts/seed_data.py

Write-Host "`n=========================================================" -ForegroundColor Green
Write-Host "  DEPLOYMENT COMPLETE!                                  " -ForegroundColor Green
Write-Host "  Workspace: $($outputs.databricks_workspace_url.value) " -ForegroundColor Green
Write-Host "=========================================================" -ForegroundColor Green
