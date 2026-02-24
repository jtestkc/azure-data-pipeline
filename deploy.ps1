param (
    [ValidateSet("1", "2", "3", "4", "All")]
    [string]$Stage = "All"
)

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "  Azure Databricks Sales Analytics + GenAI Deployment  " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "Selected Stage: $Stage" -ForegroundColor Cyan

Set-Location terraform
terraform init
Set-Location ..

function Run-Stage1 {
    Write-Host "`n[Stage 1] Provisioning Core Infrastructure..." -ForegroundColor Yellow
    Set-Location terraform
    
    # Target core resources only (exclude compute & workflows initially)
    $targets = @(
        "-target=azurerm_resource_group.main",
        "-target=azurerm_storage_account.datalake",
        "-target=azurerm_storage_container.rawdata",
        "-target=azurerm_key_vault.main",
        "-target=azurerm_key_vault_access_policy.terraform_sp",
        "-target=azurerm_key_vault_secret.storage_account_key",
        "-target=azurerm_key_vault_secret.storage_account_name",
        "-target=azurerm_eventhub_namespace.main",
        "-target=azurerm_eventhub.orders",
        "-target=azurerm_eventhub_authorization_rule.listen_send",
        "-target=azurerm_key_vault_secret.eventhub_connection_string",
        "-target=azurerm_databricks_workspace.main",
        "-target=azurerm_log_analytics_workspace.main"
    )
    
    Write-Host "Running: terraform apply -auto-approve $targets" -ForegroundColor Gray
    terraform apply -auto-approve @targets -lock=false

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Stage 1 (Core Infra) failed! Exiting..." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Set-Location ..
}

function Run-Stage2 {
    Write-Host "`n[Stage 2] Provisioning Databricks Compute & Secrets..." -ForegroundColor Yellow
    Set-Location terraform
    
    $targets = @(
        "-target=azurerm_key_vault_secret.sp_client_id",
        "-target=azurerm_key_vault_secret.sp_client_secret",
        "-target=azurerm_key_vault_secret.sp_tenant_id",
        "-target=databricks_secret_scope.keyvault",
        "-target=databricks_cluster.main"
    )

    Write-Host "Running: terraform apply -auto-approve $targets" -ForegroundColor Gray
    # Added -lock=false to avoid being blocked by previous failed runs
    terraform apply -auto-approve @targets -lock=false

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Stage 2 (Compute) failed! Check if Databricks Workspace is ready." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Set-Location ..
}

function Run-Stage3 {
    Write-Host "`n[Stage 3] Configuring Code, Git, & Workflows..." -ForegroundColor Yellow
    Set-Location terraform
    
    $targets = @(
        "-target=databricks_git_credential.personal_pat",
        "-target=databricks_repo.analytics_pipeline",
        "-target=databricks_notebook.notebook_bronze",
        "-target=databricks_notebook.notebook_silver",
        "-target=databricks_notebook.notebook_gold",
        "-target=databricks_notebook.notebook_enrichment",
        "-target=databricks_workspace_file.config_connection",
        "-target=databricks_job.pipeline"
    )

    Write-Host "Running: terraform apply -auto-approve $targets" -ForegroundColor Gray
    # Added -lock=false to avoid being blocked by previous failed runs
    terraform apply -auto-approve @targets -lock=false

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Stage 3 (Workflows) failed! Check Git credentials and repo URL." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Set-Location ..
}

function Run-Stage4 {
    Write-Host "`n[Stage 4] Generating and Seeding Synthetic Datasets..." -ForegroundColor Yellow
    
    Write-Host "Extracting Infrastructure Details for Seeding..."
    Set-Location terraform
    $outputs = terraform output -json | ConvertFrom-Json
    Set-Location ..

    python scripts/fake_data_generator.py
    python scripts/seed_data.py

    Write-Host "`n[Auto-Trigger] Starting Databricks Pipeline Job..." -ForegroundColor Yellow
    $workspaceUrl = $outputs.databricks_workspace_url.value
    $jobId = $outputs.databricks_job_id.value

    # Extract Service Principal credentials to authenticate to Databricks API
    $tfvarsPath = "terraform\terraform.tfvars"
    $clientId = (Select-String -Path $tfvarsPath -Pattern 'client_id\s*=\s*"([^"]+)"').Matches.Groups[1].Value
    $clientSecret = (Select-String -Path $tfvarsPath -Pattern 'client_secret\s*=\s*"([^"]+)"').Matches.Groups[1].Value
    $tenantId = (Select-String -Path $tfvarsPath -Pattern 'tenant_id\s*=\s*"([^"]+)"').Matches.Groups[1].Value

    # Request an Azure AD token for the Databricks application (resource ID: 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d)
    $body = @{
        grant_type    = "client_credentials"
        client_id     = $clientId
        client_secret = $clientSecret
        resource      = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
    }
    try {
        $tokenResponse = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$tenantId/oauth2/token" -Body $body
        $accessToken = $tokenResponse.access_token

        $headers = @{
            "Authorization" = "Bearer $accessToken"
            "Content-Type"  = "application/json"
        }
        $runBody = @{
            job_id = [long]$jobId
        } | ConvertTo-Json

        $runResponse = Invoke-RestMethod -Method Post -Uri "$workspaceUrl/api/2.1/jobs/run-now" -Headers $headers -Body $runBody
        Write-Host "Success! Databricks pipeline job automatically started (Run ID: $($runResponse.run_id))." -ForegroundColor Green
    }
    catch {
        Write-Host "Failed to trigger Databricks job automatically: $_" -ForegroundColor Red
    }
}

# --- Execution Flow ---

if ($Stage -in @("1", "All")) { Run-Stage1 }
if ($Stage -in @("2", "All")) { Run-Stage2 }
if ($Stage -in @("3", "All")) { Run-Stage3 }
if ($Stage -in @("4", "All")) { Run-Stage4 }

if ($Stage -eq "All") {
    Set-Location terraform
    $outputs = terraform output -json | ConvertFrom-Json
    Set-Location ..
    
    Write-Host "`n=========================================================" -ForegroundColor Green
    Write-Host "  DEPLOYMENT COMPLETE!                                  " -ForegroundColor Green
    if ($outputs) {
        Write-Host "  Workspace: $($outputs.databricks_workspace_url.value) " -ForegroundColor Green
    }
    Write-Host "=========================================================" -ForegroundColor Green
}
