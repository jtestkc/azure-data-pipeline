param (
    [string]$RepoOwner = "jtestkc",
    [string]$RepoName = "azure-data-pipeline",
    [string]$SecretName = "DATABRICKS_TOKEN"
)

# Check if GitHub CLI is installed
$ghPath = Get-Command gh -ErrorAction SilentlyContinue
if (-not $ghPath) {
    Write-Host "GitHub CLI not found. Installing..." -ForegroundColor Yellow
    winget install -e --id GitHub.cli
    # Refresh PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
}

Write-Host "=========================================================" -ForegroundColor Cyan
Write-Host "  Uploading DATABRICKS_TOKEN to GitHub Secrets         " -ForegroundColor Cyan
Write-Host "=========================================================" -ForegroundColor Cyan

# Check if logged in to GitHub
Write-Host "`nChecking GitHub authentication..." -ForegroundColor Yellow
gh auth status 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Not logged in to GitHub. Please run 'gh auth login' first." -ForegroundColor Red
    Write-Host "  gh auth login" -ForegroundColor Cyan
    exit 1
}

# Get PAT token from terraform output
Write-Host "`nGetting PAT token from Terraform output..." -ForegroundColor Yellow
Set-Location terraform

# Check if terraform is initialized
if (-not (Test-Path ".terraform/terraform.tfstate")) {
    Write-Host "Terraform not initialized. Running terraform init..." -ForegroundColor Yellow
    terraform init
}

# Get the PAT token
$patToken = terraform output -raw databricks_pat_token 2>$null
if (-not $patToken) {
    Write-Host "Failed to get PAT token from Terraform output." -ForegroundColor Red
    Write-Host "Make sure Terraform apply has completed successfully." -ForegroundColor Red
    Set-Location ..
    exit 1
}

Set-Location ..

# Upload to GitHub secrets
Write-Host "`nUploading $SecretName to GitHub secrets..." -ForegroundColor Yellow

# Use gh secret set with piped input (more secure)
$env:GITHUB_TOKEN = $patToken
$command = "gh secret set $SecretName --body `"$patToken`" --repo $RepoOwner/$RepoName"

# Alternative: Use API to set secret
$body = @{
    encrypted_value = $patToken
    key_id = ""
} | ConvertTo-Json

# Check if gh has the secret set command
gh secret set $SecretName --body "$patToken" --repo $RepoOwner/$RepoName

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n=========================================================" -ForegroundColor Green
    Write-Host "  SUCCESS! $SecretName uploaded to GitHub Secrets      " -ForegroundColor Green
    Write-Host "=========================================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "You can now run the workflow with 'run-pipeline' or 'full-cycle'" -ForegroundColor Cyan
    Write-Host "to automatically trigger the Databricks pipeline." -ForegroundColor Cyan
} else {
    Write-Host "Failed to upload secret to GitHub." -ForegroundColor Red
    exit 1
}
