<#
.SYNOPSIS
  Interactive parameter builder for the NetSuite Deployment Accelerator.
  - Captures/derives tenant, subscription, user objectId.
  - Creates config\deploymentparams.json.
  - Invokes 02_deploy.ps1 to perform deployment.

.NOTES
  Run with: pwsh ./01_set_params.ps1
#>

$ErrorActionPreference = 'Stop'

# Ensure paths
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$configDir  = Join-Path $scriptRoot 'config'
if (-not (Test-Path $configDir)) { New-Item -ItemType Directory -Path $configDir | Out-Null }

function Get-AzValue {
    param(
        [Parameter(Mandatory=$true)][string]$Query
    )
    $out = az account show --query $Query -o tsv 2>$null
    $out
}

function Get-SignedInUserObjectId {
    # Works in current Azure CLI (uses Microsoft Graph backend)
    $id = az ad signed-in-user show --query id -o tsv 2>$null
    if (-not $id) {
        Write-Warning "Could not resolve the signed-in user's objectId. You can provide it manually when prompted."
    }
    $id
}

Write-Host "-------------------------------------------------------"
Write-Host "----------- Netsuite Deployment Accelerator -----------"
Write-Host "-------------------------------------------------------"
Write-Host "Note: For any of the values, press Enter to accept default."

# Ensure user is logged in before we ask for subscription defaults
try {
    az account show -o none 2>$null
} catch {
    Write-Host "`nPlease sign in to Azure..." -ForegroundColor Cyan
    az login --only-show-errors | Out-Null
}

# Derive defaults from current az context
$defaultTenant        = Get-AzValue -Query 'tenantId'
$defaultSubscription  = Get-AzValue -Query 'id'
$defaultSubName       = Get-AzValue -Query 'name'
$defaultUserObjectId  = Get-SignedInUserObjectId

# Organization / Project
$organization = Read-Host "What is the name of the organization? (default=gocollectiv)"
if (-not $organization) { $organization = 'gocollectiv' }

$projectname = Read-Host "What is the project name? (default=nsintegration)"
if (-not $projectname) { $projectname = 'nsintegration' }

$env = Read-Host "What environment are you working on (dev,uat,prod)? (default=dev)"
if (-not $env) { $env = 'dev' }

$zone = Read-Host "What region are you deploying to? (default=eastus)"
if (-not $zone) { $zone = 'eastus' }

# Subscription/Tenant/User
$tenant_id = Read-Host "Azure Tenant Id (default=$defaultTenant)"
if (-not $tenant_id) { $tenant_id = $defaultTenant }

$subscription = Read-Host "Azure Subscription Id or Name (default=$defaultSubscription [$defaultSubName])"
if (-not $subscription) { $subscription = $defaultSubscription }

$userEntraID = Read-Host "Your Entra (AAD) Object Id (default=$defaultUserObjectId)"
if (-not $userEntraID) { $userEntraID = $defaultUserObjectId }

# Resource group
$defaultRg = "$organization-$projectname-$env"
$resourcegroupname = Read-Host "Enter Resource Group name (default=$defaultRg)"
if (-not $resourcegroupname) { $resourcegroupname = $defaultRg }

# Accept defaults?
$defaultChoice = (Read-Host "Use default resource names? (y/n) (default=y)").ToLower()
$useDefaults = ($defaultChoice -eq '' -or $defaultChoice -eq 'y' -or $defaultChoice -eq 'yes')

# Helper: sanitize names (alphanumeric + lowercase); Azure names have various rules.
function To-LowerAzSafe([string]$s) {
    if (-not $s) { return $s }
    # Storage accounts: only lower letters and digits, 3-24 chars
    ($s -replace '[^a-zA-Z0-9]', '').ToLower()
}

# Base name materials
$projectSafe = To-LowerAzSafe $projectname
$envSafe     = To-LowerAzSafe $env

# Defaults (or same as defaults for non-default path, to keep parity but allow editing)
$keyvaultname            = "kv-$projectname-$env"
$sqlservername           = "srv-$projectname-$env"
$sqlAdminUser            = "sqladmin"
$etlDbName               = "etlconfig"
$bacpacPath              = "..\etlmetadata\etlmetadata.bacpac"
$storageaccountname      = "st$projectSafe$envSafe"
$storagesku              = "Standard_LRS"
$storagefilesystemname   = $projectSafe
$synapseworkspacename    = "syn-$projectname-$env"

if (-not $useDefaults) {
    # If you want to customize, prompt here (but keep same defaults if left blank)
    $kvIn = Read-Host "Key Vault name (default=$keyvaultname)";                     if ($kvIn) { $keyvaultname = $kvIn }
    $srvIn = Read-Host "SQL Server name (default=$sqlservername)";                  if ($srvIn) { $sqlservername = $srvIn }
    $sqlAdminIn = Read-Host "SQL Admin Login (default=$sqlAdminUser)";              if ($sqlAdminIn) { $sqlAdminUser = $sqlAdminIn }
    $dbIn = Read-Host "ETL metadata database name (default=$etlDbName)";            if ($dbIn) { $etlDbName = $dbIn }
    $bacIn = Read-Host "BACPAC path (default=$bacpacPath)";                         if ($bacIn) { $bacpacPath = $bacIn }
    $saIn = Read-Host "Storage Account name (default=$storageaccountname)";         if ($saIn) { $storageaccountname = $saIn }
    $skuIn = Read-Host "Storage SKU (default=$storagesku)";                         if ($skuIn) { $storagesku = $skuIn }
    $fsIn = Read-Host "Storage Filesystem (default=$storagefilesystemname)";        if ($fsIn) { $storagefilesystemname = $fsIn }
    $synIn = Read-Host "Synapse workspace name (default=$synapseworkspacename)";    if ($synIn) { $synapseworkspacename = $synIn }
}

# Normalize certain names to Azure constraints
$storageaccountname = To-LowerAzSafe $storageaccountname
$sqlservername      = $sqlservername.ToLower()
$synapseworkspacename = $synapseworkspacename.ToLower()

# Ask for SQL admin password (secure entry)
while ($true) {
    $pwd1 = Read-Host "Enter SQL admin password (min 8 chars per Azure rules)" -AsSecureString
    $pwd2 = Read-Host "Confirm SQL admin password" -AsSecureString
    $bstr1 = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($pwd1)
    $plain1 = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr1)
    $bstr2 = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($pwd2)
    $plain2 = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr2)
    if ($plain1 -ne $plain2) {
        Write-Host "Passwords do not match, please try again." -ForegroundColor Yellow
    } elseif ([string]::IsNullOrWhiteSpace($plain1) -or $plain1.Length -lt 8) {
        Write-Host "Password too short/empty; please try again." -ForegroundColor Yellow
    } else {
        $sqlAdminPasswordPlain = $plain1
        break
    }
}

# Git integration (optional)
$gitUse = (Read-Host "Link Synapse to GitHub? (y/n) (default=n)").ToLower()
$gitEnabled = ($gitUse -eq 'y' -or $gitUse -eq 'yes')

$git = @{
    gitRepoType        = $(if ($gitEnabled) { 'GitHub' } else { '' })
    gitAccountName     = $(if ($gitEnabled) { (Read-Host "GitHub account/org (e.g., GoCollectiv)") } else { '' })
    gitRepositoryName  = $(if ($gitEnabled) { (Read-Host "GitHub repo name (e.g., NetSuiteDeployment)") } else { '' })
    gitBranchName      = $(if ($gitEnabled) { (Read-Host "Branch (e.g., main)") } else { '' })
    gitSynapseRootFolder = $(if ($gitEnabled) { (Read-Host "Synapse root folder (e.g., synapse)") } else { '' })
}

$deploymentparameters = [ordered]@{
    deployment = @{
        tenant_id     = $tenant_id
        subscription  = $subscription
        userEntraID   = $userEntraID
    }
    project = @{
        organization      = $organization
        projectname       = $projectname
        env               = $env
        zone              = $zone
        resourcegroupname = $resourcegroupname
    }
    storage = @{
        storageaccountname   = $storageaccountname
        storagesku           = $storagesku
        storagefilesystemname= $storagefilesystemname
    }
    keyvault = @{
        keyvaultname = $keyvaultname
    }
    metadata = @{
        sqlservername     = $sqlservername
        sqladmin          = $sqlAdminUser
        sqladminpassword  = $sqlAdminPasswordPlain
        etldatabasename   = $etlDbName
        dacpacfilepath    = $bacpacPath
    }
    etl = @{
        synapseworkspacename = $synapseworkspacename
    }
    git = $git
}

# Save JSON (preserve casing)
$json = ($deploymentparameters | ConvertTo-Json -Depth 5)
$jsonPath = Join-Path $configDir 'deploymentparams.json'
$json | Out-File -FilePath $jsonPath -Encoding UTF8
Write-Host "`nWrote configuration to: $jsonPath" -ForegroundColor Green

# Chain into deploy
$deployPath = Join-Path $scriptRoot '02_deploy.ps1'
if (Test-Path $deployPath) {
    Write-Host "`nStarting deployment..." -ForegroundColor Cyan
    & $deployPath
} else {
    Write-Host "`nDeployment script not found: $deployPath" -ForegroundColor Red
}
