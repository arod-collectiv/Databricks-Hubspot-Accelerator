<#
.SYNOPSIS
  Idempotent Azure deployment script for:
    - Resource Group
    - Key Vault (with role assignment)
    - Azure SQL Server + DB (optional BACPAC import)
    - Storage Account (ADLS Gen2) + container
    - Azure Synapse workspace (with firewall rule, role assignments)
    - Optional: Synapse GitHub integration
    - Optional: Create Synapse Linked Service for Key Vault via template

.NOTES
  Requires Azure CLI. Will attempt to resolve signed-in user's objectId if not provided.
#>

$ErrorActionPreference = 'Stop'
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

function ThrowOnAzError($Label) {
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI command failed at step: $Label"
    }
}

function AzOut([string]$cmd, [string]$label) {
    Write-Host $cmd -ForegroundColor DarkGray
    $output = Invoke-Expression $cmd
    if ($LASTEXITCODE -ne 0) { throw "Azure CLI failed ($label)" }
    return $output
}

function Ensure-LoggedIn {
    try {
        az account show -o none 2>$null
    } catch {
        Write-Host "Please sign in to Azure..." -ForegroundColor Cyan
        az login --only-show-errors | Out-Null
    }
}

function Load-Params {
    $cfgPath = Join-Path $scriptRoot 'config\deploymentparams.json'
    if (-not (Test-Path $cfgPath)) { throw "Config not found: $cfgPath. Run 01_set_params.ps1 first." }
    Get-Content $cfgPath -Raw | ConvertFrom-Json
}

function Get-MyObjectId {
    param([string]$provided)
    if ($provided) { return $provided }
    $id = az ad signed-in-user show --query id -o tsv 2>$null
    if (-not $id) {
        throw "Could not determine signed-in user's objectId. Provide deployment.userEntraID in config."
    }
    return $id
}

function Ensure-ResourceGroup {
    param([string]$name, [string]$location)
    Write-Host ".. Validating Resource Group: $name"
    $exists = az group exists -n $name 2>$null
    if ($exists -eq 'true') {
        Write-Host "...... Resource group $name already exists"
    } else {
        az group create -l $location -n $name --only-show-errors 1>$null
        ThrowOnAzError "rg-create"
        Write-Host "...... Resource group $name created"
    }
}

function Ensure-KeyVault {
    param(
        [string]$name,
        [string]$rg,
        [string]$location,
        [string]$scopeSubId,
        [string]$assigneeObjectId
    )
    Write-Host ".. Validating Key Vault: $name"
    $kvName = az keyvault show -n $name -g $rg --query "name" -o tsv 2>$null
    if ($kvName -eq $name) {
        Write-Host "...... Key Vault $name already exists"
    } else {
        az keyvault create -l $location -n $name -g $rg --only-show-errors 1>$null
        ThrowOnAzError "kv-create"
        Write-Host "...... Key Vault $name created"
    }

    # Assign "Key Vault Administrator" at KV scope (idempotent)
    $kvId = az keyvault show -n $name -g $rg --query id -o tsv
    $ra = az role assignment list --assignee-object-id $assigneeObjectId --scope $kvId --query "[?roleDefinitionName=='Key Vault Administrator'] | length(@)" -o tsv
    if ($ra -eq '0') {
        az role assignment create --assignee-object-id $assigneeObjectId --role "Key Vault Administrator" --scope $kvId --only-show-errors 1>$null
        ThrowOnAzError "kv-role"
        Write-Host "...... Assigned Key Vault Administrator to $assigneeObjectId at $name"
    } else {
        Write-Host "...... Key Vault Administrator role already assigned"
    }
}

function Ensure-Storage {
    param(
        [string]$accountName,
        [string]$rg,
        [string]$location,
        [string]$sku,
        [string]$filesystem,
        [string]$subscriptionId,
        [string]$assigneeObjectId
    )
    Write-Host ".. Validating Storage Account: $accountName"
    $sa = az storage account show -n $accountName -g $rg --query "name" -o tsv 2>$null
    if ($sa -eq $accountName) {
        Write-Host "...... Storage account $accountName already exists"
    } else {
        az storage account create -l $location -n $accountName -g $rg --kind StorageV2 --hns true --sku $sku --only-show-errors 1>$null
        ThrowOnAzError "sa-create"
        Write-Host "...... Storage account $accountName created"
    }

    # Container (filesystem) for ADLS Gen2
    $exists = az storage container show --account-name $accountName -n $filesystem --auth-mode login --query name -o tsv 2>$null
    if ($exists -eq $filesystem) {
        Write-Host "...... Filesystem '$filesystem' already exists"
    } else {
        az storage container create -n $filesystem --account-name $accountName --auth-mode login --only-show-errors 1>$null
        ThrowOnAzError "sa-fs-create"
        Write-Host "...... Filesystem '$filesystem' created"
    }

    # RBAC on Storage for current user (Blob Data Contributor)
    $saId = az storage account show -n $accountName -g $rg --query id -o tsv
    $hasRole = az role assignment list --assignee-object-id $assigneeObjectId --scope $saId --query "[?roleDefinitionName=='Storage Blob Data Contributor'] | length(@)" -o tsv
    if ($hasRole -eq '0') {
        az role assignment create --assignee-object-id $assigneeObjectId --role "Storage Blob Data Contributor" --scope $saId --only-show-errors 1>$null
        ThrowOnAzError "sa-role"
        Write-Host "...... Assigned 'Storage Blob Data Contributor' on $accountName"
    } else {
        Write-Host "...... Storage Blob Data Contributor already assigned"
    }
}

function Ensure-SqlServerAndDb {
    param(
        [string]$server,
        [string]$adminUser,
        [string]$adminPassword,
        [string]$dbName,
        [string]$rg,
        [string]$location,
        [string]$kvName,
        [string]$myPublicIp
    )
    Write-Host ".. Validating SQL Server: $server"
    $srv = az sql server show -n $server -g $rg --query "name" -o tsv 2>$null
    if ($srv -ne $server) {
        az sql server create -l $location -n $server -g $rg --admin-user $adminUser --admin-password $adminPassword --only-show-errors 1>$null
        ThrowOnAzError "sql-server-create"
        Write-Host "...... SQL Server $server created"

        # Firewall rules (allow Azure + your IP)
        az sql server firewall-rule create -g $rg --server $server -n AllowAllWindowsAzureIps --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 --only-show-errors 1>$null
        ThrowOnAzError "sql-fw-azure"
        if ($myPublicIp) {
            az sql server firewall-rule create -g $rg --server $server -n adminUser --start-ip-address $myPublicIp --end-ip-address $myPublicIp --only-show-errors 1>$null
            ThrowOnAzError "sql-fw-ip"
        }

        # Store password in KV as 'sqladminpassword'
        az keyvault secret set --vault-name $kvName --name "${adminUser}password" --value $adminPassword --only-show-errors 1>$null
        ThrowOnAzError "kv-set-sqlpwd"
    } else {
        Write-Host "...... SQL Server $server already exists"
    }

    # DB
    Write-Host ".. Validating SQL DB: $dbName"
    $exists = az sql db show -n $dbName --server $server -g $rg --query "name" -o tsv 2>$null
    if ($exists -ne $dbName) {
        az sql db create -g $rg --server $server -n $dbName --only-show-errors 1>$null
        ThrowOnAzError "sql-db-create"
        Write-Host "...... SQL DB $dbName created"
    } else {
        Write-Host "...... SQL DB $dbName already exists"
    }
}

function Try-Import-Bacpac {
    param(
        [string]$server,
        [string]$dbName,
        [string]$adminUser,
        [string]$adminPassword,
        [string]$bacpacPath
    )
    if (-not (Test-Path $bacpacPath)) {
        Write-Host "...... BACPAC not found at '$bacpacPath' (skipping import)" -ForegroundColor Yellow
        return
    }
    $sqlpackage = Get-Command sqlpackage.exe -ErrorAction SilentlyContinue
    if (-not $sqlpackage) {
        Write-Host "...... sqlpackage.exe not found in PATH (skipping BACPAC import)" -ForegroundColor Yellow
        return
    }
    & $sqlpackage.Source /a:Import /sf:$bacpacPath /tsn:"$server.database.windows.net" /tdn:$dbName /tu:"$adminUser@$server" /tp:$adminPassword
    if ($LASTEXITCODE -eq 0) {
        Write-Host "...... BACPAC imported into $dbName"
    } else {
        Write-Warning "BACPAC import failed; continuing deployment."
    }
}

function Ensure-Synapse {
    param(
        [string]$workspace,
        [string]$rg,
        [string]$location,
        [string]$storageAccount,
        [string]$filesystem,
        [string]$sqlAdminUser,
        [string]$sqlAdminPassword
    )
    Write-Host ".. Validating Synapse Workspace: $workspace"
    $syn = az synapse workspace show -n $workspace -g $rg --query "name" -o tsv 2>$null
    if ($syn -ne $workspace) {
        az synapse workspace create -l $location -n $workspace -g $rg --storage-account $storageAccount --file-system $filesystem --sql-admin-login-user $sqlAdminUser --sql-admin-login-password $sqlAdminPassword --only-show-errors 1>$null
        ThrowOnAzError "syn-create"
        Write-Host "...... Synapse workspace $workspace created"
    } else {
        Write-Host "...... Synapse workspace $workspace already exists"
    }
}

function Ensure-SynapseOps {
    param(
        [string]$workspace,
        [string]$rg,
        [string]$subscriptionId,
        [string]$storageAccount,
        [string]$kvName,
        [string]$assigneeObjectId,
        [string]$myPublicIp
    )
    # Firewall + Synapse roles + KV/Storage RBAC for MSI
    $syn = az synapse workspace show -n $workspace -g $rg --query "name" -o tsv 2>$null
    if ($syn -ne $workspace) {
        Write-Host "...... [WARNING] Synapse workspace does not exist; skipping firewall/roles"
        return
    }

    if ($myPublicIp) {
        az synapse workspace firewall-rule create -n "adminUser" --workspace-name $workspace -g $rg --start-ip-address $myPublicIp --end-ip-address $myPublicIp --only-show-errors 1>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "...... Added Synapse firewall rule for your IP"
        } else {
            Write-Warning "Failed to add firewall rule in Synapse; continuing."
        }
    }

    # Assign Synapse Administrator to current user
    $assigned = az synapse role assignment list --workspace-name $workspace --assignee $assigneeObjectId --role "Synapse Administrator" --query "length(@)" -o tsv 2>$null
    if ($assigned -eq '0') {
        az synapse role assignment create --workspace-name $workspace --role "Synapse Administrator" --assignee $assigneeObjectId --only-show-errors 1>$null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "...... Assigned 'Synapse Administrator' to $assigneeObjectId"
        } else {
            Write-Warning "Failed to assign Synapse role; continuing."
        }
    } else {
        Write-Host "...... Synapse Administrator already assigned"
    }

    # MSI roles for Synapse on Storage + RG (Key Vault Secrets User)
    $synId = az synapse workspace show -n $workspace -g $rg --query "identity.principalId" -o tsv 2>$null
    if (-not $synId) {
        Write-Warning "Could not resolve Synapse MSI principalId; skipping MSI role assignments."
        return
    }

    $saId = az storage account show -n $storageAccount -g $rg --query id -o tsv 2>$null
    if ($saId) {
        $has = az role assignment list --assignee-object-id $synId --scope $saId --query "[?roleDefinitionName=='Storage Blob Data Contributor'] | length(@)" -o tsv
        if ($has -eq '0') {
            az role assignment create --assignee-object-id $synId --role "Storage Blob Data Contributor" --scope $saId --only-show-errors 1>$null
            if ($LASTEXITCODE -eq 0) { Write-Host "...... Assigned Synapse MSI 'Storage Blob Data Contributor' on storage" }
        }
    }

    $rgId = az group show -n $rg --query id -o tsv
    $hasKV = az role assignment list --assignee-object-id $synId --scope $rgId --query "[?roleDefinitionName=='Key Vault Secrets User'] | length(@)" -o tsv
    if ($hasKV -eq '0') {
        az role assignment create --assignee-object-id $synId --role "Key Vault Secrets User" --scope $rgId --only-show-errors 1>$null
        if ($LASTEXITCODE -eq 0) { Write-Host "...... Assigned Synapse MSI 'Key Vault Secrets User' on RG" }
    }

    # Store Synapse SQL password in KV (useful for LS auth if needed)
    try {
        az keyvault secret set --vault-name $kvName --name "synapse-${sqlAdmin}password" --value "USE_SAME_SQL_ADMIN_PASSWORD" --only-show-errors 1>$null
        # For security, you may omit this; shown here for parity with original code intent.
    } catch {
        # ignore if KV not accessible
    }
}

function Maybe-Configure-Git {
    param(
        [pscustomobject]$git,
        [string]$workspace,
        [string]$rg
    )
    if (-not $git) { return }
    if (-not $git.gitRepoType) { return }
    if ($git.gitRepoType -notin @('GitHub','AzureDevOpsGit')) {
        Write-Warning "Unsupported gitRepoType '$($git.gitRepoType)'. Skipping Git integration."
        return
    }

    $syn = az synapse workspace show -n $workspace -g $rg --query "name" -o tsv 2>$null
    if ($syn -ne $workspace) {
        Write-Warning "Synapse $workspace not found; skipping Git integration."
        return
    }

    $cmd = @(
        "az synapse workspace update",
        "-n `"$workspace`"",
        "-g `"$rg`"",
        "--repository-type `"$($git.gitRepoType)`""
    )

    if ($git.gitRepoType -eq 'GitHub') {
        # For GitHub you typically need --host-name, --account-name, --repository-name, --collaboration-branch, --root-folder
        if ($git.gitAccountName -and $git.gitRepositoryName -and $git.gitBranchName -and $git.gitSynapseRootFolder) {
            $cmd += @(
                "--host-name `"https://github.com`"",
                "--account-name `"$($git.gitAccountName)`"",
                "--repository-name `"$($git.gitRepositoryName)`"",
                "--collaboration-branch `"$($git.gitBranchName)`"",
                "--root-folder `"$($git.gitSynapseRootFolder)`""
            )
        } else {
            Write-Warning "GitHub fields incomplete. Skipping Git linkage."
            return
        }
    }

    $full = $cmd -join ' '
    try {
        AzOut -cmd $full -label "syn-git-update" | Out-Null
        Write-Host "...... Git repository linked to Synapse"
    } catch {
        Write-Warning "Failed to configure Synapse Git linkage. You can complete this in Synapse Studio."
    }
}

function Maybe-Create-KV-LinkedService {
    param(
        [string]$workspace,
        [string]$rg,
        [string]$kvName
    )
    $syn = az synapse workspace show -n $workspace -g $rg --query "name" -o tsv 2>$null
    if ($syn -ne $workspace) {
        Write-Warning "Synapse workspace not found; skipping KV linked service."
        return
    }

    $template = Join-Path $scriptRoot "templates\kv_template.ps1"
    $jsonOut  = Join-Path $scriptRoot "linkedservices\keyvault.json"
    $jsonDir  = Split-Path -Parent $jsonOut
    if (-not (Test-Path $jsonDir)) { New-Item -ItemType Directory -Path $jsonDir | Out-Null }

    if (Test-Path $template) {
        & $template -jsonInputFilePath "templates\kv_template.json" -ls_name "keyvault" -keyvaultname $kvName -jsonOutputFilePath $jsonOut
        if ($LASTEXITCODE -eq 0 -and (Test-Path $jsonOut)) {
            # IMPORTANT: Avoid @"..." which starts a here-string in PowerShell.
            az synapse linked-service create -n "keyvault" --file "@$jsonOut" --workspace-name $workspace --only-show-errors 1>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host "...... Key Vault Linked Service created in Synapse"
            } else {
                Write-Warning "Failed to create KV Linked Service."
            }
        } else {
            Write-Warning "KV LS template execution failed or output missing."
        }
    } else {
        Write-Warning "KV LS template not found at $template. Skipping."
    }
}


function Maybe-Run-SqlcmdScripts {
    param(
        [string]$workspace,
        [string]$sqlAdmin,
        [string]$sqlPassword
    )
    $sqlcmd = Get-Command sqlcmd.exe -ErrorAction SilentlyContinue
    if (-not $sqlcmd) {
        Write-Host "sqlcmd.exe not found in PATH; skipping T-SQL script execution." -ForegroundColor Yellow
        return
    }

    $server = "$workspace-ondemand.sql.azuresynapse.net"
    $db = "warehouse"  # as used in original script
    $scripts = @(
        ".\etlmetadata\StoreProcedures\dbo.CreateDB.sql",
        ".\etlmetadata\StoreProcedures\dbo.CreateExternalSuppTable.sql",
        ".\etlmetadata\StoreProcedures\dbo.CreateExternalTable.sql",
        ".\etlmetadata\StoreProcedures\dbo.RunSupplementTableQuery.sql"
    )

    foreach ($s in $scripts) {
        if (Test-Path $s) {
            & $sqlcmd.Source -S $server -U $sqlAdmin -P $sqlPassword -i $s -d $db
            if ($LASTEXITCODE -eq 0) {
                Write-Host "...... Executed $s"
            } else {
                Write-Warning "Failed to execute $s; continuing."
            }
        } else {
            Write-Warning "Script not found: $s"
        }
    }
}

# ------------------------------
# Main
# ------------------------------
Ensure-LoggedIn
$cfg = Load-Params

# Set subscription context
if ($cfg.deployment.subscription) {
    az account set --subscription "$($cfg.deployment.subscription)" --only-show-errors 1>$null
    if ($LASTEXITCODE -ne 0) { throw "Failed to set subscription to $($cfg.deployment.subscription)" }
} else {
    Write-Warning "No subscription specified in config.deployment.subscription; using current az account."
}

# Gather working vars
$subscription = (az account show --query id -o tsv)
$tenantId     = $cfg.deployment.tenant_id
$userObjId    = Get-MyObjectId -provided $cfg.deployment.userEntraID

$organization = $cfg.project.organization
$projectname  = $cfg.project.projectname
$env          = $cfg.project.env
$zone         = $cfg.project.zone
$resourcegroupname = $cfg.project.resourcegroupname

$keyvaultname = $cfg.keyvault.keyvaultname

$sqlservername    = $cfg.metadata.sqlservername
$sqladmin         = $cfg.metadata.sqladmin
$sqladminpassword = $cfg.metadata.sqladminpassword
$etldatabasename  = $cfg.metadata.etldatabasename
$dacpacfilepath   = $cfg.metadata.dacpacfilepath  # actually a BACPAC in your inputs

$storageaccountname    = $cfg.storage.storageaccountname
$storagesku            = $cfg.storage.storagesku
$storagefilesystemname = $cfg.storage.storagefilesystemname

$synapseworkspacename  = $cfg.etl.synapseworkspacename

$git = $cfg.git

# Find public IP (for firewall)
try {
    $myip = (Invoke-WebRequest -UseBasicParsing -Uri "https://api.ipify.org/").Content
} catch { $myip = $null }

# 1) Resource Group
Ensure-ResourceGroup -name $resourcegroupname -location $zone

# 2) Key Vault
Ensure-KeyVault -name $keyvaultname -rg $resourcegroupname -location $zone -scopeSubId $subscription -assigneeObjectId $userObjId

# 3) Storage
Ensure-Storage -accountName $storageaccountname -rg $resourcegroupname -location $zone -sku $storagesku -filesystem $storagefilesystemname -subscriptionId $subscription -assigneeObjectId $userObjId

# 4) SQL + DB (+ optional BACPAC)
Ensure-SqlServerAndDb -server $sqlservername -adminUser $sqladmin -adminPassword $sqladminpassword -dbName $etldatabasename -rg $resourcegroupname -location $zone -kvName $keyvaultname -myPublicIp $myip
Try-Import-Bacpac -server $sqlservername -dbName $etldatabasename -adminUser $sqladmin -adminPassword $sqladminpassword -bacpacPath $dacpacfilepath

# 5) Synapse
Ensure-Synapse -workspace $synapseworkspacename -rg $resourcegroupname -location $zone -storageAccount $storageaccountname -filesystem $storagefilesystemname -sqlAdminUser $sqladmin -sqlAdminPassword $sqladminpassword

# 6) Synapse ops: firewall + roles + MSI RBAC
Ensure-SynapseOps -workspace $synapseworkspacename -rg $resourcegroupname -subscriptionId $subscription -storageAccount $storageaccountname -kvName $keyvaultname -assigneeObjectId $userObjId -myPublicIp $myip

# 7) Optional: Git
Maybe-Configure-Git -git $git -workspace $synapseworkspacename -rg $resourcegroupname

# 8) Optional: Linked Service to KV
Maybe-Create-KV-LinkedService -workspace $synapseworkspacename -rg $resourcegroupname -kvName $keyvaultname

# 9) Optional: Run SQL scripts via sqlcmd if available
Maybe-Run-SqlcmdScripts -workspace $synapseworkspacename -sqlAdmin $sqladmin -sqlPassword $sqladminpassword

Write-Host "`nDeployment completed." -ForegroundColor Green
