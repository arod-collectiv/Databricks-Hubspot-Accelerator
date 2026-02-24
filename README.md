# Databricks-Hubspot-Accelorator
Databricks HubSpot Rapid Deployment
Overview
This repo includes two PowerShell scripts that provision an Azure-based data platform and prepare it for HubSpot ingestion. Resource deployment is the same as the NetSuite version—the only change is that the Databricks notebooks are configured for HubSpot (not NetSuite).
Scripts


01_set_params.ps1
Interactive setup that collects deployment inputs (tenant/subscription, environment, naming, region, etc.) and writes them to config/deploymentparams.json. When finished, it automatically triggers the deployment script.


02_deploy.ps1
Reads deploymentparams.json and deploys required Azure resources idempotently (safe to re-run; it won’t duplicate existing resources).


What gets deployed

Resource Group (in your chosen region)
Azure Key Vault
Stores secrets (e.g., SQL admin password). Deploying user is granted Key Vault admin access.
Azure SQL Server + Database
Creates server + DB, configures firewall rules (Azure services + optional current IP), and stores the SQL admin password in Key Vault.
Optional: imports a provided BACPAC if configured.
ADLS Gen2 Storage Account
Creates the account + default filesystem/container; assigns the deployer Storage Blob Data Contributor.
Azure Synapse Workspace
Linked to the ADLS account; deployer gets Synapse Administrator. Workspace managed identity is granted access to Storage and Key Vault secrets.
Optional: Synapse GitHub integration (repo/branch/root folder)
Optional: Synapse Key Vault Linked Service (named keyvault)
Optional: Run post-deployment SQL init scripts (via sqlcmd if available)


### Quickstart
1) Prerequisites

Azure subscription with permissions to create resources and assign roles (Contributor/Owner recommended)
Azure CLI installed and authenticated (az login)
PowerShell 7+ (pwsh)
Internet access (used for Azure operations and optional public IP detection)
Optional: sqlpackage (for BACPAC import) and sqlcmd (for SQL init scripts)
Optional: GitHub repo details for Synapse source control

2) Run
PowerShellpwsh -File .\01_set_params.ps1Show more lines
This will:

Prompt for required values (with sensible defaults)
Create config/deploymentparams.json
Automatically execute 02_deploy.ps1

On completion, verify resources in the Azure Portal under the deployed resource group.

Databricks Notebooks (HubSpot)
After infrastructure deployment, import the Databricks notebooks and configure your cluster/workspace to access the deployed ADLS Gen2 and Key Vault-backed secrets.
Notebook flow
00_hubspot_config_and_scopes -> 01_hubspot_full_load -> 02_hubspot_incremental_load_crm

### NOTE:
The app must be configured on the Hubspot side, discussion around which tables are relevant for needs should take place before any real development takes place.
