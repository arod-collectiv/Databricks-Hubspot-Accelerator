# Databricks notebook source
# Pull in config + helpers
# Adjust the path based on where you store notebooks in your workspace repo
# e.g. %run "./00_hubspot_config_and_scopes"
# MAGIC %run "./00_hubspot_config_and_scopes"

# COMMAND ----------

# 01_hubspot_full_load
# ===========================================

def load_owners():
    # Owners are typically exposed via "owners endpoints" (scope: crm.objects.owners.read). [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)
    # Using common v3 pattern if available in your account; if this fails, replace with owners v2 endpoint.
    try:
        data = paginate_get("/crm/v3/owners/", params={"limit": 500})
    except Exception:
        data = paginate_get("/owners/v2/owners", params={"count": 500}, page_key=None)  # fallback
        # owners/v2/owners returns list directly; normalize:
        if isinstance(data, dict):
            data = [data]
    write_bronze_delta(data, "crm_owners", mode="overwrite")

def load_crm_object(object_name: str, entity: str):
    # List records. [3](https://developers.hubspot.com/docs/guides/crm/using-object-apis)
    rows = paginate_get(f"/crm/v3/objects/{object_name}", params={"limit": 100, "archived": "false"})
    write_bronze_delta(rows, entity, mode="overwrite")

def load_meeting_links():
    # List meeting scheduling pages. [16](https://developers.hubspot.com/docs/api-reference/library-meetings-v3/guide)
    rows = paginate_get("/scheduler/v3/meetings/meeting-links", params={"limit": 100})
    write_bronze_delta(rows, "scheduler_meeting_links", mode="overwrite")

def load_campaigns():
    # Campaigns API. [12](https://developers.hubspot.com/docs/api-reference/marketing-campaigns-public-api-v3/guide)
    rows = paginate_get("/marketing/v3/campaigns", params={"limit": 100})
    write_bronze_delta(rows, "marketing_campaigns", mode="overwrite")
    return rows

def load_campaign_revenue(campaigns: list[dict]):
    # Revenue report endpoint exists per docs. [13](https://developers.hubspot.com/docs/api-reference/marketing-campaigns-public-api-v3/campaign-reporting/get-marketing-v3-campaigns-campaignGuid-reports-revenue)
    out = []
    for c in campaigns:
        guid = c.get("id")
        if not guid:
            continue
        try:
            rev = hs_request("GET", f"/marketing/v3/campaigns/{guid}/reports/revenue", params={})
            rev["_campaignGuid"] = guid
            out.append(rev)
        except Exception as e:
            print(f"Revenue fetch failed for campaign {guid}: {e}")
    write_bronze_delta(out, "marketing_campaign_revenue", mode="overwrite")

def load_currencies():
    # settings.currencies.read scope provides currency/exchange rate reads. [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)
    data = hs_request("GET", "/settings/v3/currencies", params={})
    # Many settings endpoints return {results:[...]}
    rows = data.get("results", []) if isinstance(data, dict) else data
    write_bronze_delta(rows, "settings_currencies", mode="overwrite")

def load_domains():
    # cms.domains.read lists connected domains. [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)
    data = hs_request("GET", "/cms/v3/domains", params={})
    rows = data.get("results", []) if isinstance(data, dict) else data
    write_bronze_delta(rows, "cms_domains", mode="overwrite")

def load_tax_rates():
    # tax_rates.read grants access to retrieve configured tax rates. [6](https://developers.hubspot.com/docs/api-reference/crm-line-items-v3/guide)
    data = hs_request("GET", "/crm/v3/taxes/tax-rates", params={})
    rows = data.get("results", []) if isinstance(data, dict) else data
    write_bronze_delta(rows, "tax_rates", mode="overwrite")

def load_comm_pref_definitions():
    # Communication preferences v4 definitions. [14](https://developers.hubspot.com/docs/api-reference/communication-preferences-subscriptions-v4/guide)
    data = hs_request("GET", "/communication-preferences/v4/definitions", params={"includeTranslations": "true"})
    rows = data.get("results", []) if isinstance(data, dict) else data
    write_bronze_delta(rows, "comm_pref_definitions", mode="overwrite")



# Run full loads for configured sources
# These are the only tables deemed usable per what the scope had
load_owners()
load_crm_object("marketing_events", "crm_marketing_events")
load_crm_object("feedback_submissions", "crm_feedback_submissions")
load_crm_object("partner-clients", "crm_partner_clients")
load_crm_object("partner-services", "crm_partner_services")

load_meeting_links()

campaigns = load_campaigns()
load_campaign_revenue(campaigns)

load_currencies()
load_domains()
load_tax_rates()
load_comm_pref_definitions()

print("Full load complete.")