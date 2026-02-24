# Databricks notebook source
import time
import json
import math
import typing as T
from datetime import datetime, timezone

import requests
from pyspark.sql import functions as F
from pyspark.sql import types as Tz

# COMMAND ----------

# ===========================================
# 00_hubspot_config_and_scopes
# ===========================================

# Widgets (edit defaults as needed)
dbutils.widgets.text("secret_scope", "hubspot")
dbutils.widgets.text("client_id_key", "client_id")
dbutils.widgets.text("client_secret_key", "client_secret")
dbutils.widgets.text("refresh_token_key", "refresh_token")

dbutils.widgets.text("base_url", "https://api.hubapi.com")
dbutils.widgets.text("bronze_base_path", "dbfs:/mnt/datalake/bronze/hubspot")

# Unity Catalog optional (leave blank to skip table registration)
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "hubspot_bronze")

# Watermark storage (Delta table path)
dbutils.widgets.text("watermark_path", "dbfs:/mnt/datalake/_control/hubspot_watermarks")


SECRET_SCOPE = dbutils.widgets.get("secret_scope")
CLIENT_ID_KEY = dbutils.widgets.get("client_id_key")
CLIENT_SECRET_KEY = dbutils.widgets.get("client_secret_key")
REFRESH_TOKEN_KEY = dbutils.widgets.get("refresh_token_key")

BASE_URL = dbutils.widgets.get("base_url").rstrip("/")
BRONZE_BASE_PATH = dbutils.widgets.get("bronze_base_path").rstrip("/")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
WATERMARK_PATH = dbutils.widgets.get("watermark_path").rstrip("/")



def _secret(key: str) -> str:
    return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)

def get_access_token() -> str:
    """
    Refresh HubSpot OAuth access token using refresh_token grant.
    Docs: POST /oauth/v1/token.  [2](https://developers.hubspot.com/docs/api-reference/auth-oauth-v1/tokens/post-oauth-v1-token)
    """
    url = f"{BASE_URL}/oauth/v1/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": _secret(CLIENT_ID_KEY),
        "client_secret": _secret(CLIENT_SECRET_KEY),
        "refresh_token": _secret(REFRESH_TOKEN_KEY),
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=payload, headers=headers, timeout=60)
    if resp.status_code >= 300:
        raise RuntimeError(f"Token refresh failed: {resp.status_code} {resp.text}")
    return resp.json()["access_token"]



def hs_request(
    method: str,
    path: str,
    params: dict | None = None,
    json_body: dict | None = None,
    max_retries: int = 6,
    timeout_s: int = 120
) -> dict:
    """
    HubSpot API request wrapper with basic retry/backoff.
    """
    token = get_access_token()
    url = f"{BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {token}"}

    attempt = 0
    while True:
        attempt += 1
        resp = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout_s
        )
        if resp.status_code < 300:
            return resp.json() if resp.text else {}

        # Retry on transient conditions
        if resp.status_code in (429, 500, 502, 503, 504) and attempt <= max_retries:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else int(min(60, 2 ** attempt))
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"HubSpot API error {resp.status_code} for {path}: {resp.text}")



def paginate_get(path: str, params: dict | None = None, page_key: str = "results") -> list[dict]:
    """
    Cursor pagination helper for endpoints that return: {"results":[...], "paging":{"next":{"after":"..."}}}
    Many CRM list endpoints follow this pattern. [3](https://developers.hubspot.com/docs/guides/crm/using-object-apis)
    """
    out = []
    p = dict(params or {})
    while True:
        data = hs_request("GET", path, params=p)
        out.extend(data.get(page_key, []))
        after = (((data.get("paging") or {}).get("next") or {}).get("after"))
        if not after:
            break
        p["after"] = after
    return out



def write_bronze_delta(records: list[dict], entity: str, mode: str = "append") -> None:
    """
    Write raw records to Delta as-is with an ingestion timestamp.
    """
    if not records:
        print(f"[{entity}] No records to write.")
        return

    ingest_ts = datetime.now(timezone.utc).isoformat()

    rdd = sc.parallelize([json.dumps(r) for r in records])
    df = spark.read.json(rdd)

    df = (
        df
        .withColumn("_ingested_at", F.lit(ingest_ts))
        .withColumn("_entity", F.lit(entity))
    )

    target_path = f"{BRONZE_BASE_PATH}/{entity}"
    (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .save(target_path)
    )

    # Optional UC registration
    if CATALOG and SCHEMA:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{entity}
            USING DELTA
            LOCATION '{target_path}'
        """)
    print(f"[{entity}] Wrote {df.count()} rows to {target_path}")



# Watermark table schema + bootstrap
watermark_schema = Tz.StructType([
    Tz.StructField("entity", Tz.StringType(), False),
    Tz.StructField("watermark_type", Tz.StringType(), False),  # e.g. hs_lastmodifieddate
    Tz.StructField("watermark_value", Tz.StringType(), True),  # store as ISO string or ms string
    Tz.StructField("updated_at", Tz.TimestampType(), True),
])

def ensure_watermark_table():
    if not any(f.path.rstrip("/") == WATERMARK_PATH for f in dbutils.fs.ls(WATERMARK_PATH.rsplit("/", 1)[0])):
        pass  # parent exists
    # Create if missing
    try:
        spark.read.format("delta").load(WATERMARK_PATH).limit(1).collect()
    except Exception:
        empty = spark.createDataFrame([], watermark_schema)
        empty.write.format("delta").mode("overwrite").save(WATERMARK_PATH)

ensure_watermark_table()

def get_watermark(entity: str, default_iso: str = "1970-01-01T00:00:00.000Z") -> str:
    df = spark.read.format("delta").load(WATERMARK_PATH)
    row = (
        df.filter(F.col("entity") == entity)
          .orderBy(F.col("updated_at").desc_nulls_last())
          .limit(1)
          .collect()
    )
    return row[0]["watermark_value"] if row else default_iso

def set_watermark(entity: str, watermark_type: str, watermark_value: str) -> None:
    now_ts = datetime.now(timezone.utc)
    df = spark.read.format("delta").load(WATERMARK_PATH)
    new_row = spark.createDataFrame([(entity, watermark_type, watermark_value, now_ts)], watermark_schema)

    # Simple overwrite-by-entity approach (small control table)
    remaining = df.filter(F.col("entity") != entity)
    out = remaining.unionByName(new_row)
    out.write.format("delta").mode("overwrite").save(WATERMARK_PATH)



# Configure which HubSpot entities/endpoints we can ingest with your scopes.
# (You can add/remove entries based on what your app actually needs.)
SOURCES = [
    # CRM objects that you explicitly scoped
    {"entity": "crm_owners", "type": "owners", "mode": "full"},
    {"entity": "crm_marketing_events", "type": "crm_object", "object": "marketing_events", "mode": "full"},
    {"entity": "crm_feedback_submissions", "type": "crm_object", "object": "feedback_submissions", "mode": "incremental"},
    {"entity": "crm_partner_clients", "type": "crm_object", "object": "partner-clients", "mode": "full"},
    {"entity": "crm_partner_services", "type": "crm_object", "object": "partner-services", "mode": "full"},
    {"entity": "crm_custom_objects", "type": "crm_custom", "mode": "optional"},  # requires knowing objectTypeIds

    # Meetings scheduling pages
    {"entity": "scheduler_meeting_links", "type": "meeting_links", "mode": "full"},

    # Campaign revenue reporting
    {"entity": "marketing_campaigns", "type": "campaigns", "mode": "full"},
    {"entity": "marketing_campaign_revenue", "type": "campaign_revenue", "mode": "full"},

    # Settings
    {"entity": "settings_currencies", "type": "currencies", "mode": "full"},

    # CMS
    {"entity": "cms_domains", "type": "domains", "mode": "full"},

    # Commerce tax rates
    {"entity": "tax_rates", "type": "tax_rates", "mode": "full"},

    # Communication preferences (definitions + optional status reads)
    {"entity": "comm_pref_definitions", "type": "comm_pref_definitions", "mode": "full"},
]



# COMMAND ----------

# Scope reference & guidance
# ==========================
# As instructed created one with access to all scopes, then filtered down to just the ones we need for this example.
# (You can also create a new app with just the scopes you need.) Stout-Pillow was the Hubspot I used for testing purposes


scope_notes = [
    # ---- Media Bridge ----
    ("media_bridge.read",
     "Read Media Bridge objects and settings for integrator apps.",
     "Use if you ingest media objects/consumption events into a warehouse; otherwise omit.",
     "Media Bridge API requires media_bridge.read/write to connect an app to an account."),  # [5](https://developers.hubspot.com/docs/api-reference/cms-media-bridge-v1/guide)

    ("media_bridge.write",
     "Write Media Bridge objects/events into HubSpot.",
     "Not needed for *ingestion*; needed only if you are pushing media objects or events into HubSpot.",
     "Media Bridge guide lists write scope for pushing media."),  # [5](https://developers.hubspot.com/docs/api-reference/cms-media-bridge-v1/guide)

    # ---- Tax rates ----
    ("tax_rates.read",
     "Retrieve tax rates configured in the HubSpot account.",
     "Use if you ingest quoting/invoicing/line item tax configuration; otherwise omit.",
     "Line items guide: tax_rates.read provides access to retrieve configured tax rates."),  # [6](https://developers.hubspot.com/docs/api-reference/crm-line-items-v3/guide)

    # ---- CRM Extensions / Actions / Timeline ----
    ("actions",
     "CRM Extensions 'actions' scope (custom actions on contact pages).",
     "Not required for data ingestion; keep only if your app renders/actions within HubSpot UI.",
     "Listed in HubSpot scopes reference as CRM Extensions access."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    ("timeline",
     "Timeline events API usage is for sending external events into HubSpot timelines.",
     "Not required for ingestion; used for *pushing* events that appear in CRM timelines.",
     "Timeline events guide describes sending data from external systems to display in activity timelines."),  # [8](https://developers.hubspot.com/docs/api-reference/crm-timeline-v3/guide)

    ("business-intelligence",
     "Analytics endpoints that sit on top of sources and email (HubSpot 'business-intelligence' scope).",
     "Use only if you plan to extract those analytics datasets; otherwise omit.",
     "Listed in HubSpot scopes reference."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    ("oauth",
     "OAuth-related scope used in HubSpot app configuration for OAuth-based apps.",
     "Used for authentication/installation flows; not a data-ingestion scope itself.",
     "HubSpot OAuth documentation describes OAuth as the auth method for apps; app configs commonly include 'oauth' in required scopes."),  # [9](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/oauth/working-with-oauth)[10](https://unified.to/blog/how_to_set_up_your_scopes_in_hubspot)

    # ---- Record images signed URLs ----
    ("record_images.signed_urls.read",
     "Read signed URLs for record images (scope name indicates signed URL access).",
     "Use if you need to download record images via signed URLs; otherwise omit.",
     "Check HubSpot scopes reference for exact endpoints covered by this scope."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Partner objects ----
    ("crm.objects.partner-clients.read",
     "Read partner client objects.",
     "Use if you ingest partner client object data; required for read access.",
     "Listed in HubSpot scopes reference under Partner client endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    ("crm.objects.partner-clients.write",
     "Write partner client objects.",
     "Not needed for ingestion; only for creating/updating partner client objects.",
     "Listed in HubSpot scopes reference under Partner client endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    ("crm.objects.partner-services.read",
     "Read partner service objects.",
     "Use if you ingest partner service object data; required for read access.",
     "Listed in HubSpot scopes reference under Partner service endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    ("crm.objects.partner-services.write",
     "Write partner service objects.",
     "Not needed for ingestion; only for creating/updating partner service objects.",
     "Listed in HubSpot scopes reference under Partner service endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Settings ----
    ("settings.currencies.read",
     "Read exchange rates and the current company currency for the portal.",
     "Use if you need currency configuration in your warehouse; otherwise omit.",
     "Listed in HubSpot scopes reference under account information endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Forms external integrations ----
    ("external_integrations.forms.access",
     "Rename, delete, and clone existing forms.",
     "Not needed for ingestion; keep only if your integration manages HubSpot forms.",
     "Listed in HubSpot scopes reference."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Feedback submissions (survey responses) ----
    ("crm.objects.feedback_submissions.read",
     "Read survey response (feedback_submissions) records (read-only API).",
     "Use if you ingest NPS/CSAT/CES/custom survey responses; otherwise omit.",
     "Feedback submissions guide describes retrieving survey responses via /crm/v3/objects/feedback_submissions."),  # [11](https://developers.hubspot.com/docs/api-reference/crm-feedback-submissions-v3/guide)

    # ---- Campaign revenue ----
    ("marketing.campaigns.revenue.read",
     "View revenue details and deal amounts attributed to marketing campaigns.",
     "Use if you ingest campaign attribution/revenue reporting; otherwise omit.",
     "Campaigns API guide: marketing.campaigns.revenue.read provides revenue details."),  # [12](https://developers.hubspot.com/docs/api-reference/marketing-campaigns-public-api-v3/guide)[13](https://developers.hubspot.com/docs/api-reference/marketing-campaigns-public-api-v3/campaign-reporting/get-marketing-v3-campaigns-campaignGuid-reports-revenue)

    # ---- Communication preferences ----
    ("communication_preferences.read",
     "Fetch subscription type definitions and a contact’s subscription preferences.",
     "Use if ingesting subscription definitions/statuses for compliance/marketing analytics.",
     "Subscription preferences v4 guide lists required scopes."),  # [14](https://developers.hubspot.com/docs/api-reference/communication-preferences-subscriptions-v4/guide)

    ("communication_preferences.write",
     "Update subscription preferences for a contact.",
     "Not needed for ingestion; keep only if your app updates preference statuses.",
     "Subscription preferences v4 guide lists required scopes."),  # [14](https://developers.hubspot.com/docs/api-reference/communication-preferences-subscriptions-v4/guide)

    ("communication_preferences.read_write",
     "Read + update subscription preferences (combined).",
     "Use only if you both ingest and update subscription preferences.",
     "Subscription preferences v4 guide lists read/write scopes."),  # [14](https://developers.hubspot.com/docs/api-reference/communication-preferences-subscriptions-v4/guide)

    # ---- Calling transcripts extension ----
    ("crm.extensions_calling_transcripts.read",
     "Read third-party calling transcript resources for calling extensions.",
     "Not typically used for ingestion; used in calling extension workflows.",
     "Third-party transcripts doc lists required scopes."),  # [15](https://developers.hubspot.com/docs/apps/legacy-apps/extensions/calling-extensions/third-party-transcripts)

    ("crm.extensions_calling_transcripts.write",
     "Create/push third-party transcripts into HubSpot (attach to call engagement).",
     "Not needed for ingestion; needed if you send transcripts into HubSpot.",
     "Third-party transcripts doc lists required scopes."),  # [15](https://developers.hubspot.com/docs/apps/legacy-apps/extensions/calling-extensions/third-party-transcripts)

    # ---- Owners ----
    ("crm.objects.owners.read",
     "Read CRM owner (user) details assigned to CRM records.",
     "Useful dimension table (owner lookups) during ingestion.",
     "Listed in HubSpot scopes reference under Owners endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Visitor identification tokens ----
    ("conversations.visitor_identification.tokens.create",
     "Create identification tokens for authenticated website visitors in chat widget contexts.",
     "Not needed for ingestion; keep only if you implement authenticated chat experiences.",
     "Listed in HubSpot scopes reference under Visitor Identification API."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Marketing events ----
    ("crm.objects.marketing_events.read",
     "Read marketing events object records.",
     "Use if ingesting marketing event data; otherwise omit.",
     "Listed in HubSpot scopes reference under Marketing events endpoints."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Meeting links ----
    ("scheduler.meetings.meeting-link.read",
     "Read meeting scheduling pages (meeting links).",
     "Use if ingesting meetings scheduler configuration/links.",
     "Meetings library shows listing meeting links at /scheduler/v3/meetings/meeting-links."),  # [16](https://developers.hubspot.com/docs/api-reference/library-meetings-v3/guide)[17](https://developers.hubspot.com/docs/api-reference/scheduler-meetings-v3/meetings-links/get-scheduler-v3-meetings-meeting-links)

    # ---- Custom objects ----
    ("crm.objects.custom.read",
     "Read custom object records (Enterprise feature).",
     "Use if your portal uses custom objects and you ingest them.",
     "Scopes reference describes crm.objects.custom.read for custom objects."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    ("crm.objects.custom.write",
     "Write custom object records.",
     "Not needed for ingestion; only if your app creates/updates custom objects.",
     "Scopes reference describes crm.objects.custom.write for custom objects."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)

    # ---- Domains ----
    ("cms.domains.read",
     "List connected domains in an account.",
     "Use if you ingest CMS configuration / domain inventory; otherwise omit.",
     "Scopes reference includes cms.domains.read description."),  # [7](https://developers.hubspot.com/docs/apps/developer-platform/build-apps/authentication/scopes)
]

scope_df = spark.createDataFrame(scope_notes, ["scope", "what_it_allows", "use_for_ingestion", "source_note"])
display(scope_df)

# (Optional) quick check: print all scopes requested
print("Scopes list captured in this notebook:", [s[0] for s in scope_notes])