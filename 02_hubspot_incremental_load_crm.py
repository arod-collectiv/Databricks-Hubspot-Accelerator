# Databricks notebook source
# MAGIC %run "./00_hubspot_config_and_scopes"

# COMMAND ----------

# 02_hubspot_incremental_load_crm

def crm_search_since(object_name: str, since_iso: str, properties: list[str] | None = None, limit: int = 100) -> list[dict]:
    """
    Uses CRM Search API to pull records with hs_lastmodifieddate >= since.
    Search API docs: /crm/v3/objects/{object}/search. [4](https://developers.hubspot.com/docs/api-reference/search/guide)
    Community guidance: hs_lastmodifieddate can be used for incremental polling. [18](https://community.hubspot.com/t5/APIs-Integrations/How-to-use-crm-v3-objects-deals-or-another-api-to-get-the-deals/m-p/358609)
    """
    after = None
    out = []

    # HubSpot expects ms timestamps for numeric comparisons in many places;
    # we'll send ISO if supported, otherwise convert to ms in your environment if needed.
    while True:
        body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GTE",
                    "value": since_iso
                }]
            }],
            "sorts": ["hs_lastmodifieddate"],
            "limit": limit
        }
        if properties:
            body["properties"] = properties
        if after:
            body["after"] = after

        data = hs_request("POST", f"/crm/v3/objects/{object_name}/search", json_body=body)
        results = data.get("results", [])
        out.extend(results)

        after = (((data.get("paging") or {}).get("next") or {}).get("after"))
        if not after:
            break

    return out

def incremental_object(object_name: str, entity: str):
    since = get_watermark(entity)
    rows = crm_search_since(object_name, since_iso=since, properties=None, limit=100)

    write_bronze_delta(rows, entity, mode="append")

    # Update watermark to max hs_lastmodifieddate found
    if rows:
        max_wm = None
        for r in rows:
            # hs_lastmodifieddate is a property on returned objects
            props = r.get("properties") or {}
            v = props.get("hs_lastmodifieddate")
            if v and (max_wm is None or v > max_wm):
                max_wm = v
        if max_wm:
            set_watermark(entity, "hs_lastmodifieddate", max_wm)
            print(f"[{entity}] watermark -> {max_wm}")
    else:
        print(f"[{entity}] no new/updated rows since {since}")


# Incremental candidates (search-friendly)
incremental_object("feedback_submissions", "crm_feedback_submissions")

# If you later add scopes for contacts/companies/deals/tickets, you can add:
# incremental_object("contacts","crm_contacts")
# incremental_object("companies","crm_companies")
# incremental_object("deals","crm_deals")
# incremental_object("tickets","crm_tickets")

print("Incremental load complete.")