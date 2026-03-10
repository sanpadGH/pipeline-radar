"""
fda_enrich_indication.py
------------------------
Post-process FDA approval events to populate indication_raw
using the openFDA drug label API.

Integration in tracker.py:
    from fda_enrich_indication import enrich_fda_indications
    fda_events = fetch_fda_under_review()
    fda_events = enrich_fda_indications(fda_events)   # <-- add this line

openFDA rate limit: 240 req/min without API key, 1000/min with key.
Set OPENFDA_API_KEY env var to use authenticated requests.
"""

import re
import time
import requests

OPENFDA_LABEL_URL = "https://api.fda.gov/drug/label.json"

# Regex to extract NDA/BLA number from summary field
# e.g. "NDA219616/1; Brand: ..." or "BLA761464/1; Brand: ..."
_NDA_RE = re.compile(r'\b(NDA|BLA)(\d{6})', re.IGNORECASE)


def _extract_nda(summary: str) -> tuple[str, str]:
    """Returns (type, number) e.g. ('NDA', '219616') or ('', '')"""
    m = _NDA_RE.search(summary or "")
    if m:
        return m.group(1).upper(), m.group(2)
    return "", ""


def _fetch_indication(app_type: str, app_number: str) -> str:
    """
    Query openFDA label API for indications_and_usage.
    Returns cleaned indication string or empty string on failure.
    """
    if not app_type or not app_number:
        return ""

    # openFDA application_number format: "NDA219616" or "BLA761464"
    query = f'openfda.application_number:"{app_type}{app_number}"'

    try:
        r = requests.get(
            OPENFDA_LABEL_URL,
            params={"search": query, "limit": 1},
            timeout=10
        )
        if r.status_code != 200:
            return ""

        data = r.json()
        results = data.get("results", [])
        if not results:
            return ""

        label = results[0]

        # Try indications_and_usage first, then purpose, then description
        for field in ["indications_and_usage", "purpose", "description"]:
            val = label.get(field)
            if val and isinstance(val, list) and val[0]:
                text = val[0].strip()
                # Truncate to first sentence or 300 chars to keep it clean
                first_sentence = re.split(r'(?<=[.!?])\s', text)[0]
                return first_sentence[:300]

        return ""

    except Exception:
        return ""


def enrich_fda_indications(events: list[dict], delay: float = 0.25) -> list[dict]:
    """
    For each FDA event with missing indication_raw, query openFDA label API.
    Modifies events in place and returns the list.

    delay: seconds between API calls to avoid rate limiting (openFDA: 240 req/min)
    """
    enriched = 0
    skipped = 0

    for event in events:
        if event.get("source") != "fda":
            continue

        # Skip if already has indication
        if event.get("indication_raw") and str(event["indication_raw"]).strip():
            skipped += 1
            continue

        summary = event.get("summary", "")
        app_type, app_number = _extract_nda(summary)

        if not app_number:
            # Try extracting from asset_name as fallback
            app_type, app_number = _extract_nda(event.get("asset_name", ""))

        if not app_number:
            continue

        indication = _fetch_indication(app_type, app_number)

        if indication:
            event["indication_raw"] = indication
            enriched += 1

        time.sleep(delay)  # Rate limiting

    print(f"FDA indication enrichment: {enriched} enriched, {skipped} already had indication")
    return events
