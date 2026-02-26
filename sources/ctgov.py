import hashlib
from datetime import datetime, timezone
from dateutil.parser import isoparse
import requests
import time

BASE = "https://clinicaltrials.gov/api/v2"

def _safe_get(dct, path, default=""):
    cur = dct
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def _hash_id(*parts: str) -> str:
    return hashlib.sha256("||".join([p or "" for p in parts]).encode()).hexdigest()[:20]

def _get_study_detail(nct_id: str) -> dict:
    # This endpoint returns the full study record including modules
    url = f"{BASE}/studies/{nct_id}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

def extract_geography(full_study: dict) -> str:
    ps = full_study.get("protocolSection", {})
    locations = _safe_get(ps, ["contactsLocationsModule", "locations"], default=[]) or []
    countries = {loc.get("country") for loc in locations if isinstance(loc, dict) and loc.get("country")}
    countries = sorted([c for c in countries if c])

    if not countries:
        return ""
    if len(countries) == 1:
        return countries[0]
    return "Global"

def extract_asset_name(full_study: dict) -> str:
    ps = full_study.get("protocolSection", {})
    interventions = _safe_get(ps, ["armsInterventionsModule", "interventions"], default=[]) or []

    # Prefer DRUG interventions
    drug_names = []
    other_names = []

    for itv in interventions:
        if not isinstance(itv, dict):
            continue
        name = itv.get("name")
        if not name:
            continue
        if itv.get("type") == "DRUG":
            drug_names.append(name)
        else:
            other_names.append(name)

    if drug_names:
        return drug_names[0]
    if other_names:
        return other_names[0]
    return ""

def fetch_phase3_recent(days_back: int = 7, page_size: int = 100, max_pages: int = 3, sleep_s: float = 0.2):
    """
    1) Search Phase 3
    2) Filter by last_update in last N days (based on search payload)
    3) For each matching NCT, fetch full study details to extract geography + intervention
    """
    now = datetime.now(timezone.utc)

    search_url = f"{BASE}/studies"
    params = {
        "query.term": "AREA[Phase]PHASE3",
        "pageSize": page_size,
        "format": "json",
    }

    events = []
    next_page_token = None
    pages = 0

    while pages < max_pages:
        if next_page_token:
            params["pageToken"] = next_page_token

        r = requests.get(search_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        for s in data.get("studies", []):
            ps = s.get("protocolSection", {})

            nct = _safe_get(ps, ["identificationModule", "nctId"])
            if not nct:
                continue

            title = _safe_get(ps, ["identificationModule", "briefTitle"])
            sponsor = _safe_get(ps, ["sponsorCollaboratorsModule", "leadSponsor", "name"])
            conds = _safe_get(ps, ["conditionsModule", "conditions"], default=[]) or []
            start_date = _safe_get(ps, ["statusModule", "startDateStruct", "date"])
            last_update = _safe_get(ps, ["statusModule", "lastUpdatePostDateStruct", "date"])

            # recency filter based on search payload
            if not last_update:
                continue
            try:
                lu = isoparse(last_update).date()
                if (now.date() - lu).days > days_back:
                    continue
            except Exception:
                continue

            # fetch full detail to get interventions + locations
            try:
                full = _get_study_detail(nct)
                geography = extract_geography(full)
                asset = extract_asset_name(full)
            except Exception:
                geography = ""
                asset = ""

            # be gentle with rate limits
            time.sleep(sleep_s)

            event_id = _hash_id("ctgov", nct, last_update)

            events.append({
                "event_id": event_id,
                "date_detected": now.isoformat(),
                "source": "ctgov",
                "signal_type": "phase3_trial",
                "asset_name": asset,
                "company": sponsor,
                "indication_raw": ", ".join(conds),
                "phase": "3",
                "trial_id": nct,
                "start_date": start_date,
                "last_update": last_update,
                "geography": geography,
                "source_url": f"https://clinicaltrials.gov/study/{nct}",
                "title": title,
                "summary": ""
            })

        next_page_token = data.get("nextPageToken")
        pages += 1
        if not next_page_token:
            break

    return events
