import hashlib
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
import requests

CTGOV_API = "https://clinicaltrials.gov/api/v2/studies"

def _safe_get(dct, path, default=""):
    cur = dct
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def _hash_id(*parts: str) -> str:
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_phase3_recent(days_back: int = 90, page_size: int = 100, max_pages: int = 20):
    now = datetime.now(timezone.utc)
    completion_cutoff_max = (now + timedelta(days=24 * 30)).strftime("%Y-%m-%d")
    completion_cutoff_min = now.strftime("%Y-%m-%d")

    params = {
        "query.term": "AREA[Phase]PHASE3",
        "filter.advanced": f"AREA[PrimaryCompletionDate]RANGE[{completion_cutoff_min},{completion_cutoff_max}]",
        "pageSize": page_size,
        "format": "json",
        "sort": "PrimaryCompletionDate:asc",
    }

    events = []
    next_page_token = None
    pages = 0

    while pages < max_pages:
        if next_page_token:
            params["pageToken"] = next_page_token
        elif "pageToken" in params:
            del params["pageToken"]

        r = requests.get(CTGOV_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        for s in data.get("studies", []):
            ps = s.get("protocolSection", {})

            nct = _safe_get(ps, ["identificationModule", "nctId"])
            title = _safe_get(ps, ["identificationModule", "briefTitle"])
            sponsor = _safe_get(ps, ["sponsorCollaboratorsModule", "leadSponsor", "name"])
            conds = _safe_get(ps, ["conditionsModule", "conditions"], default=[]) or []
            interventions = _safe_get(ps, ["armsInterventionsModule", "interventions"], default=[]) or []
            asset_name = interventions[0].get("name", "") if interventions else ""
            locations = _safe_get(ps, ["contactsLocationsModule", "locations"], default=[]) or []
            countries = list({loc.get("country", "") for loc in locations if loc.get("country")})
            geography = ", ".join(sorted(countries)) if countries else ""
            overall = _safe_get(ps, ["statusModule", "overallStatus"])
            last_update = _safe_get(ps, ["statusModule", "lastUpdatePostDateStruct", "date"])
            primary_completion = _safe_get(ps, ["statusModule", "primaryCompletionDateStruct", "date"])

            event_id = _hash_id("ctgov", nct, primary_completion)

            events.append({
                "event_id": event_id,
                "date_detected": now.isoformat(),
                "source": "ctgov",
                "signal_type": "phase3_trial",
                "asset_name": asset_name,
                "company": sponsor,
                "indication_raw": ", ".join(conds),
                "phase": "3",
                "trial_id": nct,
                "start_date": _safe_get(ps, ["statusModule", "startDateStruct", "date"]),
                "last_update": last_update,
                "geography": geography,
                "source_url": f"https://clinicaltrials.gov/study/{nct}",
                "title": title,
                "summary": f"Status: {overall}; Primary completion: {primary_completion}",
            })

        next_page_token = data.get("nextPageToken")
        pages += 1
        if not next_page_token:
            break

    print(f"CTGOV fetched: {len(events)}")
    return events