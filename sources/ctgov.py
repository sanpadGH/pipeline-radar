import hashlib
from datetime import datetime, timezone
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

    params = {
        "query.term": "AREA[Phase]PHASE3",
        "pageSize": page_size,
        "format": "json",
        "sort": "LastUpdatePostDate:desc",
    }

    events = []
    next_page_token = None
    pages = 0
    stop_early = False

    while pages < max_pages and not stop_early:
        if next_page_token:
            params["pageToken"] = next_page_token
        elif "pageToken" in params:
            del params["pageToken"]

        r = requests.get(CTGOV_API, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        for s in data.get("studies", []):
            ps = s.get("protocolSection", {})

            last_update = _safe_get(ps, ["statusModule", "lastUpdatePostDateStruct", "date"])

            if not last_update:
                continue

            try:
                lu = isoparse(last_update).date()
                delta = (now.date() - lu).days
                if delta > days_back:
                    stop_early = True
                    break
            except Exception:
                continue

            nct = _safe_get(ps, ["identificationModule", "nctId"])
            title = _safe_get(ps, ["identificationModule", "briefTitle"])
            sponsor = _safe_get(ps, ["sponsorCollaboratorsModule", "leadSponsor", "name"])
            conds = _safe_get(ps, ["conditionsModule", "conditions"], default=[]) or []
            interventions = _safe_get(ps, ["armsInterventionsModule", "interventions"], default=[]) or []
            asset_name = interventions[0].get("name", "") if interventions else ""
            overall = _safe_get(ps, ["statusModule", "overallStatus"])

            event_id = _hash_id("ctgov", nct, last_update)

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
                "geography": "",
                "source_url": f"https://clinicaltrials.gov/study/{nct}",
                "title": title,
                "summary": f"Status: {overall}",
            })

        next_page_token = data.get("nextPageToken")
        pages += 1
        if not next_page_token:
            break

    return events
