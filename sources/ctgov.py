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
    h = hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()
    return h[:20]

def fetch_phase3_recent(days_back: int = 7, page_size: int = 100):
    now = datetime.now(timezone.utc)

    params = {
        "query.term": "AREA[Phase]PHASE3",
        "pageSize": page_size,
        "format": "json",
    }

    r = requests.get(CTGOV_API, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    events = []

    for s in data.get("studies", []):
        ps = s.get("protocolSection", {})

        nct = _safe_get(ps, ["identificationModule", "nctId"])
        title = _safe_get(ps, ["identificationModule", "briefTitle"])
        sponsor = _safe_get(ps, ["sponsorCollaboratorsModule", "leadSponsor", "name"])
        conds = _safe_get(ps, ["conditionsModule", "conditions"], default=[]) or []
        last_update = _safe_get(ps, ["statusModule", "lastUpdatePostDateStruct", "date"])

        if not last_update:
            continue

        try:
            lu = isoparse(last_update).date()
            if (now.date() - lu).days > days_back:
                continue
        except:
            continue

        event_id = _hash_id("ctgov", nct, last_update)

        events.append({
            "event_id": event_id,
            "date_detected": now.isoformat(),
            "source": "ctgov",
            "signal_type": "phase3_trial",
            "asset_name": "",
            "company": sponsor,
            "indication_raw": ", ".join(conds),
            "phase": "3",
            "trial_id": nct,
            "start_date": "",
            "last_update": last_update,
            "geography": "",
            "source_url": f"https://clinicaltrials.gov/study/{nct}",
            "title": title,
            "summary": ""
        })

    return events
