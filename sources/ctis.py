import hashlib
import json
import requests
from datetime import datetime, timezone

OVERVIEW_URL = "https://euclinicaltrials.eu/ctis-public-api/search"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_ctis_phase3(page_size: int = 200, max_pages: int = 10):
    now = datetime.now(timezone.utc)

    events = []
    page = 1
    next_page = True

    while next_page and page <= max_pages:
        payload = {
            "pagination": {"page": page, "size": page_size},
            "sort": {"property": "decisionDate", "direction": "DESC"},
            "searchCriteria": {
                "containAll": None,
                "containAny": None,
                "containNot": None,
                "title": None,
                "number": None,
                "status": None,
                "medicalCondition": None,
                "sponsor": None,
                "endPoint": None,
                "productName": None,
                "trialPhaseCode": None,
                "eudraCtCode": None,
                "trialRegion": None,
            },
        }

        r = requests.post(
            OVERVIEW_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        for t in data.get("data", []):
            trial_phase = (t.get("trialPhase") or "").lower()
            if "phase iii" not in trial_phase and "phase 3" not in trial_phase:
                continue

            ct_number = (t.get("ctNumber") or "").strip()
            title = (t.get("ctTitle") or "").strip()
            sponsor = (t.get("sponsor") or "").strip()
            condition = (t.get("conditions") or "").strip()
            last_updated = (t.get("lastUpdated") or "").strip()

            if not ct_number:
                continue

            event_id = _hash_id("ctis", ct_number)

            events.append({
                "event_id": event_id,
                "date_detected": now.isoformat(),
                "source": "ctis",
                "signal_type": "phase3_trial",
                "asset_name": "",
                "company": sponsor,
                "indication_raw": condition,
                "phase": "3",
                "trial_id": ct_number,
                "start_date": "",
                "last_update": last_updated,
                "geography": "EU",
                "source_url": f"https://euclinicaltrials.eu/ctis-public/search/{ct_number}",
                "title": title,
                "summary": f"trialPhase={t.get('trialPhase','')}; decisionDate={t.get('decisionDateOverall','')}",
            })

        next_page = data.get("pagination", {}).get("nextPage", False)
        page += 1

    return events
