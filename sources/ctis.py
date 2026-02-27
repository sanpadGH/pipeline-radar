import requests
import hashlib
from datetime import datetime, timezone

CTIS_SEARCH_URL = "https://euclinicaltrials.eu/ctis-public-api/search"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode()).hexdigest()[:20]

def fetch_ctis_phase3():

    now = datetime.now(timezone.utc)

    payload = {
        "query": "*",
        "filters": {
            "trialPhase": ["Phase 3"]
        },
        "size": 100
    }

    r = requests.post(CTIS_SEARCH_URL, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()

    events = []

    for t in data.get("hits", []):

        eudract = t.get("eudractNumber", "")
        title = t.get("title", "")
        sponsor = t.get("sponsorName", "")
        condition = t.get("medicalCondition", "")
        nct = t.get("nctNumber", "")

        trial_key = eudract or nct

        if not trial_key:
            continue

        event_id = _hash_id("ctis", trial_key)

        events.append({
            "event_id": event_id,
            "date_detected": now.isoformat(),
            "source": "ctis",
            "signal_type": "phase3_trial",
            "asset_name": "",
            "company": sponsor,
            "indication_raw": condition,
            "phase": "3",
            "trial_id": trial_key,
            "start_date": "",
            "last_update": "",
            "geography": "EU",
            "source_url": "",
            "title": title,
            "summary": f"EudraCT: {eudract} NCT: {nct}",
        })

    return events
