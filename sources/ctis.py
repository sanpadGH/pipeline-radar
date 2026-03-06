import hashlib
import json
import time
import requests
from datetime import datetime, timezone

OVERVIEW_URL = "https://euclinicaltrials.eu/ctis-public-api/search"
RETRIEVE_URL = "https://euclinicaltrials.eu/ctis-public-api/retrieve"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def _extract_active_substance(detail):
    try:
        products = (
            detail.get("authorizedApplication", {})
            .get("authorizedPartI", {})
            .get("products", [])
        )
        for p in products:
            if p.get("mpRoleInTrial") == "1":
                name = (
                    p.get("productDictionaryInfo", {})
                    .get("activeSubstanceName", "")
                )
                if name and name.upper() != "N/A":
                    return name.strip()
    except Exception:
        pass
    return ""

def _extract_start_date(detail):
    try:
        return (detail.get("decisionDate") or "")[:10]
    except Exception:
        return ""

def enrich_ctis_trials(trials, cache):
    new_cache = {}
    enriched = []

    for t in trials:
        ct_number = t["id"]

        if ct_number in cache:
            t["asset_name"] = cache[ct_number]["asset_name"]
            t["start_date"] = cache[ct_number]["start_date"]
            enriched.append(t)
            continue

        try:
            r = requests.get(f"{RETRIEVE_URL}/{ct_number}", timeout=30)
            r.raise_for_status()
            detail = r.json()
            asset = _extract_active_substance(detail)
            start = _extract_start_date(detail)
        except Exception as ex:
            print(f"Warning: could not retrieve CTIS {ct_number}: {ex}")
            asset = ""
            start = ""

        t["asset_name"] = asset
        t["start_date"] = start
        new_cache[ct_number] = {"asset_name": asset, "start_date": start}
        enriched.append(t)
        time.sleep(0.5)

    print(f"CTIS enriched: {len(enriched)} trials, {len(new_cache)} new cache entries")
    return enriched, new_cache

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
            decision_date = (t.get("decisionDateOverall") or "").strip()

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
                "id": ct_number,
                "start_date": decision_date,
                "last_update": last_updated,
                "geography": "EU",
                "source_url": f"https://euclinicaltrials.eu/ctis-public/view/{ct_number}",
                "title": title,
                "summary": f"trialPhase={t.get('trialPhase','')}; decisionDate={decision_date}",
            })

        next_page = data.get("pagination", {}).get("nextPage", False)
        page += 1

    print(f"CTIS fetched: {len(events)}")
    return events