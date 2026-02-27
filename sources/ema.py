import hashlib
import json
import requests
from datetime import datetime, timezone

EMA_MEDICINES_JSON_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines_json-report_en.json"

def _hash_id(*parts: str) -> str:
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def _find_medicine_records(obj):
    """
    EMA JSON can be nested; we try to find the list of medicine dicts by searching for dicts
    that look like medicines (contain ema_product_number + name_of_medicine/medicine_url).
    """
    records = []

    def looks_like_medicine(d):
        return isinstance(d, dict) and (
            "ema_product_number" in d and ("name_of_medicine" in d or "medicine_url" in d)
        )

    def walk(x):
        if isinstance(x, dict):
            if looks_like_medicine(x):
                records.append(x)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(obj)
    return records

def _is_under_review(rec: dict) -> bool:
    """
    Heuristic:
    - Keep medicines that are not yet authorised / have pending evaluation.
    EMA provides medicine_status and opinion_status in the dataset.  [oai_citation:2‡European Medicines Agency (EMA)](https://www.ema.europa.eu/en/about-us/about-website/download-website-data-json-data-format)
    We'll include common "under evaluation" / "pending decision" style statuses.
    """
    ms = (rec.get("medicine_status") or "").strip().lower()
    os_ = (rec.get("opinion_status") or "").strip().lower()

    # Broad, safe inclusions for "under review"
    keywords = [
        "under evaluation",
        "under review",
        "ongoing",
        "pending",
        "submitted",
        "application",   # some records mention application status textually
        "withdrawn",     # if you prefer to exclude withdrawn later, we can filter
        "refused",
    ]

    # Exclude clearly authorised products
    if "authorised" in ms or "authorized" in ms:
        return False
    if "authorised" in os_ or "authorized" in os_:
        return False

    return any(k in ms for k in keywords) or any(k in os_ for k in keywords)

def fetch_ema_under_review(timeout_s: int = 60):
    now = datetime.now(timezone.utc)

    r = requests.get(EMA_MEDICINES_JSON_URL, timeout=timeout_s)
    r.raise_for_status()

    # Some servers return JSON with BOM; strip if needed
    text = r.text.lstrip("\ufeff")
    data = json.loads(text)

    recs = _find_medicine_records(data)

    events = []
    for rec in recs:
        if not _is_under_review(rec):
            continue

        name = (rec.get("name_of_medicine") or "").strip()
        ema_no = (rec.get("ema_product_number") or "").strip()
        inn = (rec.get("international_non_proprietary_name_common_name") or "").strip()
        active = (rec.get("active_substance") or "").strip()
        company = (rec.get("marketing_authorisation_developer_applicant_holder") or "").strip()
        indication = (rec.get("therapeutic_indication") or "").strip()
        medicine_url = (rec.get("medicine_url") or "").strip()
        last_updated = (rec.get("last_updated_date") or "").strip()

        asset = inn or active or name

        # Stable ID: one row per EMA product (like we did per NCT)
        # If you prefer per procedure/update later, we can change it.
        event_id = _hash_id("ema", ema_no or medicine_url or name)

        title = name or asset
        summary = f"medicine_status={rec.get('medicine_status','')}; opinion_status={rec.get('opinion_status','')}".strip()

        events.append({
            "event_id": event_id,
            "date_detected": now.isoformat(),
            "source": "ema",
            "signal_type": "ema_under_review",
            "asset_name": asset,
            "company": company,
            "indication_raw": indication,
            "phase": "",
            "trial_id": "",
            "start_date": "",
            "last_update": last_updated,
            "geography": "EU",
            "source_url": medicine_url,
            "title": title,
            "summary": summary,
        })

    return events
