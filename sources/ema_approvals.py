import hashlib
import pandas as pd
from datetime import datetime, timezone

EPAR_URL = "https://www.ema.europa.eu/en/documents/report/medicines-output-medicines-report_en.xlsx"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_ema_approvals():
    now = datetime.now(timezone.utc)
    cutoff_year = now.year - 1

    try:
        df = pd.read_excel(EPAR_URL, header=8)
    except Exception as ex:
        print(f"Warning: could not load EMA approvals dataset: {ex}")
        return []

    df = df[df["Category"] == "Human"]
    df = df[df["Medicine status"] == "Authorised"]
    df = df[df["Generic"].str.strip().str.lower() != "yes"]
    df = df[df["Biosimilar"].str.strip().str.lower() != "yes"]

    df["_auth_date"] = pd.to_datetime(df["Marketing authorisation date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["_auth_date"])
    df = df[df["_auth_date"].dt.year >= cutoff_year]

    events = []

    for _, row in df.iterrows():
        auth_date = row["_auth_date"]
        auth_date_str = auth_date.strftime("%Y-%m-%d")

        inn = str(row.get("International non-proprietary name (INN) / common name", "") or "").strip()
        medicine_name = str(row.get("Name of medicine", "") or "").strip()
        company = str(row.get("Marketing authorisation developer / applicant / holder", "") or "").strip()
        indication = str(row.get("Therapeutic indication", "") or "").strip()
        product_number = str(row.get("EMA product number", "") or "").strip()
        therapeutic_area = str(row.get("Pharmacotherapeutic group\n(human)", "") or "").strip()
        orphan = str(row.get("Orphan medicine", "") or "").strip()
        url = str(row.get("Medicine URL", "") or "").strip()

        if not inn and not medicine_name:
            continue

        event_id = _hash_id("ema_approval", product_number or inn or medicine_name)

        events.append({
            "event_id": event_id,
            "date_detected": now.isoformat(),
            "source": "ema",
            "signal_type": "ema_approval",
            "asset_name": inn or medicine_name,
            "company": company,
            "indication_raw": indication,
            "phase": "",
            "trial_id": product_number,
            "start_date": "",
            "last_update": auth_date_str,
            "geography": "EU",
            "source_url": url,
            "title": medicine_name,
            "summary": f"Auth date: {auth_date_str}; TA: {therapeutic_area}; Orphan: {orphan}",
        })

    print(f"EMA approvals fetched: {len(events)}")
    return events