import hashlib
import pandas as pd
from datetime import datetime, timezone

EPAR_URL = "https://www.ema.europa.eu/system/files/documents/other/medicines_output_european_public_assessment_reports_en.xlsx"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_ema_approvals():
    now = datetime.now(timezone.utc)
    cutoff_year = now.year - 1

    try:
        df = pd.read_excel(EPAR_URL, header=8)
    except Exception as ex:
        print(f"Warning: could not load EPAR dataset: {ex}")
        return []

    # Filter authorised only
    df = df[df["Authorisation status"] == "Authorised"]

    # Filter humans only
    df = df[df["Category"] == "Human"]

    print("Max Decision date:", pd.to_datetime(df["Decision date"], errors="coerce").max())
    print("Max Marketing auth date:", pd.to_datetime(df["Marketing authorisation date"], errors="coerce").max())

    # Filter generics and biosimilars
    df = df[df["Generic"].str.lower() != "yes"]
    df = df[df["Biosimilar"].str.lower() != "yes"]

    # Filter by Decision date >= cutoff
    df = df.dropna(subset=["Decision date"])
    df["_decision_year"] = pd.to_datetime(df["Decision date"], errors="coerce").dt.year
    df = df[df["_decision_year"] >= cutoff_year]

    print(f"EMA approvals after filters: {len(df)}")
    if len(df) > 0:
        print("Sample filtered:", df[["Medicine name", "Decision date", "Generic", "Biosimilar"]].head(5).to_string())

    events = []

    for _, row in df.iterrows():
        decision_date = pd.to_datetime(row.get("Decision date"), errors="coerce")
        if pd.isna(decision_date):
            continue

        auth_date_str = decision_date.strftime("%Y-%m-%d")

        inn = str(row.get("International non-proprietary name (INN) / common name", "") or "").strip()
        medicine_name = str(row.get("Medicine name", "") or "").strip()
        company = str(row.get("Marketing authorisation holder/company name", "") or "").strip()
        indication = str(row.get("Condition / indication", "") or "").strip()
        product_number = str(row.get("Product number", "") or "").strip()
        therapeutic_area = str(row.get("Human pharmacotherapeutic group", "") or "").strip()
        orphan = str(row.get("Orphan medicine", "") or "").strip()
        url = str(row.get("URL", "") or "").strip()

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
            "summary": f"Decision date: {auth_date_str}; TA: {therapeutic_area}; Orphan: {orphan}",
        })

    print(f"EMA approvals fetched: {len(events)}")
    return events