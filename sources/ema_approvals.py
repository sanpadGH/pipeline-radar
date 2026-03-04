import hashlib
import pandas as pd
import requests
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

    print("EPAR shape:", df.shape)
    print("EPAR columns:", list(df.columns)[:10])
    print("Authorisation status values:", df["Authorisation status"].value_counts().head() if "Authorisation status" in df.columns else "COLUMN NOT FOUND")
    print("Sample auth dates:", df["Marketing authorisation date"].dropna().head() if "Marketing authorisation date" in df.columns else "COLUMN NOT FOUND")

    events = []

    for _, row in df.iterrows():
        status = str(row.get("Authorisation status", "") or "").strip()
        if status.lower() != "authorised":
            continue

        generic = str(row.get("Generic", "") or "").strip().lower()
        biosimilar = str(row.get("Biosimilar", "") or "").strip().lower()
        if generic == "yes" or biosimilar == "yes":
            continue

        auth_date = row.get("Marketing authorisation date", None)
        if auth_date is None:
            continue

        try:
            if hasattr(auth_date, 'year'):
                auth_year = auth_date.year
                auth_date_str = auth_date.strftime("%Y-%m-%d")
            else:
                import dateutil.parser
                parsed = dateutil.parser.parse(str(auth_date))
                auth_year = parsed.year
                auth_date_str = parsed.strftime("%Y-%m-%d")
        except Exception:
            continue

        if auth_year < cutoff_year:
            continue

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
            "summary": f"Auth date: {auth_date_str}; TA: {therapeutic_area}; Orphan: {orphan}",
        })

    print(f"EMA approvals fetched: {len(events)}")
    return events