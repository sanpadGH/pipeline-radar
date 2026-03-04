import hashlib
import requests
import zipfile
import io
import json
from datetime import datetime, timezone

FDA_DOWNLOAD_URL = "https://api.fda.gov/download.json"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_fda_under_review():
    now = datetime.now(timezone.utc)

    # Get download manifest
    r = requests.get(FDA_DOWNLOAD_URL, timeout=30)
    r.raise_for_status()
    manifest = r.json()

    # Find drugsfda partitions
    partitions = (
        manifest.get("results", {})
        .get("drug", {})
        .get("drugsfda", {})
        .get("partitions", [])
    )

    if not partitions:
        print("Warning: could not find FDA drugsfda partitions")
        return []

    events = []

    for partition in partitions:
        url = partition.get("file")
        if not url:
            continue

        r = requests.get(url, timeout=120)
        r.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for fname in z.namelist():
                if not fname.endswith(".json"):
                    continue
                with z.open(fname) as f:
                    data = json.load(f)

                for record in data.get("results", []):
                    appl_type = record.get("application_number", "")[:3]
                    if appl_type not in ("NDA", "BLA"):
                        continue

                    app_no = record.get("application_number", "").strip()
                    sponsor = record.get("sponsor_name", "").strip()

                    products = record.get("products", []) or []
                    drug_name = products[0].get("brand_name", "") if products else ""
                    if not drug_name:
                        drug_name = products[0].get("generic_name", "") if products else ""

                    for sub in record.get("submissions", []) or []:
                        sub_status = sub.get("submission_status", "").strip()
                        sub_type = sub.get("submission_type", "").strip()
                        sub_no = sub.get("submission_number", "").strip()
                        action_date = sub.get("submission_status_date", "").strip()

                        if sub_status == "Filed":
                            signal_type = "fda_under_review"
                        elif sub_status == "AP":
                            signal_type = "fda_approval"
                        else:
                            continue

                        if sub_type not in ("ORIG", "SUPPL"):
                            continue

                        event_id = _hash_id("fda", app_no, sub_no, sub_status)

                        events.append({
                            "event_id": event_id,
                            "date_detected": now.isoformat(),
                            "source": "fda",
                            "signal_type": signal_type,
                            "asset_name": drug_name,
                            "company": sponsor,
                            "indication_raw": "",
                            "phase": "",
                            "trial_id": app_no,
                            "start_date": "",
                            "last_update": action_date,
                            "geography": "US",
                            "source_url": f"https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo={app_no.replace('NDA','').replace('BLA','')}",
                            "title": drug_name,
                            "summary": f"{app_no}/{sub_no}; Status: {sub_status}; Date: {action_date}",
                        })

    print(f"FDA fetched: {len(events)}")
    return events
