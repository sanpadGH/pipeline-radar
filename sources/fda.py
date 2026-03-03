import hashlib
import requests
import zipfile
import io
import csv
from datetime import datetime, timezone

FDA_SUBMISSIONS_URL = "https://api.fda.gov/download.json"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_fda_under_review():
    now = datetime.now(timezone.utc)

    # Get download manifest
    r = requests.get(FDA_SUBMISSIONS_URL, timeout=30)
    r.raise_for_status()
    manifest = r.json()

    # Find drug submissions dataset
    submissions_url = None
    for dataset in manifest.get("results", {}).get("drug", {}).get("drugsfda", {}).get("partitions", []):
        submissions_url = dataset.get("file")
        break

    if not submissions_url:
        print("Warning: could not find FDA submissions URL")
        return []

    # Download and extract
    r = requests.get(submissions_url, timeout=60)
    r.raise_for_status()

    events = []

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # Find submissions file
        sub_file = [f for f in z.namelist() if "Submissions" in f]
        app_file = [f for f in z.namelist() if "Applications" in f]

        if not sub_file or not app_file:
            print("Warning: expected files not found in FDA zip:", z.namelist())
            return []

        # Load applications for sponsor name
        sponsors = {}
        with z.open(app_file[0]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                app_no = row.get("ApplNo", "").strip()
                sponsor = row.get("SponsorName", "").strip()
                drug = row.get("DrugName", "").strip()
                if app_no:
                    sponsors[app_no] = {"sponsor": sponsor, "drug": drug}

        # Load submissions and filter under review / recent approvals
        with z.open(sub_file[0]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                sub_type = row.get("SubmissionType", "").strip()
                sub_status = row.get("SubmissionStatus", "").strip()
                app_no = row.get("ApplNo", "").strip()
                sub_no = row.get("SubmissionNo", "").strip()
                action_date = row.get("SubmissionStatusDate", "").strip()

                # Only NDA and BLA
                appl_type = row.get("ApplType", "").strip()
                if appl_type not in ("NDA", "BLA"):
                    continue

                # Under review or recently approved
                if sub_status == "Filed":
                    signal_type = "fda_under_review"
                elif sub_status == "AP":  # Approved
                    signal_type = "fda_approval"
                else:
                    continue

                # Only original applications (not supplements)
                if sub_type not in ("ORIG", "SUPPL"):
                    continue

                sponsor_info = sponsors.get(app_no, {})
                sponsor_name = sponsor_info.get("sponsor", "")
                drug_name = sponsor_info.get("drug", "")

                event_id = _hash_id("fda", app_no, sub_no, sub_status)

                events.append({
                    "event_id": event_id,
                    "date_detected": now.isoformat(),
                    "source": "fda",
                    "signal_type": signal_type,
                    "asset_name": drug_name,
                    "company": sponsor_name,
                    "indication_raw": "",
                    "phase": "",
                    "trial_id": f"{appl_type}{app_no}",
                    "start_date": "",
                    "last_update": action_date,
                    "geography": "US",
                    "source_url": f"https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo={app_no}",
                    "title": drug_name,
                    "summary": f"{appl_type}{app_no}/{sub_no}; Status: {sub_status}; Date: {action_date}",
                })

    print(f"FDA fetched: {len(events)} (under review + recent approvals)")
    return events
