import hashlib
import requests
import zipfile
import io
import json
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

FDA_DOWNLOAD_URL = "https://api.fda.gov/download.json"
FDA_ADCOM_URL = "https://www.fda.gov/advisory-committees/advisory-committee-calendar"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def fetch_fda_approvals():
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=365)).strftime("%Y%m%d")

    r = requests.get(FDA_DOWNLOAD_URL, timeout=30)
    r.raise_for_status()
    manifest = r.json()

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
                    app_no = record.get("application_number", "").strip()
                    appl_type = app_no[:3]
                    if appl_type not in ("NDA", "BLA"):
                        continue

                    sponsor = record.get("sponsor_name", "").strip()
                    products = record.get("products", []) or []
                    drug_name = ""
                    if products:
                        drug_name = products[0].get("brand_name", "") or products[0].get("generic_name", "")

                    # Keep only most recent relevant submission
                    best_sub = None
                    for sub in record.get("submissions", []) or []:
                        sub_status = sub.get("submission_status", "").strip()
                        sub_type = sub.get("submission_type", "").strip()
                        action_date = sub.get("submission_status_date", "").strip()

                        if sub_status != "AP":
                            continue
                        if sub_type not in ("ORIG", "SUPPL"):
                            continue
                        if action_date and action_date < cutoff:
                            continue

                        if best_sub is None or action_date > best_sub.get("submission_status_date", ""):
                            best_sub = sub

                    if best_sub is None:
                        continue

                    sub_no = best_sub.get("submission_number", "").strip()
                    action_date = best_sub.get("submission_status_date", "").strip()

                    # Format date YYYYMMDD -> YYYY-MM-DD
                    if len(action_date) == 8:
                        action_date = f"{action_date[:4]}-{action_date[4:6]}-{action_date[6:]}"

                    event_id = _hash_id("fda", app_no, sub_no, "AP")

                    events.append({
                        "event_id": event_id,
                        "date_detected": now.isoformat(),
                        "source": "fda",
                        "signal_type": "fda_approval",
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
                        "summary": f"{app_no}/{sub_no}; Status: AP; Date: {action_date}",
                    })

    print(f"FDA approvals fetched: {len(events)}")
    return events


def fetch_fda_adcom():
    now = datetime.now(timezone.utc)
    events = []

    try:
        r = requests.get(FDA_ADCOM_URL, timeout=60)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # AdCom calendar is in a table
        for row in soup.select("table tbody tr"):
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) < 3:
                continue

            date_str = cols[0]
            committee = cols[1] if len(cols) > 1 else ""
            topic = cols[2] if len(cols) > 2 else ""
            link_tag = row.find("a", href=True)
            url = link_tag["href"] if link_tag else FDA_ADCOM_URL
            if url.startswith("/"):
                url = "https://www.fda.gov" + url

            if not date_str or not topic:
                continue

            event_id = _hash_id("fda_adcom", date_str, topic[:50])

            events.append({
                "event_id": event_id,
                "date_detected": now.isoformat(),
                "source": "fda",
                "signal_type": "fda_adcom",
                "asset_name": "",
                "company": "",
                "indication_raw": "",
                "phase": "",
                "trial_id": "",
                "start_date": date_str,
                "last_update": "",
                "geography": "US",
                "source_url": url,
                "title": topic,
                "summary": f"Committee: {committee}; Date: {date_str}",
            })

    except Exception as ex:
        print(f"Warning: FDA AdCom scraping failed: {ex}")

    print(f"FDA AdCom fetched: {len(events)}")
    return events


def fetch_fda_under_review():
    approvals = fetch_fda_approvals()
    adcom = fetch_fda_adcom()
    return approvals + adcom
