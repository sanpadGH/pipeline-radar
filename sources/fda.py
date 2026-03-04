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
                    brand_name = ""
                    generic_name = ""
                    if products:
                        brand_name = products[0].get("brand_name", "").strip()
                        generic_name = products[0].get("generic_name", "").strip()

                    # Keep only most recent ORIG submission
                    best_sub = None
                    for sub in record.get("submissions", []) or []:
                        sub_status = sub.get("submission_status", "").strip()
                        sub_type = sub.get("submission_type", "").strip()
                        action_date = sub.get("submission_status_date", "").strip()

                        if sub_status != "AP":
                            continue
                        if sub_type != "ORIG":
                            continue
                        if action_date and action_date < cutoff:
                            continue

                        if best_sub is None or action_date > best_sub.get("submission_status_date", ""):
                            best_sub = sub

                    if best_sub is None:
                        continue

                    sub_no = best_sub.get("submission_number", "").strip()
                    action_date = best_sub.get("submission_status_date", "").strip()

                    if len(action_date) == 8:
                        action_date = f"{action_date[:4]}-{action_date[4:6]}-{action_date[6:]}"

                    event_id = _hash_id("fda", app_no, sub_no, "AP")

                    events.append({
                        "event_id": event_id,
                        "date_detected": now.isoformat(),
                        "source": "fda",
                        "signal_type": "fda_approval",
                        "asset_name": generic_name or brand_name,
                        "company": sponsor,
                        "indication_raw": "",
                        "phase": "",
                        "trial_id": app_no,
                        "start_date": "",
                        "last_update": action_date,
                        "geography": "US",
                        "source_url": f"https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo={app_no.replace('NDA','').replace('BLA','')}",
                        "title": brand_name or generic_name,
                        "summary": f"{app_no}/{sub_no}; Brand: {brand_name}; INN: {generic_name}; Status: AP; Date: {action_date}",
                    })

    print(f"FDA approvals fetched: {len(events)}")
    return events


def fetch_fda_adcom():
    now = datetime.now(timezone.utc)
    events = []

    try:
        url = (
            "https://www.federalregister.gov/api/v1/documents.json"
            "?conditions[agencies][]=food-and-drug-administration"
            "&conditions[term]=advisory+committee"
            "&conditions[type][]=Notice"
            "&per_page=40"
            "&order=newest"
        )
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()

        for doc in data.get("results", []):
            title = doc.get("title", "").strip()
            pub_date = doc.get("publication_date", "").strip()
            doc_url = doc.get("html_url", "").strip()
            abstract = doc.get("abstract", "").strip()

            if not title:
                continue

            event_id = _hash_id("fda_adcom", doc.get("document_number", title[:40]))

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
                "start_date": pub_date,
                "last_update": pub_date,
                "geography": "US",
                "source_url": doc_url,
                "title": title,
                "summary": abstract[:300] if abstract else "",
            })

    except Exception as ex:
        print(f"Warning: FDA AdCom Federal Register fetch failed: {ex}")

    print(f"FDA AdCom fetched: {len(events)}")
    return events


def fetch_fda_under_review():
    approvals = fetch_fda_approvals()
    adcom = fetch_fda_adcom()
    return approvals + adcom
