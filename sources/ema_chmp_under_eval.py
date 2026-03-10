import hashlib
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

EMA_UNDER_EVAL_PAGE = "https://www.ema.europa.eu/en/medicines/medicines-human-use-under-evaluation"

def _hash_id(*parts: str) -> str:
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def _get_with_retry(url: str, retries: int = 3, backoff: float = 30.0, **kwargs) -> requests.Response:
    for attempt in range(retries):
        r = requests.get(url, **kwargs)
        if r.status_code == 429:
            wait = backoff * (attempt + 1)
            print(f"EMA rate limited (429), waiting {wait}s before retry {attempt + 1}/{retries}...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r

def _latest_under_eval_xlsx_url() -> str:
    r = _get_with_retry(EMA_UNDER_EVAL_PAGE, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "applications-new-human-medicines-under-evaluation" in href and href.endswith("_en.xlsx"):
            if href.startswith("/"):
                return "https://www.ema.europa.eu" + href
            return href
    raise RuntimeError("Could not find latest under-evaluation XLSX link on EMA page")

def fetch_ema_under_review_chmp():
    now = datetime.now(timezone.utc)
    xlsx_url = _latest_under_eval_xlsx_url()
    df = pd.read_excel(xlsx_url, header=14)
    events = []
    for _, row in df.iterrows():
        inn = str(row.get("International non-proprietary name (INN) / Common Name", "")).strip()
        indication = str(row.get("Indication - Summary", "")).strip()
        ema_no = str(row.get("EMA Prod. Number", "")).strip()
        start_eval = row.get("Start of evaluation", "")
        substance_type = str(row.get("Substance type (classification)", "")).strip()
        is_prime = str(row.get("Is PRIME", "")).strip()
        orphan = str(row.get("Orphan Product", "")).strip()
        if not inn or inn.lower() == "nan":
            continue
        event_id = _hash_id("ema_chmp_under_eval", ema_no or inn)
        events.append({
            "event_id": event_id,
            "date_detected": now.isoformat(),
            "source": "ema",
            "signal_type": "ema_under_review_chmp",
            "asset_name": inn,
            "aliases": "",
            "company": "",
            "indication_raw": indication,
            "id": ema_no,
            "start_date": str(start_eval) if start_eval is not None else "",
            "last_update": "",
            "geography": "EU",
            "source_url": xlsx_url,
            "title": inn,
            "summary": f"EMA Prod: {ema_no}; Start eval: {start_eval}; Type: {substance_type}; PRIME: {is_prime}; Orphan: {orphan}",
        })
    return events
