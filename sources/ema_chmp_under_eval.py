import hashlib
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

EMA_UNDER_EVAL_PAGE = "https://www.ema.europa.eu/en/medicines/medicines-human-use-under-evaluation"

def _hash_id(*parts: str) -> str:
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def _latest_under_eval_xlsx_url() -> str:
    r = requests.get(EMA_UNDER_EVAL_PAGE, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "applications-new-human-medicines-under-evaluation" in href and href.endswith("_en.xlsx"):
            return "https://www.ema.europa.eu" + href if href.startswith("/") else href

    raise RuntimeError("Could not find the under-evaluation XLSX link on EMA page")

def fetch_ema_under_review_chmp():
    now = datetime.now(timezone.utc)
    xlsx_url = _latest_under_eval_xlsx_url()

    # En estos ficheros, normalmente la tabla empieza en la fila 15 (header=14)
    df = pd.read_excel(xlsx_url, header=14)

    events = []
    for _, row in df.iterrows():
        inn = str(row.get("International non-proprietary name (INN) / Common Name", "")).strip()
        indication = str(row.get("Indication - Summary", "")).strip()
        ema_no = str(row.get("EMA Prod. Number", "")).strip()
        start_eval = row.get("Start of evaluation", "")

        if not inn or inn.lower() == "nan":
            continue

        event_id = _hash_id("ema_chmp_under_eval", ema_no or inn)

        events.append({
            "event_id": event_id,
            "date_detected": now.isoformat(),
            "source": "ema",
            "signal_type": "ema_under_review_chmp",
            "asset_name": inn,
            "company": "",                 # ese Excel no suele traer applicant; lo podemos enriquecer luego
            "indication_raw": indication,
            "phase": "",
            "trial_id": ema_no,            # lo usamos como identificador regulatorio
            "start_date": str(start_eval) if start_eval is not None else "",
            "last_update": "",
            "geography": "EU",
            "source_url": xlsx_url,
            "title": inn,
            "summary": f"CHMP under evaluation; EMA Prod: {ema_no}; Start eval: {start_eval}",
        })

    return events
