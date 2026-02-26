import json
import os
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

COLUMNS = [
    "event_id","date_detected","source","signal_type","asset_name","company",
    "indication_raw","phase","trial_id","start_date","last_update",
    "geography","source_url","title","summary"
]

def _client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=SCOPES
    )
    return gspread.authorize(creds)

def upsert_events(spreadsheet_id, worksheet_name, events):

    gc = _client()
    ws = gc.open_by_key(spreadsheet_id).worksheet(worksheet_name)

    existing = ws.get_all_records()
    existing_ids = {r["event_id"] for r in existing if r.get("event_id")}

    new_rows = []
    for e in events:
        if e["event_id"] not in existing_ids:
            new_rows.append([e.get(col, "") for col in COLUMNS])

    if new_rows:
        ws.append_rows(new_rows)

    return len(new_rows)
