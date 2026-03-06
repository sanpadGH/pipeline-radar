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
    "indication_raw","id","start_date","last_update",
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

    ws.update("A1", [COLUMNS])
    print("Header written:", COLUMNS)

    existing = ws.get_all_records(expected_headers=COLUMNS)
    print("Existing rows:", len(existing))

    existing_ids = {r["event_id"] for r in existing if r.get("event_id")}
    existing_trial_ids = {r["id"] for r in existing if r.get("id")}

    new_rows = []
    for e in events:
        if e.get("id") in existing_trial_ids:
            continue
        if e["event_id"] in existing_ids:
            continue
        new_rows.append([e.get(col, "") for col in COLUMNS])

    print("New rows to insert:", len(new_rows))

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")

    return len(new_rows)