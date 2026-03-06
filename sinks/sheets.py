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

CACHE_COLUMNS = ["ct_number", "asset_name", "start_date"]
COMPANY_MAP_COLUMNS = ["inn", "ema_no", "company", "source", "nct_id"]

def _client():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=SCOPES
    )
    return gspread.authorize(creds)

def get_or_create_worksheet(spreadsheet, name, headers):
    try:
        ws = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=2000, cols=len(headers))
        ws.update("A1", [headers])
    return ws

def load_ctis_cache(spreadsheet_id):
    gc = _client()
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_or_create_worksheet(ss, "ctis_cache", CACHE_COLUMNS)
    rows = ws.get_all_records(expected_headers=CACHE_COLUMNS)
    return {r["ct_number"]: {"asset_name": r["asset_name"], "start_date": r["start_date"]} for r in rows if r.get("ct_number")}

def save_ctis_cache(spreadsheet_id, cache_updates):
    if not cache_updates:
        return
    gc = _client()
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_or_create_worksheet(ss, "ctis_cache", CACHE_COLUMNS)
    rows = [[ct, v["asset_name"], v["start_date"]] for ct, v in cache_updates.items()]
    ws.append_rows(rows, value_input_option="RAW")
    print(f"CTIS cache updated: {len(rows)} new entries")

def load_ema_company_map(spreadsheet_id):
    gc = _client()
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_or_create_worksheet(ss, "ema_company_map", COMPANY_MAP_COLUMNS)
    rows = ws.get_all_records(expected_headers=COMPANY_MAP_COLUMNS)
    return {r["inn"].lower(): r for r in rows if r.get("inn")}

def save_ema_company_map(spreadsheet_id, new_entries):
    if not new_entries:
        return
    gc = _client()
    ss = gc.open_by_key(spreadsheet_id)
    ws = get_or_create_worksheet(ss, "ema_company_map", COMPANY_MAP_COLUMNS)
    rows = [[e["inn"], e["ema_no"], e["company"], e["source"], e["nct_id"]] for e in new_entries]
    ws.append_rows(rows, value_input_option="RAW")
    print(f"EMA company map updated: {len(rows)} new entries")

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