import os
from sources.ctgov import fetch_phase3_recent
from sources.ema_chmp_under_eval import fetch_ema_under_review_chmp
from sources.ctis import fetch_ctis_phase3
from sinks.sheets import upsert_events

def main():
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    worksheet = os.environ.get("WORKSHEET_NAME", "events")
    days_back = int(os.environ.get("DAYS_BACK", "7"))

    print("Running tracker...")
    print("Days back (CTGOV):", days_back)

    ctgov_events = fetch_phase3_recent(days_back=days_back)
    print("CTGOV fetched:", len(ctgov_events))

    ema_events = fetch_ema_under_review_chmp()
    print("EMA CHMP under evaluation fetched:", len(ema_events))

    ctis_events = fetch_ctis_phase3()
    print("CTIS fetched:", len(ctis_events))

    all_events = ctgov_events + ema_events + ctis_events
    inserted = upsert_events(spreadsheet_id, worksheet, all_events)
    print("Inserted rows:", inserted)

if __name__ == "__main__":
    main()
