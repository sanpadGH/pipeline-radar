import os
from sources.ctgov import fetch_phase3_recent
from sources.ema import fetch_ema_under_review
from sinks.sheets import upsert_events

def main():
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    worksheet = os.environ.get("WORKSHEET_NAME", "events")
    days_back = int(os.environ.get("DAYS_BACK", "7"))

    print("Running tracker...")
    print("Days back (CTGOV):", days_back)

    # ---- CTGOV ----
    ctgov_events = fetch_phase3_recent(days_back=days_back)
    print("CTGOV fetched:", len(ctgov_events))

    # ---- EMA ----
    ema_events = fetch_ema_under_review()
    print("EMA fetched:", len(ema_events))

    all_events = ctgov_events + ema_events

    inserted = upsert_events(spreadsheet_id, worksheet, all_events)

    print("Inserted rows:", inserted)

if __name__ == "__main__":
    main()
