import os
from sources.ctgov import fetch_phase3_recent
from sinks.sheets import upsert_events

def main():
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    worksheet = os.environ.get("WORKSHEET_NAME", "events")
    days_back = int(os.environ.get("DAYS_BACK", "7"))

    print("Running tracker...")
    print("Days back:", days_back)

    events = fetch_phase3_recent(days_back=days_back)

    print("Fetched events:", len(events))

    inserted = upsert_events(spreadsheet_id, worksheet, events)

    print("Inserted rows:", inserted)

if __name__ == "__main__":
    main()
