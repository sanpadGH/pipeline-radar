import os
from sources.ctgov import fetch_phase3_recent
from sources.ema_chmp_under_eval import fetch_ema_under_review_chmp
from sources.ctis import fetch_ctis_phase3, enrich_ctis_trials
from sources.fda import fetch_fda_under_review
from sources.ema_approvals import fetch_ema_approvals
from sinks.sheets import upsert_events, load_ctis_cache, save_ctis_cache

def main():
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    worksheet = os.environ.get("WORKSHEET_NAME", "events")
    days_back = int(os.environ.get("DAYS_BACK", "90"))

    print("Running tracker...")
    print("Days back (CTGOV):", days_back)

    ctgov_events = fetch_phase3_recent(days_back=days_back)

    ema_events = fetch_ema_under_review_chmp()
    print("EMA CHMP under evaluation fetched:", len(ema_events))

    ctis_events = fetch_ctis_phase3()

    print("Loading CTIS cache...")
    ctis_cache = load_ctis_cache(spreadsheet_id)
    print(f"CTIS cache loaded: {len(ctis_cache)} entries")

    ctis_events, new_cache = enrich_ctis_trials(ctis_events, ctis_cache)
    save_ctis_cache(spreadsheet_id, new_cache)

    fda_events = fetch_fda_under_review()

    ema_approval_events = fetch_ema_approvals()

    all_events = ema_events + ema_approval_events + fda_events + ctis_events + ctgov_events

    inserted = upsert_events(spreadsheet_id, worksheet, all_events)
    print("Inserted rows:", inserted)

if __name__ == "__main__":
    main()