import time
import requests

CTGOV_API = "https://clinicaltrials.gov/api/v2/studies"

def _lookup_company_ctgov(inn: str) -> dict:
    try:
        params = {
            "query.term": f"AREA[InterventionName]{inn}",
            "filter.advanced": "AREA[LeadSponsorClass]INDUSTRY",
            "pageSize": 3,
        }
        r = requests.get(CTGOV_API, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        studies = data.get("studies", [])
        for s in studies:
            ps = s.get("protocolSection", {})
            sponsor = ps.get("sponsorCollaboratorsModule", {}).get("leadSponsor", {})
            company = sponsor.get("name", "").strip()
            sponsor_class = sponsor.get("class", "").strip()
            nct_id = ps.get("identificationModule", {}).get("nctId", "")
            if company and sponsor_class == "INDUSTRY":
                return {"company": company, "nct_id": nct_id}
    except Exception as ex:
        print(f"Warning: CT.gov lookup failed for {inn}: {ex}")
    return {"company": "", "nct_id": ""}

def enrich_ema_companies(events, company_map):
    new_entries = []

    for e in events:
        inn = (e.get("asset_name") or "").strip()
        ema_no = (e.get("id") or "").strip()

        if not inn:
            continue

        inn_key = inn.lower()

        if inn_key in company_map:
            e["company"] = company_map[inn_key]["company"]
            continue

        result = _lookup_company_ctgov(inn)
        company = result["company"]
        nct_id = result["nct_id"]

        e["company"] = company
        company_map[inn_key] = {"company": company, "nct_id": nct_id}

        new_entries.append({
            "inn": inn,
            "ema_no": ema_no,
            "company": company,
            "source": "ctgov",
            "nct_id": nct_id,
        })

        time.sleep(0.3)

    found = sum(1 for e in new_entries if e["company"])
    print(f"EMA company lookup: {len(new_entries)} queried, {found} found")
    return events, new_entries
