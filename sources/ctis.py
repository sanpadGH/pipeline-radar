import hashlib
import json
import time
import requests
from datetime import datetime, timezone

OVERVIEW_URL = "https://euclinicaltrials.eu/ctis-public-api/search"
RETRIEVE_URL = "https://euclinicaltrials.eu/ctis-public-api/retrieve"

def _hash_id(*parts):
    return hashlib.sha256("||".join([p or "" for p in parts]).encode("utf-8")).hexdigest()[:20]

def _sponsor_words(sponsor: str) -> set:
    """Extract meaningful words from sponsor name for fuzzy matching."""
    stopwords = {"inc", "ltd", "llc", "gmbh", "bv", "sa", "ag", "co", "corp",
                 "the", "and", "of", "for", "global", "development", "pharma",
                 "pharmaceuticals", "biosciences", "therapeutics", "oncology"}
    words = set()
    for w in sponsor.lower().replace(".", " ").replace(",", " ").replace("-", " ").split():
        if w not in stopwords and len(w) > 2:
            words.add(w)
    return words

def _extract_active_substance(detail, sponsor: str = "") -> tuple[str, str]:
    """
    Returns (asset_name, aliases).

    Strategy:
    1. Filter products with part1MpRoleTypeCode == "1" (investigational)
    2. Among those, prefer the one whose nameOrg matches the sponsor
    3. Fallback: first product with part1MpRoleTypeCode == "1"
    4. aliases = synonyms + brand name (prodName) if available
    """
    try:
        products = (
            detail.get("authorizedApplication", {})
            .get("authorizedPartI", {})
            .get("products", [])
        )

        investigational = [
            p for p in products
            if str(p.get("part1MpRoleTypeCode", "")).strip() == "1"
        ]

        if not investigational:
            return "", ""

        # Try to match sponsor
        chosen = None
        if sponsor:
            sponsor_words = _sponsor_words(sponsor)
            for p in investigational:
                name_org = p.get("productDictionaryInfo", {}).get("nameOrg", "").lower()
                if any(w in name_org for w in sponsor_words):
                    chosen = p
                    break

        if not chosen:
            chosen = investigational[0]

        # asset_name
        asset_name = (
            chosen.get("productDictionaryInfo", {})
            .get("activeSubstanceName", "")
            .strip()
        )
        if not asset_name or asset_name.upper() == "N/A":
            return "", ""

        # aliases: synonyms + brand name
        aliases = []
        substances = chosen.get("productDictionaryInfo", {}).get("productSubstances", [])
        for s in substances:
            for syn in (s.get("synonyms") or []):
                syn = syn.strip()
                if syn and len(syn) <= 30 and syn not in aliases:
                    aliases.append(syn)

        # Add sponsor product code if present
        sponsor_code = (chosen.get("sponsorProductCodeEdit") or "").strip()
        if sponsor_code and sponsor_code not in aliases:
            aliases.append(sponsor_code)

        # Add brand name (prodName) — strip dosage info after first comma
        prod_name = chosen.get("productDictionaryInfo", {}).get("prodName", "").strip()
        if prod_name:
            brand = prod_name.split(" ")[0].strip()  # first word is usually the brand
            if brand and brand.upper() != asset_name.upper() and brand not in aliases:
                aliases.append(brand)

        return asset_name, "; ".join(aliases)

    except Exception:
        return "", ""

def _extract_start_date(detail):
    try:
        return (detail.get("decisionDate") or "")[:10]
    except Exception:
        return ""

def enrich_ctis_trials(trials, cache):
    new_cache = {}
    enriched = []

    for t in trials:
        ct_number = t["id"]

        if ct_number in cache:
            t["asset_name"] = cache[ct_number]["asset_name"]
            t["aliases"] = cache[ct_number].get("aliases", "")
            t["start_date"] = cache[ct_number]["start_date"]
            enriched.append(t)
            continue

        try:
            r = requests.get(f"{RETRIEVE_URL}/{ct_number}", timeout=30)
            r.raise_for_status()
            detail = r.json()
            asset, aliases = _extract_active_substance(detail, sponsor=t.get("company", ""))
            start = _extract_start_date(detail)
        except Exception as ex:
            print(f"Warning: could not retrieve CTIS {ct_number}: {ex}")
            asset = ""
            aliases = ""
            start = ""

        t["asset_name"] = asset
        t["aliases"] = aliases
        t["start_date"] = start
        new_cache[ct_number] = {"asset_name": asset, "aliases": aliases, "start_date": start}
        enriched.append(t)
        time.sleep(0.5)

    print(f"CTIS enriched: {len(enriched)} trials, {len(new_cache)} new cache entries")
    return enriched, new_cache

def fetch_ctis_phase3(page_size: int = 200, max_pages: int = 10):
    now = datetime.now(timezone.utc)

    events = []
    page = 1
    next_page = True

    while next_page and page <= max_pages:
        payload = {
            "pagination": {"page": page, "size": page_size},
            "sort": {"property": "decisionDate", "direction": "DESC"},
            "searchCriteria": {
                "containAll": None,
                "containAny": None,
                "containNot": None,
                "title": None,
                "number": None,
                "status": None,
                "medicalCondition": None,
                "sponsor": None,
                "endPoint": None,
                "productName": None,
                "trialPhaseCode": None,
                "eudraCtCode": None,
                "trialRegion": None,
            },
        }

        r = requests.post(
            OVERVIEW_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=60
        )
        r.raise_for_status()
        data = r.json()

        for t in data.get("data", []):
            trial_phase = (t.get("trialPhase") or "").lower()
            if "phase iii" not in trial_phase and "phase 3" not in trial_phase:
                continue

            ct_number = (t.get("ctNumber") or "").strip()
            title = (t.get("ctTitle") or "").strip()
            sponsor = (t.get("sponsor") or "").strip()
            condition = (t.get("conditions") or "").strip()
            last_updated = (t.get("lastUpdated") or "").strip()
            decision_date = (t.get("decisionDateOverall") or "").strip()

            if not ct_number:
                continue

            event_id = _hash_id("ctis", ct_number)

            events.append({
                "event_id": event_id,
                "date_detected": now.isoformat(),
                "source": "ctis",
                "signal_type": "phase3_trial",
                "asset_name": "",
                "aliases": "",
                "company": sponsor,
                "indication_raw": condition,
                "id": ct_number,
                "start_date": decision_date,
                "last_update": last_updated,
                "geography": "EU",
                "source_url": f"https://euclinicaltrials.eu/ctis-public/view/{ct_number}",
                "title": title,
                "summary": f"trialPhase={t.get('trialPhase','')}; decisionDate={decision_date}",
            })

        next_page = data.get("pagination", {}).get("nextPage", False)
        page += 1

    print(f"CTIS fetched: {len(events)}")
    return events
