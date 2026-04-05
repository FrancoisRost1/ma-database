"""
Real deal seeder — loads curated real_deals.csv into the database.
Each row fans out into: parties, sectors, deals, valuation_metrics, deal_metadata.
All records get data_origin='real'. Scoring runs after insertion.
"""
import uuid
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

from ma.db import queries
from ma.scoring.completeness import compute_completeness
from ma.scoring.confidence import compute_confidence
from ma.utils.formatting import normalize_deal_type, normalize_deal_status, parse_date


def seed_real_deals(config: dict) -> int:
    """
    Load real_deals.csv into the database.
    Returns the number of deals successfully inserted.
    Skips rows that fail validation silently (counts logged).
    """
    path = config["seed"]["real_deals_path"]
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = df.replace("", None)
    # Coerce blanks and "nan" strings to None
    df = df.where(df.notna(), other=None)

    # Build sector lookup: sector_name -> sector_id
    sector_lookup = _seed_sectors(config)
    # Build party lookup: party_name.lower() -> party_id
    party_lookup = _seed_parties(config)

    inserted = 0
    for _, row in df.iterrows():
        try:
            deal_id = str(uuid.uuid4())

            # Resolve sector
            sector_name = _str(row.get("sector_name"))
            sub_industry = _str(row.get("sub_industry"))
            sector_id = _resolve_sector(sector_name, sub_industry, sector_lookup)

            # Resolve acquirer party
            acquirer_name = _str(row.get("acquirer_name"))
            acquirer_type = _str(row.get("acquirer_type")) or "strategic"
            acquirer_party_id = None
            if acquirer_name:
                acquirer_party_id = _get_or_create_party(
                    acquirer_name, acquirer_type, party_lookup
                )

            # Build deal dict
            deal = {
                "deal_id": deal_id,
                "announcement_date": _date(row.get("announcement_date")),
                "closing_date": _date(row.get("closing_date")),
                "deal_type": normalize_deal_type(_str(row.get("deal_type")) or ""),
                "deal_status": normalize_deal_status(_str(row.get("deal_status")) or "closed"),
                "deal_value_usd": _float(row.get("deal_value_usd")),
                "enterprise_value": _float(row.get("enterprise_value")),
                "equity_value": _float(row.get("equity_value")),
                "currency": "USD",
                "acquirer_party_id": acquirer_party_id,
                "target_name": _str(row.get("target_name")) or "",
                "target_party_id": None,
                "target_status": _str(row.get("target_status")),
                "sector_id": sector_id,
                "geography": _str(row.get("geography")) or "US",
                "minority_or_control": _str(row.get("minority_or_control")) or "control",
                "hostile_or_friendly": _str(row.get("hostile_or_friendly")),
                "consortium_flag": _bool(row.get("consortium_flag")),
                "financing_structure_text": _str(row.get("financing_structure_text")),
                "notes": _str(row.get("notes")),
                "data_origin": "real",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }

            # Skip if missing core required field
            if not deal["target_name"] or not deal["announcement_date"]:
                continue

            queries.insert_deal(deal)

            # Valuation metrics
            vm = {
                "deal_id": deal_id,
                "ev_to_ebitda": _float(row.get("ev_to_ebitda")),
                "ev_to_revenue": _float(row.get("ev_to_revenue")),
                "premium_paid_pct": _float(row.get("premium_paid_pct")),
                "leverage_multiple": _float(row.get("leverage_multiple")),
                "target_revenue": _float(row.get("target_revenue")),
                "target_ebitda": _float(row.get("target_ebitda")),
                "target_ebitda_margin": _float(row.get("target_ebitda_margin")),
            }
            queries.insert_valuation_metrics(vm)

            # Build flat record for scoring
            flat = {**deal, **vm,
                    "acquirer_type": acquirer_type,
                    "sector_name": sector_name,
                    "sub_industry": sub_industry,
                    "source_url": _str(row.get("citation"))}

            completeness = compute_completeness(flat, config)
            confidence = compute_confidence(flat, completeness, config)

            dm = {
                "deal_id": deal_id,
                "data_source": _str(row.get("data_source")),
                "source_url": _str(row.get("citation")),
                "citation": _str(row.get("citation")),
                "completeness_score": completeness,
                "confidence_score": confidence,
                "last_reviewed": None,
                "reviewed_by": None,
            }
            queries.insert_deal_metadata(dm)
            inserted += 1

        except Exception as e:
            # Log and continue — real seeds should not crash the pipeline
            print(f"[seed_real] Skipped row (error: {e}): {row.get('target_name')}")

    return inserted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_sectors(config: dict) -> dict:
    """
    Ensure all sectors from config.yaml exist in the database.
    Returns a lookup: (sector_name, sub_industry) -> sector_id.
    Also creates a catch-all entry per sector_name with sub_industry=None.
    """
    lookup = {}
    for entry in config.get("sectors", []):
        sname = entry["sector_name"]
        # Seed the parent sector row (sub_industry=None)
        found, sid = queries.sector_exists(sname, None)
        if not found:
            sid = str(uuid.uuid4())
            queries.insert_sector({"sector_id": sid, "sector_name": sname, "sub_industry": None})
        lookup[(sname, None)] = sid

        for sub in entry.get("sub_industries", []):
            found_sub, sub_id = queries.sector_exists(sname, sub)
            if not found_sub:
                sub_id = str(uuid.uuid4())
                queries.insert_sector({"sector_id": sub_id, "sector_name": sname, "sub_industry": sub})
            lookup[(sname, sub)] = sub_id

    return lookup


def _seed_parties(config: dict) -> dict:
    """
    Ensure all known sponsors and consortiums from config.yaml exist in the database.
    Returns a lookup: lower(party_name) -> party_id.
    """
    lookup = {}
    for sponsor_name in config.get("sponsors", []):
        found, pid = queries.party_exists(sponsor_name)
        if not found:
            pid = str(uuid.uuid4())
            queries.insert_party({
                "party_id": pid,
                "party_name": sponsor_name,
                "party_type": "sponsor",
                "headquarters": "US",
                "description": None,
            })
        lookup[sponsor_name.lower()] = pid

    # Seed known consortium vehicles so consortium acquirer_type is functional
    for consortium_name in config.get("consortiums", []):
        found, pid = queries.party_exists(consortium_name)
        if not found:
            pid = str(uuid.uuid4())
            queries.insert_party({
                "party_id": pid,
                "party_name": consortium_name,
                "party_type": "consortium",
                "headquarters": "US",
                "description": None,
            })
        lookup[consortium_name.lower()] = pid

    return lookup


def _get_or_create_party(name: str, party_type: str, lookup: dict) -> str:
    """Get existing party_id or create a new party record. Updates lookup in-place."""
    key = name.lower()
    if key in lookup:
        return lookup[key]
    found, pid = queries.party_exists(name)
    if found:
        lookup[key] = pid
        return pid
    pid = str(uuid.uuid4())
    queries.insert_party({
        "party_id": pid,
        "party_name": name,
        "party_type": party_type,
        "headquarters": None,
        "description": None,
    })
    lookup[key] = pid
    return pid


def _resolve_sector(sector_name: str, sub_industry: str, lookup: dict) -> Optional[str]:
    """Return sector_id for the most specific match available."""
    if not sector_name:
        return None
    if (sector_name, sub_industry) in lookup:
        return lookup[(sector_name, sub_industry)]
    if (sector_name, None) in lookup:
        return lookup[(sector_name, None)]
    return None


def _str(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("nan", "none", "null", ""):
        return None
    return s


def _float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        v = float(str(val).strip())
        return None if np.isnan(v) else v
    except (ValueError, TypeError):
        return None


def _date(val):
    if val is None:
        return None
    d = parse_date(val)
    return d


def _bool(val) -> bool:
    if val is None:
        return False
    return str(val).strip().upper() in ("TRUE", "1", "YES")
