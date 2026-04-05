"""
Bulk CSV import — validates and loads user-provided deal CSVs into the database.
Supports preview-before-commit workflow for the Data Management tab.
Re-uses the same validation and seeding logic as the real/synthetic seeders.
"""
import uuid
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

from ma.db import queries
from ma.ingest.validator import validate_batch, detect_duplicates
from ma.ingest.seed_real import (
    _get_or_create_party, _resolve_sector, _seed_sectors, _seed_parties,
    _str, _float, _date, _bool,
)
from ma.scoring.completeness import compute_completeness
from ma.scoring.confidence import compute_confidence
from ma.utils.formatting import normalize_deal_type, normalize_deal_status


def preview_csv(filepath: str, config: dict) -> dict:
    """
    Parse and validate a CSV without committing to the database.
    Returns a dict with:
      - total_rows: int
      - valid_rows: int
      - invalid_rows: list of {row_index, errors}
      - duplicate_warnings: list of {row_index, matches}
      - preview_df: first 20 rows as DataFrame
    """
    df = _load_csv(filepath)
    rows = df.to_dict("records")

    validation_df = validate_batch(rows, config)
    invalid = validation_df[~validation_df["is_valid"]].to_dict("records")

    dup_warnings = []
    for i, row in enumerate(rows):
        dupes = detect_duplicates(row, config)
        if dupes:
            dup_warnings.append({"row_index": i, "matches": dupes})

    return {
        "total_rows": len(rows),
        "valid_rows": int(validation_df["is_valid"].sum()),
        "invalid_rows": invalid,
        "duplicate_warnings": dup_warnings,
        "preview_df": df.head(20),
    }


def import_csv(filepath: str, config: dict, skip_invalid: bool = True) -> dict:
    """
    Load a CSV into the database after validation.
    Returns a summary dict with inserted, skipped, and error counts.

    skip_invalid=True: skip rows that fail validation (log them).
    skip_invalid=False: abort entire import if any row is invalid.
    """
    df = _load_csv(filepath)
    rows = df.to_dict("records")

    validation_df = validate_batch(rows, config)
    if not skip_invalid and not validation_df["is_valid"].all():
        return {"inserted": 0, "skipped": 0, "errors": validation_df[~validation_df["is_valid"]].to_dict("records")}

    sector_lookup = _seed_sectors(config)
    party_lookup = _seed_parties(config)

    inserted = 0
    skipped = 0
    errors = []

    for i, row in enumerate(rows):
        if not validation_df.loc[i, "is_valid"] and skip_invalid:
            skipped += 1
            errors.append({"row_index": i, "error": validation_df.loc[i, "errors"]})
            continue

        try:
            deal_id = str(uuid.uuid4())

            sector_name = _str(row.get("sector_name"))
            sub_industry = _str(row.get("sub_industry"))
            sector_id = _resolve_sector(sector_name, sub_industry, sector_lookup)

            acquirer_name = _str(row.get("acquirer_name"))
            acquirer_type = _str(row.get("acquirer_type")) or "strategic"
            acquirer_party_id = None
            if acquirer_name:
                acquirer_party_id = _get_or_create_party(acquirer_name, acquirer_type, party_lookup)

            # data_origin must be explicit in the CSV
            data_origin = _str(row.get("data_origin")) or "real"

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
                "minority_or_control": _str(row.get("minority_or_control")),
                "hostile_or_friendly": _str(row.get("hostile_or_friendly")),
                "consortium_flag": _bool(row.get("consortium_flag")),
                "financing_structure_text": _str(row.get("financing_structure_text")),
                "notes": _str(row.get("notes")),
                "data_origin": data_origin,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }

            queries.insert_deal(deal)

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

            flat = {**deal, **vm,
                    "acquirer_type": acquirer_type,
                    "sector_name": sector_name,
                    "sub_industry": sub_industry,
                    "source_url": _str(row.get("source_url"))}

            completeness = compute_completeness(flat, config)
            confidence = compute_confidence(flat, completeness, config)

            dm = {
                "deal_id": deal_id,
                "data_source": _str(row.get("data_source")),
                "source_url": _str(row.get("source_url")),
                "citation": _str(row.get("citation")),
                "completeness_score": completeness,
                "confidence_score": confidence,
                "last_reviewed": None,
                "reviewed_by": None,
            }
            queries.insert_deal_metadata(dm)
            inserted += 1

        except Exception as e:
            skipped += 1
            errors.append({"row_index": i, "error": str(e)})

    return {"inserted": inserted, "skipped": skipped, "errors": errors}


def _load_csv(filepath: str) -> pd.DataFrame:
    """Load a deal CSV, normalizing column names."""
    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
    df = df.where(df.notna(), other=None)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df
