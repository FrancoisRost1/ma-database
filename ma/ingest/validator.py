"""
Field validation and duplicate detection for inbound deal records.
Used by both CSV import and the Streamlit data entry form.
All validation rules are driven by config.yaml validation section.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
from ma.db.engine import get_connection


VALID_DEAL_TYPES = {"lbo", "strategic_acquisition", "merger", "take_private", "carve_out"}
VALID_DEAL_STATUSES = {"announced", "closed", "terminated", "pending"}
VALID_DATA_ORIGINS = {"real", "synthetic"}
VALID_TARGET_STATUSES = {"public", "private", "subsidiary", "carve_out"}
VALID_PARTY_TYPES = {"sponsor", "strategic", "consortium", "other"}


class ValidationError(Exception):
    """Raised when a deal record fails validation."""
    pass


def validate_deal(row: dict, config: dict) -> "list[str]":
    """
    Validate a single deal row dict.
    Returns a list of error messages (empty list = valid).
    Drives config.yaml validation rules.
    """
    errors = []
    vcfg = config["validation"]

    # Required fields
    for field in vcfg["required_fields"]:
        val = row.get(field)
        if val is None or (isinstance(val, float) and np.isnan(val)) or str(val).strip() == "":
            errors.append(f"Required field missing: '{field}'")

    # data_origin must be 'real' or 'synthetic'
    origin = str(row.get("data_origin", "")).strip().lower()
    if origin not in VALID_DATA_ORIGINS:
        errors.append(f"data_origin must be 'real' or 'synthetic', got: '{origin}'")

    # deal_type
    dt = str(row.get("deal_type", "")).strip().lower()
    if dt and dt not in VALID_DEAL_TYPES:
        errors.append(f"Unknown deal_type: '{dt}'. Valid: {VALID_DEAL_TYPES}")

    # deal_status
    ds = str(row.get("deal_status", "")).strip().lower()
    if ds and ds not in VALID_DEAL_STATUSES:
        errors.append(f"Unknown deal_status: '{ds}'. Valid: {VALID_DEAL_STATUSES}")

    # Numeric range checks
    def check_range(field, lo, hi):
        val = row.get(field)
        if val is not None and not (isinstance(val, float) and np.isnan(val)):
            try:
                v = float(val)
                if v < lo or v > hi:
                    errors.append(f"{field} out of range [{lo}, {hi}]: {v}")
            except (TypeError, ValueError):
                errors.append(f"{field} is not numeric: {val}")

    check_range("deal_value_usd", vcfg["deal_value_min"], vcfg["deal_value_max"])
    check_range("ev_to_ebitda", vcfg["ev_to_ebitda_min"], vcfg["ev_to_ebitda_max"])
    check_range("ev_to_revenue", vcfg["ev_to_revenue_min"], vcfg["ev_to_revenue_max"])
    check_range("premium_paid_pct", vcfg["premium_min"], vcfg["premium_max"])
    check_range("leverage_multiple", vcfg["leverage_min"], vcfg["leverage_max"])

    # announcement_date must not be more than 1 year in the future
    ann = row.get("announcement_date")
    if ann:
        try:
            ann_d = pd.to_datetime(ann).date()
            cutoff = datetime.today().date().replace(year=datetime.today().year + 1)
            if ann_d > cutoff:
                errors.append(
                    f"announcement_date ({ann_d}) is more than 1 year in the future"
                )
        except Exception:
            pass

    # Date logic: closing_date must be >= announcement_date
    cl = row.get("closing_date")
    if ann and cl:
        try:
            ann_d = pd.to_datetime(ann).date()
            cl_d = pd.to_datetime(cl).date()
            if cl_d < ann_d:
                errors.append(f"closing_date ({cl_d}) is before announcement_date ({ann_d})")
        except Exception:
            pass  # date parsing errors are caught elsewhere

    return errors


def detect_duplicates(row: dict, config: dict) -> "list[dict]":
    """
    Check whether a deal already exists in the database using the duplicate detection rules.
    Returns a list of matching existing deals (empty = no duplicate found).

    Rule: same acquirer_name + target_name within ±N days = likely duplicate.
    Assumption: consortium deals attributed to primary sponsor only.
    """
    vcfg = config["validation"]
    tolerance = vcfg.get("duplicate_date_tolerance_days", 30)

    ann = row.get("announcement_date")
    acquirer = row.get("acquirer_name", "")
    target = row.get("target_name", "")

    if not ann or not target:
        return []

    try:
        ann_d = pd.to_datetime(ann).date()
    except Exception:
        return []

    conn = get_connection()
    sql = """
        SELECT deal_id, target_name, acquirer_name, announcement_date
        FROM v_deals_flat
        WHERE LOWER(target_name) = LOWER(?)
          AND ABS(DATEDIFF('day', announcement_date, ?)) <= ?
    """
    results = conn.execute(sql, [target, str(ann_d), tolerance]).df()

    if acquirer and not results.empty:
        results = results[results["acquirer_name"].str.lower() == acquirer.strip().lower()]

    return results.to_dict("records")


def validate_batch(rows: "list[dict]", config: dict) -> pd.DataFrame:
    """
    Validate a batch of rows. Returns a DataFrame with columns:
    row_index | errors | is_valid
    """
    records = []
    for i, row in enumerate(rows):
        errs = validate_deal(row, config)
        records.append({"row_index": i, "errors": "; ".join(errs), "is_valid": len(errs) == 0})
    return pd.DataFrame(records)
