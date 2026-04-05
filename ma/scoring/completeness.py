"""
Completeness scoring — weighted percentage of key fields populated per deal.
Based on the three-tier weight system defined in config.yaml.
Completeness = data coverage, separate from data trustworthiness (confidence.py).
"""
import numpy as np
from typing import Optional


def compute_completeness(deal_row: dict, config: dict) -> float:
    """
    Compute weighted completeness score (0–100) for a single deal record.

    deal_row must be a flat dict containing all deal + valuation + party fields
    as returned by v_deals_flat or equivalent.

    Assumption: tier-1 fields all carry weight 3.0, tier-2 weight 2.0, tier-3 weight 1.0.
    """
    comp_cfg = config["completeness"]
    tier1 = comp_cfg["tier_1"]
    tier2 = comp_cfg["tier_2"]
    tier3 = comp_cfg["tier_3"]

    total_weight = 0.0
    filled_weight = 0.0

    for tier, weight in [(tier1, tier1["weight"]), (tier2, tier2["weight"]), (tier3, tier3["weight"])]:
        for field in tier["fields"]:
            total_weight += weight
            val = deal_row.get(field)
            if _is_filled(val):
                filled_weight += weight

    if total_weight == 0:
        return 0.0

    return round((filled_weight / total_weight) * 100, 2)


def quality_tier(score: float, config: dict) -> str:
    """
    Return the quality tier label for a given completeness score.
    Thresholds defined in config.yaml completeness.thresholds.
    """
    thresholds = config["completeness"]["thresholds"]
    if score >= thresholds["high"]:
        return "High"
    elif score >= thresholds["medium"]:
        return "Medium"
    return "Low"


def compute_batch(deals: "list[dict]", config: dict) -> "list[float]":
    """Compute completeness for a list of deal dicts. Returns scores in the same order."""
    return [compute_completeness(d, config) for d in deals]


def _is_filled(value) -> bool:
    """Return True if a field value is considered populated (not null/NaN/empty)."""
    if value is None:
        return False
    if isinstance(value, float) and np.isnan(value):
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True
