"""
Confidence scoring — rule-based trustworthiness rating per deal.
Confidence is conceptually distinct from completeness:
  completeness = how much data is filled
  confidence   = how much we trust that data

Phase 1: rule-based only (no ML, no cross-reference validation).
"""


def compute_confidence(deal_row: dict, completeness_score: float, config: dict) -> float:
    """
    Compute confidence score (0.0–1.0) for a single deal record.

    Rules (in priority order):
    1. If completeness < 30% → cap at 0.3 regardless of origin
    2. data_origin='real' AND source_url is not null → 1.0
    3. data_origin='real' AND source_url is null → 0.8
    4. data_origin='synthetic' → 0.5
    """
    conf_cfg = config["confidence"]
    cap_cfg = conf_cfg["low_completeness_cap"]
    rules = conf_cfg["rules"]

    # Low completeness cap — applies before all other rules
    if completeness_score < cap_cfg["threshold"]:
        return cap_cfg["max_confidence"]

    data_origin = (deal_row.get("data_origin") or "").strip().lower()
    source_url = deal_row.get("source_url")
    has_source = bool(source_url and str(source_url).strip())

    if data_origin == "real":
        return rules["real_with_source"] if has_source else rules["real_without_source"]

    if data_origin == "synthetic":
        return rules["synthetic"]

    # Fallback for unknown data_origin
    return 0.3


def confidence_label(score: float) -> str:
    """Human-readable label for a confidence score."""
    if score >= 0.9:
        return "High"
    elif score >= 0.7:
        return "Medium-High"
    elif score >= 0.5:
        return "Medium"
    elif score >= 0.3:
        return "Low"
    return "Very Low"
