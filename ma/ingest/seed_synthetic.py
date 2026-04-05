"""
Synthetic deal generator — extends the real dataset to ~300-500 total deals.
All synthetic records carry data_origin='synthetic' — NEVER mixed invisibly.
Valuation parameters are calibrated by sector per config.yaml.
Assumption: distributions are realistic but not based on any specific real transaction.
"""
import uuid
import numpy as np
import pandas as pd
from datetime import date, timedelta
from typing import Optional

from ma.db import queries
from ma.scoring.completeness import compute_completeness
from ma.scoring.confidence import compute_confidence

# Fixed RNG seed for reproducibility
_RNG = np.random.default_rng(42)

# Target name fragments for synthetic deal generation
_SYNTHETIC_TARGETS = [
    "Apex Software", "BlueStar Analytics", "Crestline Health", "DeltaPath Corp",
    "Edgewater Technologies", "Frontier Industrial", "GlobalSync Payments", "Harbor Life Sciences",
    "Inova Systems", "Jetstream Energy", "Keystone Materials", "Lighthouse Consumer",
    "Meridian Healthcare IT", "Nexus Industrial Services", "Omega Retail Holdings",
    "Pinnacle Financial Solutions", "Quantum Biotech", "Redwood Real Estate Services",
    "Summit Aerospace", "Titan Chemicals", "Unified Communications Corp", "Vector Logistics",
    "Westport Energy Partners", "Xenith Software", "Yellowstone Utilities", "Zephyr Media",
    "Cascade Manufacturing", "Dune Capital Advisors", "Eclipse Semiconductor", "Falcon Defense",
    "Granite Construction Services", "Highland Healthcare Solutions", "Iron Peak Materials",
    "Jasper Consumer Brands", "Kodiak Energy Solutions", "Lumen Data Services",
    "Marble Financial Technology", "Nordic Industrials Group", "Offshore Energy Holdings",
    "Pacific Logistics Partners", "Quartz Application Technologies", "Ridge Capital Markets",
    "Silver Springs Healthcare", "Tundra Software", "Upper Bay Specialty Chemicals",
    "Valley Medical Devices", "Windward Asset Management", "XR Technologies",
    "Yellowtail Energy", "Zenith Aerospace Components", "Atlas Biomedical",
    "Beacon Industrial Distribution", "Crown Consumer Goods", "Diamond Analytics Platform",
    "Echo Healthcare Services", "Forge Industrial Tech", "Genesis Biotechnology",
    "Helix Life Sciences", "Iris Software Group", "Jade Real Estate Trust",
    "Knox Specialty Finance", "Legacy Media Holdings", "Matrix Industrial Solutions",
    "Nova Cybersecurity", "Orbit Fintech", "Prism Insurance Services",
    "Quantum Energy Systems", "Radius Consumer Brands", "Spire Healthcare IT",
    "Terra Materials Group", "Union Retail Holdings", "Vega Communications",
    "Waterfall Asset Management", "Xcel Aerospace Defense", "York Utilities Group",
    "Zara Industrial", "Anchor Software Solutions", "Bristol Pharma Holdings",
    "Citadel Financial Services", "Delphi Technology Group", "Emerald Healthcare Analytics",
    "Frontier Communications Tech", "Glacier Energy Partners", "Horizon Industrial Services",
    "Indigo Consumer Retail", "Jupiter Specialty Chemicals", "Knightsbridge Capital",
    "Lakeview Medical Devices", "Monument Software Platform", "Northern Energy Holdings",
    "Optic Media Group", "Paramount Real Estate Group", "Quintus Biomedical",
    "Raven Defense Systems", "Sterling Financial Analytics", "Topaz Semiconductor",
    "Ultima Healthcare", "Vertex Industrial Components", "Wavelength Communications",
    "Xray Software", "Yield Analytics Corp", "Zone Industrial Automation",
]


def seed_synthetic_deals(config: dict, existing_count: int) -> int:
    """
    Generate synthetic deals up to the target count.
    Returns the number of deals inserted.
    existing_count: how many deals already exist (real + prior synthetic).
    """
    target = config["seed"]["target_synthetic_count"]
    n_to_generate = max(0, target - max(0, existing_count - config["seed"]["target_real_count"]))
    n_to_generate = min(n_to_generate, config["seed"]["synthetic_max"])

    if n_to_generate <= 0:
        return 0

    # Build sector and party lookups (already seeded by real seeder)
    sector_lookup = _build_sector_lookup()
    party_lookup = _build_party_lookup(config)

    syn_cfg = config["synthetic"]
    sectors = [s["sector_name"] for s in config["sectors"]]
    sector_weights = _compute_sector_weights(sectors)

    year_start = syn_cfg["year_range"]["start"]
    year_end = syn_cfg["year_range"]["end"]
    years = list(range(year_start, year_end + 1))
    year_weights = _recency_weights(years, syn_cfg["year_recency_bias"])

    deal_type_choices = list(syn_cfg["deal_type_weights"].keys())
    deal_type_weights = list(syn_cfg["deal_type_weights"].values())
    acquirer_type_choices = list(syn_cfg["acquirer_type_weights"].keys())
    acquirer_type_weights = list(syn_cfg["acquirer_type_weights"].values())
    status_choices = list(syn_cfg["deal_status_weights"].keys())
    status_weights = list(syn_cfg["deal_status_weights"].values())

    target_names = list(_SYNTHETIC_TARGETS)
    if len(target_names) < n_to_generate:
        target_names = [f"{t} {i}" for i, t in enumerate(target_names * (n_to_generate // len(target_names) + 2))]

    inserted = 0
    used_target_names = set()

    for i in range(n_to_generate):
        try:
            deal_id = str(uuid.uuid4())

            # Deal type and acquirer type
            deal_type = _weighted_choice(deal_type_choices, deal_type_weights)
            acquirer_type = _weighted_choice(acquirer_type_choices, acquirer_type_weights)
            # LBOs are always sponsor-led by definition (leveraged = financial buyer).
            # Take-privates can be strategic (e.g. Elon Musk/Twitter) — no override.
            # No extra RNG call — direct assignment from already-drawn type.
            if deal_type == "lbo":
                acquirer_type = "sponsor"

            # Sector
            sector_name = _weighted_choice(sectors, sector_weights)
            sector_id, sub_industry = _pick_sector(sector_name, sector_lookup, config)

            # Acquirer
            acquirer_party_id, acquirer_name = _pick_acquirer(acquirer_type, party_lookup, config)

            # Year and dates
            year = _weighted_choice(years, year_weights)
            ann_date = _random_date_in_year(year)
            deal_status = _weighted_choice(status_choices, status_weights)
            closing_date = _closing_date(ann_date, deal_status)

            # Deal value (log-normal)
            dv_cfg = syn_cfg["deal_value"]
            deal_value = _lognormal_clipped(
                dv_cfg["median"], dv_cfg["log_std"],
                dv_cfg["min"], dv_cfg["max"]
            )
            fill = syn_cfg["field_fill_probability"]

            enterprise_value = deal_value * _RNG.uniform(0.95, 1.10) if _fill(fill["enterprise_value"]) else None
            equity_value = enterprise_value * _RNG.uniform(0.70, 0.90) if enterprise_value and _fill(fill["equity_value"]) else None

            # Valuation metrics
            ev_ebitda_cfg = syn_cfg["ev_to_ebitda_by_sector"].get(sector_name, {"mean": 11.0, "std": 2.5})
            ev_to_ebitda = _normal_clipped(ev_ebitda_cfg["mean"], ev_ebitda_cfg["std"], 4.0, 40.0) if _fill(fill["ev_to_ebitda"]) else None

            ev_rev_cfg = syn_cfg["ev_to_revenue_by_sector"].get(sector_name, {"mean": 2.5, "std": 1.0})
            ev_to_revenue = _normal_clipped(ev_rev_cfg["mean"], ev_rev_cfg["std"], 0.2, 20.0) if _fill(fill["ev_to_revenue"]) else None

            # Target financials
            target_ebitda = None
            target_revenue = None
            target_ebitda_margin = None
            if ev_to_ebitda and enterprise_value and _fill(fill["target_ebitda"]):
                target_ebitda = enterprise_value / ev_to_ebitda
            if ev_to_revenue and enterprise_value and _fill(fill["target_revenue"]):
                target_revenue = enterprise_value / ev_to_revenue
            if target_ebitda and target_revenue and target_revenue > 0:
                target_ebitda_margin = (target_ebitda / target_revenue) * 100

            # Premium (public targets only)
            target_status = _random_target_status(deal_type)
            prem_cfg = syn_cfg["premium"]
            premium_paid_pct = None
            if target_status == "public" and _fill(fill["premium_paid_pct"]):
                premium_paid_pct = _normal_clipped(prem_cfg["mean"], prem_cfg["std"], prem_cfg["min"], prem_cfg["max"])

            # Leverage (LBO only)
            lev_cfg = syn_cfg["leverage"]
            leverage_multiple = None
            if deal_type in ("lbo", "take_private") and _fill(fill["leverage_multiple"]):
                leverage_multiple = _normal_clipped(lev_cfg["mean"], lev_cfg["std"], lev_cfg["min"], lev_cfg["max"])

            hostile_or_friendly = None
            if _fill(fill["hostile_or_friendly"]):
                hostile_or_friendly = _RNG.choice(["friendly", "friendly", "friendly", "hostile"], p=[0.85, 0.05, 0.05, 0.05])
                hostile_or_friendly = "friendly" if _RNG.random() > 0.05 else "hostile"

            financing_text = None
            if _fill(fill["financing_structure_text"]):
                financing_text = _financing_text(deal_type)

            # Unique target name
            target_name = _unique_name(target_names, used_target_names, i)
            used_target_names.add(target_name)

            deal = {
                "deal_id": deal_id,
                "announcement_date": ann_date,
                "closing_date": closing_date,
                "deal_type": deal_type,
                "deal_status": deal_status,
                "deal_value_usd": round(deal_value, 1),
                "deal_value_local": None,
                "currency": "USD",
                "enterprise_value": round(enterprise_value, 1) if enterprise_value else None,
                "equity_value": round(equity_value, 1) if equity_value else None,
                "acquirer_party_id": acquirer_party_id,
                "target_name": target_name,
                "target_party_id": None,
                "target_status": target_status,
                "sector_id": sector_id,
                "geography": "US",
                "minority_or_control": "control",
                "hostile_or_friendly": hostile_or_friendly,
                "consortium_flag": bool(_RNG.random() < 0.08),
                "financing_structure_text": financing_text,
                "notes": None,
                "data_origin": "synthetic",
                "created_at": None,
                "updated_at": None,
            }

            queries.insert_deal(deal)

            vm = {
                "deal_id": deal_id,
                "ev_to_ebitda": round(ev_to_ebitda, 2) if ev_to_ebitda else None,
                "ev_to_revenue": round(ev_to_revenue, 2) if ev_to_revenue else None,
                "premium_paid_pct": round(premium_paid_pct, 1) if premium_paid_pct else None,
                "leverage_multiple": round(leverage_multiple, 2) if leverage_multiple else None,
                "target_revenue": round(target_revenue, 1) if target_revenue else None,
                "target_ebitda": round(target_ebitda, 1) if target_ebitda else None,
                "target_ebitda_margin": round(target_ebitda_margin, 1) if target_ebitda_margin else None,
            }
            queries.insert_valuation_metrics(vm)

            flat = {**deal, **vm,
                    "acquirer_type": acquirer_type,
                    "sector_name": sector_name,
                    "sub_industry": sub_industry,
                    "source_url": None}

            completeness = compute_completeness(flat, config)
            confidence = compute_confidence(flat, completeness, config)

            dm = {
                "deal_id": deal_id,
                "data_source": "synthetic",
                "source_url": None,
                "citation": None,
                "completeness_score": completeness,
                "confidence_score": confidence,
                "last_reviewed": None,
                "reviewed_by": None,
            }
            queries.insert_deal_metadata(dm)
            inserted += 1

        except Exception as e:
            print(f"[seed_synthetic] Error on record {i}: {e}")

    return inserted


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_sector_lookup() -> dict:
    """Fetch all sectors from the database into a lookup dict."""
    from ma.db.engine import get_connection
    conn = get_connection()
    df = conn.execute("SELECT sector_id, sector_name, sub_industry FROM sectors").df()
    lookup = {}
    for _, row in df.iterrows():
        lookup[(row["sector_name"], row["sub_industry"])] = row["sector_id"]
    return lookup


def _build_party_lookup(config: dict) -> dict:
    """Fetch all parties from the database into a lookup dict."""
    from ma.db.engine import get_connection
    conn = get_connection()
    df = conn.execute("SELECT party_id, party_name, party_type FROM parties").df()
    lookup: dict[str, list] = {}
    for _, row in df.iterrows():
        t = row["party_type"]
        if t not in lookup:
            lookup[t] = []
        lookup[t].append((row["party_id"], row["party_name"]))
    return lookup


def _compute_sector_weights(sectors: list) -> list:
    """Uniform weights for sector selection (can be extended later)."""
    n = len(sectors)
    return [1.0 / n] * n


def _recency_weights(years: list, bias: float) -> list:
    """Compute year weights with recency bias — more weight on recent years."""
    min_year = min(years)
    raw = [bias ** (y - min_year) for y in years]
    total = sum(raw)
    return [w / total for w in raw]


def _weighted_choice(choices: list, weights: list):
    """Select one item from choices using given weights."""
    idx = _RNG.choice(len(choices), p=np.array(weights) / sum(weights))
    return choices[idx]


def _lognormal_clipped(median: float, log_std: float, lo: float, hi: float) -> float:
    """Sample from log-normal, clipped to [lo, hi]."""
    mu = np.log(median)
    v = _RNG.lognormal(mu, log_std)
    return float(np.clip(v, lo, hi))


def _normal_clipped(mean: float, std: float, lo: float, hi: float) -> float:
    """Sample from normal, clipped to [lo, hi]."""
    v = _RNG.normal(mean, std)
    return float(np.clip(v, lo, hi))


def _fill(prob: float) -> bool:
    return _RNG.random() < prob


def _random_date_in_year(year: int) -> date:
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    days = (end - start).days
    return start + timedelta(days=int(_RNG.integers(0, days)))


def _closing_date(ann_date: date, deal_status: str):
    if deal_status not in ("closed",):
        return None
    months = int(_RNG.integers(3, 18))
    return ann_date + timedelta(days=months * 30)


def _random_target_status(deal_type: str) -> str:
    if deal_type in ("lbo", "take_private"):
        return _RNG.choice(["public", "private"], p=[0.55, 0.45])
    return _RNG.choice(["public", "private", "subsidiary"], p=[0.40, 0.40, 0.20])


def _pick_sector(sector_name: str, lookup: dict, config: dict):
    """Return (sector_id, sub_industry) for a given sector_name."""
    subs = []
    for entry in config.get("sectors", []):
        if entry["sector_name"] == sector_name:
            subs = entry.get("sub_industries", [])
            break
    sub_industry = None
    if subs and _RNG.random() > 0.2:
        sub_industry = str(_RNG.choice(subs))
    sid = lookup.get((sector_name, sub_industry)) or lookup.get((sector_name, None))
    return sid, sub_industry


def _pick_acquirer(acquirer_type: str, party_lookup: dict, config: dict):
    """Return (party_id, party_name) for a given acquirer type."""
    options = party_lookup.get(acquirer_type, [])
    if not options:
        # Fallback: use any available party
        for t in ("strategic", "sponsor", "other"):
            if party_lookup.get(t):
                options = party_lookup[t]
                break
    if not options:
        return None, None
    idx = int(_RNG.integers(0, len(options)))
    pid, pname = options[idx]
    return pid, pname


def _financing_text(deal_type: str) -> str:
    if deal_type in ("lbo", "take_private"):
        return _RNG.choice([
            "Senior secured term loan + revolving credit facility",
            "First lien TLB + second lien notes + revolver",
            "Senior notes + mezzanine debt + equity",
            "TLB + high yield bonds + equity co-invest",
        ])
    return _RNG.choice([
        "All cash",
        "Cash and stock",
        "All stock",
        "Cash, stock and assumption of debt",
    ])


def _unique_name(names: list, used: set, idx: int) -> str:
    """Return a synthetic target name not already used."""
    for n in names[idx:] + names[:idx]:
        if n not in used:
            return n
    return f"Synthetic Target {idx}"
