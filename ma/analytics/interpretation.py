"""
Analytical Interpretation Helpers, rule-based analyst-style text for common
valuation and market patterns. NOT AI-generated, deterministic analyst logic
grounded in M&A and corporate finance fundamentals.

These functions produce the kind of concise, investment-committee-ready
commentary that a senior analyst would attach to a chart: "why does this sector
trade at a premium?", "what does this sponsor's multiple profile imply?",
"what does this regime transition mean for new entries?"

Financial language used: control premium expectations, leverage capacity,
earnings visibility, multiple compression risk, dry powder deployment,
synergy-driven pricing, recurring revenue, switching costs, cyclicality.
"""


# Sector characteristics used to ground valuation premium/discount interpretations.
# Source: GICS sector fundamentals and historical M&A patterns.
_SECTOR_PROFILES = {
    "Technology": {
        "premium_drivers": "recurring revenue, high switching costs, scalable business models, and software margin expansion optionality",
        "discount_drivers": "cyclical demand exposure, hardware commoditization, or regulatory risk in platform assets",
        "leverage_capacity": "moderate, asset-light but high capex in hardware/infrastructure segments",
    },
    "Healthcare": {
        "premium_drivers": "inelastic demand, regulatory moats, patent protection, and demographic tailwinds",
        "discount_drivers": "reimbursement rate risk, clinical trial uncertainty, or binary regulatory outcomes",
        "leverage_capacity": "moderate-to-high in services and distribution; lower in early-stage biotech",
    },
    "Industrials": {
        "premium_drivers": "government contract backlogs, defense spending visibility, or aftermarket service streams",
        "discount_drivers": "input cost exposure, cyclical end-market demand, and capital intensity",
        "leverage_capacity": "moderate, asset-heavy but stable cash flows in aerospace/defense",
    },
    "Consumer Discretionary": {
        "premium_drivers": "brand equity, omnichannel scale advantages, or luxury pricing power",
        "discount_drivers": "discretionary demand sensitivity to economic cycles and consumer sentiment shifts",
        "leverage_capacity": "moderate, depends on recession resilience of the specific sub-segment",
    },
    "Consumer Staples": {
        "premium_drivers": "defensive demand, strong distribution networks, and pricing power over inputs",
        "discount_drivers": "private label competition, private equity recapitalization risk, or commodity exposure",
        "leverage_capacity": "high, predictable cash flows support debt service",
    },
    "Financials": {
        "premium_drivers": "network effects in payments, cross-sell density in banking, or fee-based revenue in asset management",
        "discount_drivers": "credit cycle exposure, regulatory capital requirements, and interest rate sensitivity",
        "leverage_capacity": "structurally limited, regulated balance sheets constrain LBO structures",
    },
    "Energy": {
        "premium_drivers": "commodity price upside optionality, infrastructure scarcity, or transition-linked assets",
        "discount_drivers": "commodity price cyclicality, ESG-driven capital avoidance, and long reinvestment cycles",
        "leverage_capacity": "variable, midstream (high, stable cash flows) vs E&P (low, volatile).",
    },
    "Materials": {
        "premium_drivers": "critical mineral scarcity, specialty chemical pricing power, or consolidation-driven market structure",
        "discount_drivers": "commodity input cost volatility, capex intensity, and global demand cyclicality",
        "leverage_capacity": "low-to-moderate, commodity price volatility constrains debt capacity",
    },
    "Real Estate": {
        "premium_drivers": "long-term contractual cash flows, inflation linkage in rents, and development optionality",
        "discount_drivers": "cap rate compression sensitivity to rates and overleverage in development assets",
        "leverage_capacity": "high, stable NOI supports debt; LTVs typically 50-65% in core RE",
    },
    "Communication Services": {
        "premium_drivers": "recurring subscription revenue, audience scale, and digital advertising network effects",
        "discount_drivers": "content cost inflation, cord-cutting disruption in legacy media, and spectrum scarcity costs",
        "leverage_capacity": "moderate, telecom infrastructure is high; media is lower due to content cyclicality",
    },
    "Utilities": {
        "premium_drivers": "regulated return visibility, inflation pass-through mechanisms, and grid investment tailwinds",
        "discount_drivers": "rate case risk, rising capex for grid modernization, and interest rate sensitivity",
        "leverage_capacity": "high, regulated revenue streams provide strong debt coverage",
    },
}

_DEFAULT_SECTOR_PROFILE = {
    "premium_drivers": "sector-specific structural advantages",
    "discount_drivers": "cyclicality, capital intensity, or earnings volatility",
    "leverage_capacity": "sector-specific",
}


def interpret_valuation_premium(sector: str, premium: float, percentile: float = None) -> str:
    """
    Returns interpretation of why a sector trades at a premium or discount.

    premium: sector median minus market median (positive = premium)
    percentile: historical percentile (0-100), optional

    E.g., 'Technology's +2.3x premium to market reflects recurring revenue,
    high switching costs, and scalable business models. At the 78th percentile
    of its historical range, multiples are elevated but not extreme, investors
    should monitor for earnings visibility risk if growth decelerates.'
    """
    profile = _SECTOR_PROFILES.get(sector, _DEFAULT_SECTOR_PROFILE)
    pct_str = ""
    if percentile is not None and not (percentile != percentile):  # nan check
        pct_str = f" At the **{percentile:.0f}th percentile** of its historical range"
        if percentile >= 80:
            pct_str += ", multiples are elevated, multiple compression risk should be factored into return projections."
        elif percentile >= 60:
            pct_str += ", multiples are above average but within historical norms."
        elif percentile >= 40:
            pct_str += ", multiples are near long-run average, limited directional valuation signal."
        else:
            pct_str += ", multiples are below average, potential mean-reversion tailwind for entry."

    if premium > 0:
        return (
            f"**{sector}**'s +{premium:.1f}x premium to market reflects {profile['premium_drivers']}. "
            f"Leverage capacity is {profile['leverage_capacity']}."
            + (f" {pct_str}" if pct_str else "")
        )
    elif premium < 0:
        return (
            f"**{sector}**'s {premium:.1f}x discount to market reflects exposure to "
            f"{profile['discount_drivers']}. "
            f"Leverage capacity is {profile['leverage_capacity']}."
            + (f" {pct_str}" if pct_str else "")
        )
    else:
        return (
            f"**{sector}** trades in line with the broader market, balancing "
            f"{profile['premium_drivers']} against {profile['discount_drivers']}."
            + (f" {pct_str}" if pct_str else "")
        )


def interpret_sponsor_vs_strategic_spread(sector: str, spread: float) -> str:
    """
    Returns interpretation of why sponsors pay more or less than strategics
    in a given sector.

    spread: sponsor median EV/EBITDA minus strategic median (positive = sponsors pay more)

    Financial context:
    - Sponsors pay more when leverage amplifies returns and cash flow visibility
      is high enough to support debt service.
    - Strategics pay more when synergies (revenue, cost, or platform) justify
      entry prices that sponsors cannot match on a standalone basis.
    """
    profile = _SECTOR_PROFILES.get(sector, _DEFAULT_SECTOR_PROFILE)

    if spread > 1.5:
        return (
            f"In **{sector}**, sponsors pay **{spread:.1f}x above** strategic acquirers. "
            f"This premium reflects the leverage capacity of the sector ({profile['leverage_capacity']}) "
            f"and competitive PE auction dynamics. Sponsors are effectively paying for the value "
            f"they believe operational improvement and financial engineering can unlock."
        )
    elif spread < -1.5:
        return (
            f"In **{sector}**, strategics outbid sponsors by **{abs(spread):.1f}x**. "
            f"Synergy-driven pricing, where integration benefits justify higher entry, "
            f"makes strategic deals structurally more competitive than LBO structures. "
            f"PE sponsors face difficulty clearing their return hurdles at these levels."
        )
    else:
        return (
            f"In **{sector}**, sponsor and strategic pricing are broadly aligned "
            f"({spread:+.1f}x spread), suggesting competitive dynamics are balanced between "
            f"financial and industrial buyers."
        )


def interpret_regime(regime_label: str, prior_regime: str = None) -> str:
    """
    Returns interpretation of current market regime and transition dynamics.

    regime_label: one of Peak/Late-Cycle, Recovery/Opportunity,
                  Selective/Cautious, Trough/Distressed
    prior_regime: optional prior regime for transition context
    """
    _regime_text = {
        "Peak / Late-Cycle": (
            "High deal activity combined with elevated multiples is characteristic of **late-cycle** conditions. "
            "Historically, sponsor returns compress when market-wide entry multiples are elevated, "
            "exit multiple contraction and higher debt costs can erode return projections significantly. "
            "New entrants should stress-test returns under multiple compression scenarios."
        ),
        "Recovery / Opportunity": (
            "Rising deal activity with below-average valuations signals a **recovery entry window**. "
            "Credit availability is improving but competitive pressure has not yet driven up multiples. "
            "Historically, vintages deployed in recovery regimes generate the strongest returns, "
            "the spread between entry and potential exit multiples is widest here."
        ),
        "Selective / Cautious": (
            "Depressed deal volumes despite elevated multiples indicate a **selective market**. "
            "Only assets with exceptional earnings visibility or strategic scarcity justify current pricing. "
            "Multiple compression risk is elevated, buyers are effectively relying on earnings growth "
            "to validate entry prices rather than exit multiple expansion."
        ),
        "Trough / Distressed": (
            "Both deal activity and valuations are below historical norms, **trough conditions**. "
            "Dry powder deployment is constrained by credit availability or macro uncertainty. "
            "Patient capital with flexible mandates has historically generated outsized returns "
            "by deploying when transaction volumes are low and pricing reflects distress rather than fundamentals."
        ),
        "Indeterminate": (
            "The current regime cannot be classified with confidence based on available data. "
            "Consider expanding the date range or broadening sector filters for a more complete picture."
        ),
    }

    text = _regime_text.get(regime_label, f"Regime: {regime_label}.")

    if prior_regime and prior_regime != regime_label:
        transition_context = {
            ("Peak / Late-Cycle", "Selective / Cautious"): (
                " This transition from Peak to Selective typically follows a credit tightening event, "
                "deal volumes fall first, before multiples fully reprice."
            ),
            ("Peak / Late-Cycle", "Trough / Distressed"): (
                " A direct transition from Peak to Trough is unusual and typically signals a macro shock "
                "or systemic credit disruption, deal activity and valuations repriced simultaneously."
            ),
            ("Trough / Distressed", "Recovery / Opportunity"): (
                " This transition from Trough to Recovery is the historically optimal entry signal, "
                "activity rebounds before multiples follow."
            ),
            ("Recovery / Opportunity", "Peak / Late-Cycle"): (
                " The transition from Recovery to Peak marks the compression of the opportunity window, "
                "the spread between entry and potential exit is narrowing as competition intensifies."
            ),
        }.get((prior_regime, regime_label), "")
        text += transition_context

    return text


def interpret_imbalance(signal: str, sector: str) -> str:
    """
    Returns actionable interpretation of a sector's imbalance signal.

    signal: one of Overheating, Healthy Growth, Narrowing, Cooling
    sector: sector name for contextual grounding
    """
    profile = _SECTOR_PROFILES.get(sector, _DEFAULT_SECTOR_PROFILE)

    _signal_text = {
        "Overheating": (
            f"**{sector}** is showing **overheating** characteristics, rising deal activity alongside "
            f"multiple expansion. Competitive auction processes are intensifying, and dry powder "
            f"concentration in this sector suggests control premium expectations are elevated. "
            f"New entrants should consider whether leverage capacity ({profile['leverage_capacity']}) "
            f"can support current pricing before committing."
        ),
        "Healthy Growth": (
            f"**{sector}** shows **healthy growth** dynamics, increasing transaction volume with "
            f"stable or compressing multiples. This is the preferred entry profile: demand is growing "
            f"without pricing distortion. Leverage capacity ({profile['leverage_capacity']}) can be "
            f"deployed efficiently at these multiples."
        ),
        "Narrowing": (
            f"**{sector}** is in a **narrowing** phase, deal volume is declining while multiples "
            f"remain elevated. Only assets with premium characteristics ({profile['premium_drivers']}) "
            f"are clearing at current prices. Selection quality is paramount, "
            f"paying peak multiples in a contracting deal market leaves limited exit optionality."
        ),
        "Cooling": (
            f"**{sector}** is **cooling**, both activity and multiples are contracting. "
            f"Capital is rotating elsewhere, creating potential relative value for contrarian buyers. "
            f"Watch for secondary process activity and motivated sellers as leverage ratios reset. "
            f"Key risk: ensuring valuation contraction reflects cyclicality rather than structural impairment."
        ),
        "Insufficient Data": (
            f"Insufficient transaction history to determine imbalance signal for {sector}."
        ),
    }

    return _signal_text.get(signal, f"{sector}: {signal} market conditions.")
