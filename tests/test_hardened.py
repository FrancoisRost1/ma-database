"""
Hardened test suite — independent stress tests for financial logic, data integrity,
edge cases, and analytical correctness not covered by the standard test suite.

DO NOT modify source modules. Only report failures and their root cause.

Uses the shared fixtures from conftest.py:
  - config   → loaded config.yaml
  - db        → fresh DuckDB with schema
  - seeded_db → db with real + synthetic seeds loaded
"""
import copy
import os
import pytest
import pandas as pd
import numpy as np

from ma.scoring.completeness import compute_completeness
from ma.scoring.confidence import compute_confidence
from ma.ingest.validator import validate_deal
from ma.db.engine import get_connection
from ma.db import queries
from ma.analytics import valuation, market_activity, sponsor_intel
from ma.analytics.snapshot import generate_snapshot
from ma.export.csv_export import export_deals_csv
from ma.export.excel_export import export_deals_excel


# ---------------------------------------------------------------------------
# Shared row-builder helpers — do NOT import or modify source code
# ---------------------------------------------------------------------------

def _tier1_fields() -> dict:
    # 7 fields × weight 3.0 = 21 (data_origin removed — always filled, no analytical signal)
    return {
        "announcement_date": "2022-01-15",
        "acquirer_party_id": "party-001",
        "target_name": "Test Corp",
        "deal_type": "lbo",
        "sector_id": "sector-001",
        "deal_value_usd": 1500.0,
        "deal_status": "closed",
    }


def _tier2_fields() -> dict:
    # 9 fields × weight 2.0 = 18 (enterprise_value, target_ebitda, target_revenue added)
    return {
        "ev_to_ebitda": 12.5,
        "ev_to_revenue": 3.0,
        "premium_paid_pct": 25.0,
        "acquirer_type": "sponsor",
        "closing_date": "2022-06-30",
        "geography": "US",
        "enterprise_value": 2500.0,
        "target_ebitda": 300.0,
        "target_revenue": 1200.0,
    }


def _tier3_fields() -> dict:
    # 6 fields × weight 1.0 = 6
    return {
        "financing_structure_text": "Senior secured + subordinated",
        "leverage_multiple": 5.5,
        "hostile_or_friendly": "friendly",
        "notes": "Flagship buyout",
        "source_url": "https://example.com/deal",
        "sub_industry": "Application Software",
    }


# ===========================================================================
# Category 1: Completeness scoring — math verification
# ===========================================================================

class TestCompletenessMath:
    """Verify the weighted completeness formula against known exact values.
    Total possible weight = (7×3.0) + (9×2.0) + (6×1.0) = 21 + 18 + 6 = 45."""

    def test_completeness_exact_math_full_row(self, config):
        """All 22 fields filled → weighted score must equal exactly 100.0 (45/45 × 100)."""
        row = {**_tier1_fields(), **_tier2_fields(), **_tier3_fields()}
        score = compute_completeness(row, config)
        assert score == pytest.approx(100.0, abs=0.1)

    def test_completeness_exact_math_tier1_only(self, config):
        """Only 7 tier-1 fields filled → score must equal (21/45) × 100 = 46.67."""
        row = _tier1_fields()
        score = compute_completeness(row, config)
        expected = (21 / 45) * 100  # 46.666...
        assert score == pytest.approx(expected, abs=0.1)

    def test_completeness_exact_math_tier1_plus_tier2(self, config):
        """16 fields (tier 1+2) filled → score must equal (39/45) × 100 = 86.67."""
        row = {**_tier1_fields(), **_tier2_fields()}
        score = compute_completeness(row, config)
        expected = (39 / 45) * 100  # 86.666...
        assert score == pytest.approx(expected, abs=0.1)

    def test_completeness_single_tier3_field(self, config):
        """Only 1 tier-3 field filled → score must equal (1/45) × 100 = 2.22."""
        row = {"notes": "Some analyst notes"}
        score = compute_completeness(row, config)
        expected = (1 / 45) * 100  # 2.222...
        assert score == pytest.approx(expected, abs=0.1)

    def test_completeness_empty_string_treated_as_missing(self, config):
        """Empty string fields must be treated identically to None (both = missing)."""
        row_with_none = {"target_name": None, "deal_type": "lbo", "data_origin": "real"}
        row_with_empty = {"target_name": "", "deal_type": "lbo", "data_origin": "real"}
        score_none = compute_completeness(row_with_none, config)
        score_empty = compute_completeness(row_with_empty, config)
        assert score_none == pytest.approx(score_empty, abs=0.01), (
            "Empty string not treated the same as None in completeness scoring"
        )


# ===========================================================================
# Category 2: Confidence scoring — edge cases
# ===========================================================================

class TestConfidenceEdgeCases:
    """Validate confidence score boundary conditions.
    Rules (priority order):
      1. completeness < 30% → cap at 0.3
      2. real + source_url → 1.0
      3. real + no source → 0.8
      4. synthetic → 0.5"""

    def test_confidence_synthetic_with_low_completeness(self, config):
        """Synthetic + completeness 20% → low-completeness cap applies → must equal 0.3."""
        row = {"data_origin": "synthetic", "source_url": None}
        score = compute_confidence(row, 20.0, config)
        assert score == pytest.approx(0.3, abs=0.001), (
            "Expected 0.3 (cap) for synthetic with 20% completeness"
        )

    def test_confidence_real_source_url_empty_string(self, config):
        """source_url='' must behave like None → returns 0.8 (not 1.0)."""
        row = {"data_origin": "real", "source_url": ""}
        score = compute_confidence(row, 80.0, config)
        assert score == pytest.approx(0.8, abs=0.001), (
            "Empty string source_url should not count as 'has source' → expected 0.8"
        )

    def test_confidence_real_with_source_at_boundary(self, config):
        """Completeness exactly 30.0 → cap threshold is strict <30, so 30.0 must NOT be capped."""
        row = {"data_origin": "real", "source_url": "https://example.com/deal"}
        score = compute_confidence(row, 30.0, config)
        assert score == pytest.approx(1.0, abs=0.001), (
            "Completeness=30.0 should NOT trigger the <30 cap; expected 1.0 for real+source"
        )

    def test_confidence_never_negative(self, config):
        """Confidence must be >= 0 for all valid and edge-case inputs."""
        cases = [
            ({"data_origin": "real", "source_url": None}, 0.0),
            ({"data_origin": "synthetic", "source_url": None}, 0.0),
            ({"data_origin": "unknown", "source_url": None}, 0.0),
            ({"data_origin": "real", "source_url": "http://x.com"}, 100.0),
            ({"data_origin": "synthetic"}, 29.9),
        ]
        for row, completeness in cases:
            score = compute_confidence(row, completeness, config)
            assert score >= 0, f"Negative confidence ({score}) for {row}, completeness={completeness}"

    def test_confidence_never_above_1(self, config):
        """Confidence must be <= 1.0 for all valid and edge-case inputs."""
        cases = [
            ({"data_origin": "real", "source_url": "http://x.com"}, 100.0),
            ({"data_origin": "real", "source_url": None}, 90.0),
            ({"data_origin": "synthetic", "source_url": "http://x.com"}, 80.0),
        ]
        for row, completeness in cases:
            score = compute_confidence(row, completeness, config)
            assert score <= 1.0, f"Confidence > 1.0 ({score}) for {row}"


# ===========================================================================
# Category 3: Synthetic data realism validation
# ===========================================================================

class TestSyntheticDataRealism:
    """After seeding, validate that synthetic data follows the config distributions.
    Uses seeded_db fixture — queries raw tables for direct inspection."""

    def test_synthetic_deal_value_range(self, seeded_db, config):
        """All synthetic deal_value_usd must be within config min (100) and max (50000)."""
        conn = get_connection()
        df = conn.execute("""
            SELECT deal_value_usd FROM deals
            WHERE data_origin = 'synthetic' AND deal_value_usd IS NOT NULL
        """).df()
        min_v = config["synthetic"]["deal_value"]["min"]
        max_v = config["synthetic"]["deal_value"]["max"]
        assert (df["deal_value_usd"] >= min_v).all(), "Synthetic deal value below config minimum"
        assert (df["deal_value_usd"] <= max_v).all(), "Synthetic deal value above config maximum"

    def test_synthetic_ev_ebitda_range(self, seeded_db, config):
        """All synthetic ev_to_ebitda must be within validation range [1.0, 100.0]."""
        conn = get_connection()
        df = conn.execute("""
            SELECT vm.ev_to_ebitda FROM valuation_metrics vm
            JOIN deals d ON d.deal_id = vm.deal_id
            WHERE d.data_origin = 'synthetic' AND vm.ev_to_ebitda IS NOT NULL
        """).df()
        lo = config["validation"]["ev_to_ebitda_min"]
        hi = config["validation"]["ev_to_ebitda_max"]
        assert (df["ev_to_ebitda"] >= lo).all(), f"Synthetic EV/EBITDA below {lo}x"
        assert (df["ev_to_ebitda"] <= hi).all(), f"Synthetic EV/EBITDA above {hi}x"

    def test_synthetic_acquirer_type_distribution(self, seeded_db, config):
        """Sponsor deals should be 30-60% of synthetic deals (config target: 40%)."""
        df = queries.get_all_deals({"data_origin": "synthetic"})
        assert not df.empty, "No synthetic deals found"
        total = len(df)
        sponsor_count = (df["acquirer_type"] == "sponsor").sum()
        sponsor_pct = sponsor_count / total * 100
        assert 30 <= sponsor_pct <= 60, (
            f"Sponsor pct {sponsor_pct:.1f}% outside expected 30-60% range"
        )

    def test_synthetic_sector_coverage(self, seeded_db, config):
        """Synthetic deals must cover at least 8 of the 11 defined sectors."""
        conn = get_connection()
        df = conn.execute("""
            SELECT DISTINCT s.sector_name FROM deals d
            JOIN sectors s ON d.sector_id = s.sector_id
            WHERE d.data_origin = 'synthetic'
        """).df()
        assert len(df) >= 8, f"Only {len(df)} sectors covered by synthetic data, expected ≥ 8"

    def test_synthetic_sponsor_coverage(self, seeded_db, config):
        """Synthetic sponsor deals must use at least 10 of the 20 defined sponsors."""
        conn = get_connection()
        df = conn.execute("""
            SELECT DISTINCT p.party_name FROM deals d
            JOIN parties p ON d.acquirer_party_id = p.party_id
            WHERE d.data_origin = 'synthetic' AND p.party_type = 'sponsor'
        """).df()
        assert len(df) >= 10, f"Only {len(df)} sponsors used, expected ≥ 10"

    def test_synthetic_year_coverage(self, seeded_db, config):
        """Synthetic deals must span at least 8 of the 11 years in the configured range."""
        conn = get_connection()
        df = conn.execute("""
            SELECT DISTINCT YEAR(announcement_date) AS yr FROM deals
            WHERE data_origin = 'synthetic'
        """).df()
        assert len(df) >= 8, f"Only {len(df)} years covered by synthetic data, expected ≥ 8"

    def test_synthetic_no_negative_deal_values(self, seeded_db, config):
        """No synthetic deal may have a negative deal_value_usd."""
        conn = get_connection()
        df = conn.execute("""
            SELECT deal_value_usd FROM deals
            WHERE data_origin = 'synthetic' AND deal_value_usd IS NOT NULL
        """).df()
        assert (df["deal_value_usd"] >= 0).all(), "Negative deal values found in synthetic data"

    def test_synthetic_premium_only_for_public_targets(self, seeded_db, config):
        """premium_paid_pct must only be set when target_status = 'public' in synthetic deals."""
        conn = get_connection()
        df = conn.execute("""
            SELECT d.target_status, vm.premium_paid_pct FROM deals d
            JOIN valuation_metrics vm ON d.deal_id = vm.deal_id
            WHERE d.data_origin = 'synthetic'
              AND vm.premium_paid_pct IS NOT NULL
        """).df()
        if df.empty:
            pytest.skip("No synthetic deals with premium_paid_pct set")
        # Target status must be 'public' (or null if not specified, which is also acceptable)
        non_public = df[df["target_status"].notna() & (df["target_status"] != "public")]
        assert len(non_public) == 0, (
            f"Found {len(non_public)} synthetic deals with premium_paid_pct but "
            f"target_status is not 'public': {non_public['target_status'].tolist()}"
        )


# ===========================================================================
# Category 4: Relational integrity
# ===========================================================================

class TestRelationalIntegrity:
    """Validate foreign key relationships and view correctness after full seed."""

    def test_no_orphaned_valuation_metrics(self, seeded_db):
        """Every deal_id in valuation_metrics must have a matching deal in deals."""
        conn = get_connection()
        orphans = conn.execute("""
            SELECT vm.deal_id FROM valuation_metrics vm
            LEFT JOIN deals d ON vm.deal_id = d.deal_id
            WHERE d.deal_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, f"Found {len(orphans)} orphaned valuation_metrics rows"

    def test_no_orphaned_deal_metadata(self, seeded_db):
        """Every deal_id in deal_metadata must have a matching deal in deals."""
        conn = get_connection()
        orphans = conn.execute("""
            SELECT dm.deal_id FROM deal_metadata dm
            LEFT JOIN deals d ON dm.deal_id = d.deal_id
            WHERE d.deal_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, f"Found {len(orphans)} orphaned deal_metadata rows"

    def test_every_deal_has_valid_sector(self, seeded_db):
        """Every non-null sector_id in deals must match a row in sectors."""
        conn = get_connection()
        orphans = conn.execute("""
            SELECT d.deal_id FROM deals d
            LEFT JOIN sectors s ON d.sector_id = s.sector_id
            WHERE d.sector_id IS NOT NULL AND s.sector_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, f"Found {len(orphans)} deals with invalid sector_id"

    def test_every_deal_has_valid_acquirer_party(self, seeded_db):
        """Every non-null acquirer_party_id in deals must match a row in parties."""
        conn = get_connection()
        orphans = conn.execute("""
            SELECT d.deal_id FROM deals d
            LEFT JOIN parties p ON d.acquirer_party_id = p.party_id
            WHERE d.acquirer_party_id IS NOT NULL AND p.party_id IS NULL
        """).fetchall()
        assert len(orphans) == 0, f"Found {len(orphans)} deals with invalid acquirer_party_id"

    def test_flat_view_no_row_duplication(self, seeded_db):
        """v_deals_flat row count must equal deals row count — joins must not fan out rows."""
        conn = get_connection()
        deals_count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        view_count = conn.execute("SELECT COUNT(*) FROM v_deals_flat").fetchone()[0]
        assert deals_count == view_count, (
            f"v_deals_flat has {view_count} rows but deals has {deals_count} — "
            "a LEFT JOIN is multiplying rows (likely a one-to-many relationship)"
        )

    def test_flat_view_has_all_key_columns(self, seeded_db):
        """v_deals_flat must expose all columns required by analytics and export layers."""
        required = [
            "deal_id", "announcement_date", "acquirer_name", "target_name",
            "sector_name", "deal_type", "deal_value_usd", "ev_to_ebitda",
            "data_origin", "completeness_score",
        ]
        conn = get_connection()
        df = conn.execute("SELECT * FROM v_deals_flat LIMIT 1").df()
        missing = [c for c in required if c not in df.columns]
        assert len(missing) == 0, f"v_deals_flat is missing required columns: {missing}"


# ===========================================================================
# Category 5: Data origin isolation
# ===========================================================================

class TestDataOriginIsolation:
    """Validate that data_origin filters are correctly enforced at every layer."""

    def test_filter_real_only_returns_no_synthetic(self, seeded_db):
        """get_all_deals with data_origin='real' must return zero synthetic rows."""
        df = queries.get_all_deals({"data_origin": "real"})
        assert not df.empty, "No real deals found"
        assert (df["data_origin"] == "real").all(), "Found synthetic rows in real-only result"

    def test_filter_synthetic_only_returns_no_real(self, seeded_db):
        """get_all_deals with data_origin='synthetic' must return zero real rows."""
        df = queries.get_all_deals({"data_origin": "synthetic"})
        assert not df.empty, "No synthetic deals found"
        assert (df["data_origin"] == "synthetic").all(), "Found real rows in synthetic-only result"

    def test_combined_equals_sum_of_parts(self, seeded_db):
        """Total deal count must equal real + synthetic counts (no hidden records)."""
        total = queries.get_deals_count()
        real_count = queries.get_deals_count({"data_origin": "real"})
        synth_count = queries.get_deals_count({"data_origin": "synthetic"})
        assert total == real_count + synth_count, (
            f"Total ({total}) ≠ real ({real_count}) + synthetic ({synth_count}) — "
            "there may be records with an unexpected data_origin value"
        )

    def test_data_origin_never_null_after_seed(self, seeded_db):
        """data_origin must never be NULL in the deals table after seeding."""
        conn = get_connection()
        null_count = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE data_origin IS NULL"
        ).fetchone()[0]
        assert null_count == 0, f"Found {null_count} deals with NULL data_origin"

    def test_data_origin_only_valid_values(self, seeded_db):
        """Only 'real' and 'synthetic' may appear as data_origin values."""
        conn = get_connection()
        df = conn.execute("SELECT DISTINCT data_origin FROM deals").df()
        actual = set(df["data_origin"].dropna().tolist())
        valid = {"real", "synthetic"}
        invalid = actual - valid
        assert len(invalid) == 0, f"Invalid data_origin values in database: {invalid}"


# ===========================================================================
# Category 6: Validator edge cases
# ===========================================================================

class TestValidatorEdgeCases:
    """Verify that validate_deal() correctly accepts/rejects boundary conditions."""

    def _base_deal(self) -> dict:
        """Minimal valid deal dict satisfying all required fields."""
        return {
            "announcement_date": "2022-01-15",
            "target_name": "Test Corp",
            "deal_type": "lbo",
            "deal_status": "closed",
            "data_origin": "real",
        }

    def test_validator_future_announcement_date(self, config):
        """announcement_date in 2030 should produce a validation warning or error.

        NOTE: If this test fails with 0 errors, it means the validator does not
        implement future-date checking — this is a source code gap, not a test bug.
        """
        row = self._base_deal()
        row["announcement_date"] = "2030-06-01"
        errors = validate_deal(row, config)
        assert len(errors) > 0, (
            "Expected validation error for announcement_date='2030-06-01', but none raised. "
            "Source code does not check for future dates."
        )

    def test_validator_closing_same_day_as_announcement(self, config):
        """closing_date = announcement_date must be valid (same-day close happens in reality)."""
        row = self._base_deal()
        row["closing_date"] = "2022-01-15"  # same as announcement_date
        errors = validate_deal(row, config)
        date_errors = [e for e in errors if "closing_date" in e.lower()]
        assert len(date_errors) == 0, (
            f"Same-day close incorrectly flagged as invalid: {date_errors}"
        )

    def test_validator_ev_ebitda_at_boundary(self, config):
        """ev_to_ebitda = 1.0 (min) must pass; 0.9 must fail."""
        row_pass = {**self._base_deal(), "ev_to_ebitda": 1.0}
        row_fail = {**self._base_deal(), "ev_to_ebitda": 0.9}
        pass_errors = [e for e in validate_deal(row_pass, config) if "ev_to_ebitda" in e]
        fail_errors = [e for e in validate_deal(row_fail, config) if "ev_to_ebitda" in e]
        assert len(pass_errors) == 0, f"ev_to_ebitda=1.0 (min) incorrectly flagged: {pass_errors}"
        assert len(fail_errors) > 0, "ev_to_ebitda=0.9 (below min) should fail but didn't"

    def test_validator_ev_ebitda_at_upper_boundary(self, config):
        """ev_to_ebitda = 100.0 (max) must pass; 100.1 must fail."""
        row_pass = {**self._base_deal(), "ev_to_ebitda": 100.0}
        row_fail = {**self._base_deal(), "ev_to_ebitda": 100.1}
        pass_errors = [e for e in validate_deal(row_pass, config) if "ev_to_ebitda" in e]
        fail_errors = [e for e in validate_deal(row_fail, config) if "ev_to_ebitda" in e]
        assert len(pass_errors) == 0, f"ev_to_ebitda=100.0 (max) incorrectly flagged: {pass_errors}"
        assert len(fail_errors) > 0, "ev_to_ebitda=100.1 (above max) should fail but didn't"

    def test_validator_negative_premium(self, config):
        """premium_paid_pct = -10.0 must pass (distressed deals; config min = -50)."""
        row = {**self._base_deal(), "premium_paid_pct": -10.0}
        errors = [e for e in validate_deal(row, config) if "premium" in e.lower()]
        assert len(errors) == 0, (
            f"premium_paid_pct=-10.0 incorrectly flagged as invalid: {errors}"
        )

    def test_validator_leverage_at_max(self, config):
        """leverage_multiple = 15.0 (max) must pass; 15.1 must fail."""
        row_pass = {**self._base_deal(), "leverage_multiple": 15.0}
        row_fail = {**self._base_deal(), "leverage_multiple": 15.1}
        pass_errors = [e for e in validate_deal(row_pass, config) if "leverage" in e.lower()]
        fail_errors = [e for e in validate_deal(row_fail, config) if "leverage" in e.lower()]
        assert len(pass_errors) == 0, f"leverage_multiple=15.0 incorrectly flagged: {pass_errors}"
        assert len(fail_errors) > 0, "leverage_multiple=15.1 should fail but didn't"

    def test_validator_empty_target_name(self, config):
        """target_name = '' must fail — it is a required field."""
        row = self._base_deal()
        row["target_name"] = ""
        errors = validate_deal(row, config)
        assert any("target_name" in e for e in errors), (
            f"Empty target_name not flagged as missing required field. errors={errors}"
        )

    def test_validator_whitespace_target_name(self, config):
        """target_name = '   ' must fail — whitespace-only is not a valid name."""
        row = self._base_deal()
        row["target_name"] = "   "
        errors = validate_deal(row, config)
        assert any("target_name" in e for e in errors), (
            f"Whitespace-only target_name not flagged as invalid. errors={errors}"
        )


# ===========================================================================
# Category 7: Valuation analytics correctness
# ===========================================================================

class TestValuationAnalytics:
    """Validate that valuation analytics functions return financially sensible output."""

    def test_ev_ebitda_by_sector_no_negative_values(self, seeded_db):
        """All ev_to_ebitda values returned by ev_ebitda_by_sector() must be > 0."""
        df = valuation.ev_ebitda_by_sector()
        assert not df.empty, "ev_ebitda_by_sector() returned an empty DataFrame"
        assert (df["ev_to_ebitda"] > 0).all(), (
            f"Found non-positive ev_to_ebitda values: {df[df['ev_to_ebitda'] <= 0]}"
        )

    def test_sector_valuation_stats_median_within_range(self, seeded_db):
        """Median EV/EBITDA per sector must be between 1x and 50x (financial sanity)."""
        df = valuation.sector_valuation_stats()
        assert not df.empty
        for _, row in df.iterrows():
            assert 1.0 <= row["median"] <= 50.0, (
                f"Sector '{row['sector_name']}' median EV/EBITDA {row['median']:.1f}x "
                "outside plausible range [1x, 50x]"
            )

    def test_sponsor_vs_strategic_both_present(self, seeded_db):
        """sponsor_vs_strategic_multiples() must return rows for both 'sponsor' and 'strategic'.

        Requires both types to have ev_to_ebitda data. If either is missing, the seeded
        dataset may not have enough coverage.
        """
        df = valuation.sponsor_vs_strategic_multiples()
        assert not df.empty, "sponsor_vs_strategic_multiples() returned empty"
        types = set(df["acquirer_type"].tolist())
        assert "sponsor" in types, "Missing 'sponsor' in sponsor_vs_strategic output"
        assert "strategic" in types, "Missing 'strategic' in sponsor_vs_strategic output"

    def test_valuation_regime_shift_has_multiple_years(self, seeded_db):
        """median_ev_ebitda_by_sector_year() must contain at least 3 distinct years."""
        df = valuation.median_ev_ebitda_by_sector_year()
        assert not df.empty, "median_ev_ebitda_by_sector_year() returned empty"
        n_years = df["year"].nunique()
        assert n_years >= 3, (
            f"Only {n_years} years in valuation regime data, expected ≥ 3"
        )

    def test_premium_analysis_only_valid_range(self, seeded_db, config):
        """All premium_paid_pct values must fall within the config bounds [-50, 200]."""
        df = valuation.premium_distribution()
        if df.empty:
            pytest.skip("No public target deals with premium data — skipping")
        lo = config["validation"]["premium_min"]
        hi = config["validation"]["premium_max"]
        assert (df["premium_paid_pct"] >= lo).all(), (
            f"Premium below config minimum ({lo}%)"
        )
        assert (df["premium_paid_pct"] <= hi).all(), (
            f"Premium above config maximum ({hi}%)"
        )


# ===========================================================================
# Category 8: Market activity analytics correctness
# ===========================================================================

class TestMarketActivityAnalytics:
    """Validate that market activity aggregations are internally consistent."""

    def test_deal_count_sums_to_total(self, seeded_db):
        """Sum of annual deal counts in deal_count_over_time() must equal total deals in DB."""
        df = market_activity.deal_count_over_time()
        total_from_ts = int(df["deal_count"].sum())
        total_from_db = queries.get_deals_count()
        assert total_from_ts == total_from_db, (
            f"Time series sum ({total_from_ts}) ≠ DB total ({total_from_db})"
        )

    def test_sector_heatmap_covers_all_active_sectors(self, seeded_db):
        """sector_activity_heatmap() must include every sector that has at least one deal."""
        conn = get_connection()
        active_sectors = set(
            conn.execute("""
                SELECT DISTINCT s.sector_name FROM deals d
                JOIN sectors s ON d.sector_id = s.sector_id
                WHERE s.sector_name IS NOT NULL
            """).df()["sector_name"].tolist()
        )
        heatmap = market_activity.sector_activity_heatmap()
        assert not heatmap.empty, "Sector heatmap returned empty"
        heatmap_sectors = set(heatmap["sector_name"].tolist())
        missing = active_sectors - heatmap_sectors
        assert len(missing) == 0, (
            f"Sectors with deals missing from heatmap: {missing}"
        )

    def test_deal_completion_rate_between_0_and_100(self, seeded_db):
        """Deal completion rate percentage must be in [0, 100]."""
        result = market_activity.deal_completion_rate()
        rate = result.get("completion_rate_pct")
        if rate is None:
            pytest.skip("Completion rate is None (no closed or terminated deals)")
        assert 0 <= rate <= 100, f"Completion rate {rate} outside [0, 100]"

    def test_deal_status_breakdown_sums_to_total(self, seeded_db):
        """Sum of all status deal counts must equal total deal count in DB."""
        df = market_activity.deal_status_breakdown()
        total_from_breakdown = int(df["deal_count"].sum())
        total_from_db = queries.get_deals_count()
        assert total_from_breakdown == total_from_db, (
            f"Status breakdown total ({total_from_breakdown}) ≠ DB total ({total_from_db})"
        )

    def test_sponsor_vs_strategic_sums_correctly(self, seeded_db):
        """Per-year sponsor+strategic+other sum from trend must equal deals with acquirer per year."""
        trend_df = market_activity.sponsor_vs_strategic_trend()
        if trend_df.empty:
            pytest.skip("No sponsor/strategic trend data")
        trend_by_year = trend_df.groupby("year")["deal_count"].sum()

        # Reference: deals that have a non-null acquirer_type (from the party join)
        conn = get_connection()
        ref_df = conn.execute("""
            SELECT YEAR(announcement_date) AS year, COUNT(*) AS deal_count
            FROM v_deals_flat
            WHERE acquirer_type IS NOT NULL
            GROUP BY year
        """).df()
        ref_by_year = ref_df.set_index("year")["deal_count"]

        for year, trend_count in trend_by_year.items():
            if year in ref_by_year.index:
                assert trend_count == ref_by_year[year], (
                    f"Year {year}: trend sum ({trend_count}) ≠ reference ({ref_by_year[year]})"
                )


# ===========================================================================
# Category 9: Sponsor intelligence correctness
# ===========================================================================

class TestSponsorIntelligence:
    """Validate sponsor ranking, heatmap, and entry multiple computations."""

    def test_sponsor_rankings_no_strategic_acquirers(self, seeded_db):
        """sponsor_rankings() must only contain party_type='sponsor' entities."""
        df = sponsor_intel.sponsor_rankings()
        if df.empty:
            pytest.skip("No sponsor deals found")
        conn = get_connection()
        sponsor_names = df["sponsor_name"].tolist()
        placeholders = ", ".join(["?" for _ in sponsor_names])
        result = conn.execute(
            f"SELECT party_name, party_type FROM parties WHERE party_name IN ({placeholders})",
            sponsor_names,
        ).df()
        non_sponsors = result[result["party_type"] != "sponsor"]
        assert len(non_sponsors) == 0, (
            f"sponsor_rankings() includes non-sponsor entities: "
            f"{non_sponsors['party_name'].tolist()}"
        )

    def test_sponsor_sector_heatmap_has_deals(self, seeded_db):
        """All cells in the sponsor × sector heatmap must be >= 0 (no negative counts)."""
        df = sponsor_intel.sponsor_sector_heatmap()
        if df.empty:
            pytest.skip("No sponsor heatmap data")
        numeric_cols = [c for c in df.columns if c != "sponsor_name"]
        for col in numeric_cols:
            assert (df[col] >= 0).all(), f"Negative count in heatmap column '{col}'"

    def test_most_active_sponsor_is_in_rankings(self, seeded_db):
        """most_active_sponsor() must return the same name as the first row in sponsor_rankings()."""
        top_name = sponsor_intel.most_active_sponsor()
        if top_name == "N/A":
            pytest.skip("No sponsor data found")
        rankings = sponsor_intel.sponsor_rankings(top_n=1)
        assert not rankings.empty
        first_in_rankings = rankings.iloc[0]["sponsor_name"]
        assert top_name == first_in_rankings, (
            f"most_active_sponsor() = '{top_name}' but rankings[0] = '{first_in_rankings}'"
        )

    def test_sponsor_entry_multiples_positive(self, seeded_db):
        """All avg_ev_to_ebitda values in sponsor_entry_multiples() must be > 0."""
        df = sponsor_intel.sponsor_entry_multiples()
        if df.empty:
            pytest.skip("No sponsor EV/EBITDA data")
        assert (df["avg_ev_to_ebitda"] > 0).all(), (
            "Found non-positive avg_ev_to_ebitda in sponsor_entry_multiples()"
        )


# ===========================================================================
# Category 10: Snapshot memo quality
# ===========================================================================

class TestSnapshotMemoQuality:
    """Validate that the auto-generated M&A Market Snapshot meets quality criteria."""

    def test_snapshot_mentions_sector_data(self, seeded_db, config):
        """Snapshot memo must reference at least one configured sector name."""
        memo = generate_snapshot(filters=None, config=config)
        sector_names = [s["sector_name"] for s in config["sectors"]]
        assert any(sector in memo for sector in sector_names), (
            "Snapshot memo does not mention any of the configured sector names"
        )

    def test_snapshot_mentions_sponsor_data(self, seeded_db, config):
        """Snapshot memo must reference at least one configured sponsor name."""
        memo = generate_snapshot(filters=None, config=config)
        sponsor_names = config["sponsors"]
        assert any(sponsor in memo for sponsor in sponsor_names), (
            "Snapshot memo does not mention any sponsor name. "
            "Check whether sponsor data is included in the memo template."
        )

    def test_snapshot_real_only_filter_no_synthetic_mention(self, seeded_db, config):
        """Real-only filtered memo must report 0 synthetic records (not inflated counts).

        The memo data note always mentions the word 'synthetic', but when filtered
        to real-only the count should be 0.
        """
        memo = generate_snapshot(filters={"data_origin": "real"}, config=config)
        # Acceptable: mentions "synthetic" with count 0 (e.g., "+ 0 synthetic records")
        # Not acceptable: mentions synthetic with non-zero figures
        if "synthetic" in memo.lower():
            assert "0 synthetic" in memo, (
                "Real-only snapshot memo mentions synthetic data without a 0 count — "
                "check that the filter is applied to kpis correctly"
            )

    def test_snapshot_length_within_config_limit(self, seeded_db, config):
        """Snapshot memo word count must not exceed config snapshot.max_length_words (300)."""
        memo = generate_snapshot(filters=None, config=config)
        word_count = len(memo.split())
        max_words = config["snapshot"]["max_length_words"]
        assert word_count <= max_words, (
            f"Snapshot has {word_count} words, exceeds configured limit of {max_words}"
        )


# ===========================================================================
# Category 11: Export data integrity
# ===========================================================================

class TestExportIntegrity:
    """Validate that CSV and Excel exports faithfully reflect the database state."""

    def _csv_config(self, config: dict, tmp_path) -> dict:
        """Return a config copy with csv output_dir pointing to tmp_path."""
        cfg = copy.deepcopy(config)
        cfg.setdefault("export", {}).setdefault("csv", {})["output_dir"] = str(tmp_path)
        return cfg

    def _excel_config(self, config: dict, tmp_path) -> dict:
        """Return a config copy with excel output_dir pointing to tmp_path."""
        cfg = copy.deepcopy(config)
        cfg.setdefault("export", {}).setdefault("excel", {})["output_dir"] = str(tmp_path)
        return cfg

    def test_csv_export_row_count_matches_db(self, seeded_db, config, tmp_path):
        """Unfiltered CSV export must have exactly as many rows as there are deals in DB."""
        cfg = self._csv_config(config, tmp_path)
        filepath = export_deals_csv(filters=None, config=cfg, filename="count_check.csv")
        df = pd.read_csv(filepath)
        db_count = queries.get_deals_count()
        assert len(df) == db_count, (
            f"CSV has {len(df)} rows but DB has {db_count} deals"
        )

    def test_csv_export_all_key_columns_present(self, seeded_db, config, tmp_path):
        """CSV must contain all columns required for downstream analytical use."""
        required = [
            "deal_id", "announcement_date", "target_name",
            "deal_type", "deal_value_usd", "data_origin",
        ]
        cfg = self._csv_config(config, tmp_path)
        filepath = export_deals_csv(filters=None, config=cfg, filename="cols_check.csv")
        df = pd.read_csv(filepath)
        missing = [c for c in required if c not in df.columns]
        assert len(missing) == 0, f"CSV missing required columns: {missing}"

    def test_excel_export_deals_sheet_matches_csv(self, seeded_db, config, tmp_path):
        """Excel 'Deals' sheet row count must match the unfiltered CSV export row count."""
        csv_cfg = self._csv_config(config, tmp_path)
        xlsx_cfg = self._excel_config(config, tmp_path)
        csv_path = export_deals_csv(filters=None, config=csv_cfg, filename="compare.csv")
        xlsx_path = export_deals_excel(filters=None, config=xlsx_cfg, filename="compare.xlsx")
        csv_df = pd.read_csv(csv_path)
        xlsx_df = pd.read_excel(xlsx_path, sheet_name="Deals")
        assert len(csv_df) == len(xlsx_df), (
            f"CSV has {len(csv_df)} rows but Excel 'Deals' sheet has {len(xlsx_df)} rows"
        )

    def test_export_filtered_by_origin(self, seeded_db, config, tmp_path):
        """Real-only filtered CSV export must contain only real deals."""
        cfg = self._csv_config(config, tmp_path)
        filepath = export_deals_csv(
            filters={"data_origin": "real"}, config=cfg, filename="real_only.csv"
        )
        df = pd.read_csv(filepath)
        assert not df.empty, "Real-only export returned an empty CSV"
        assert (df["data_origin"] == "real").all(), (
            "Real-only filtered CSV contains non-real rows"
        )


# ===========================================================================
# Category 12: Database-level constraints
# ===========================================================================

class TestDatabaseConstraints:
    """Verify PRIMARY KEY enforcement at the raw DuckDB layer.

    Strategy: insert a record, attempt a duplicate insert, verify count stays at 1.
    Both 'INSERT OR IGNORE' (silent) and an exception (strict) count as 'rejected'.
    """

    def test_deal_id_uniqueness(self, db):
        """Inserting a duplicate deal_id must be silently rejected or raise an error."""
        conn = get_connection()
        conn.execute("INSERT OR IGNORE INTO parties VALUES ('tp001', 'TestCo', 'strategic', 'US', NULL)")
        conn.execute("INSERT OR IGNORE INTO sectors VALUES ('ts001', 'Technology', 'Software')")
        conn.execute("""
            INSERT INTO deals (deal_id, announcement_date, deal_type, deal_status,
                               target_name, data_origin, acquirer_party_id, sector_id)
            VALUES ('dup-deal-001', '2022-01-15', 'lbo', 'closed',
                    'Test Target', 'real', 'tp001', 'ts001')
        """)
        count_before = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE deal_id = 'dup-deal-001'"
        ).fetchone()[0]
        assert count_before == 1

        try:
            conn.execute("""
                INSERT INTO deals (deal_id, announcement_date, deal_type, deal_status,
                                   target_name, data_origin, acquirer_party_id, sector_id)
                VALUES ('dup-deal-001', '2023-06-01', 'merger', 'announced',
                        'Other Target', 'synthetic', 'tp001', 'ts001')
            """)
        except Exception:
            pass  # PK violation exception is also an acceptable enforcement mechanism

        count_after = conn.execute(
            "SELECT COUNT(*) FROM deals WHERE deal_id = 'dup-deal-001'"
        ).fetchone()[0]
        assert count_after == 1, (
            "Duplicate deal_id was inserted without rejection — PRIMARY KEY not enforced"
        )

    def test_party_id_uniqueness(self, db):
        """Inserting a duplicate party_id must be rejected (count must stay at 1)."""
        conn = get_connection()
        conn.execute("INSERT INTO parties VALUES ('dup-party-001', 'Original Corp', 'sponsor', NULL, NULL)")
        count_before = conn.execute(
            "SELECT COUNT(*) FROM parties WHERE party_id = 'dup-party-001'"
        ).fetchone()[0]
        assert count_before == 1

        try:
            conn.execute("INSERT INTO parties VALUES ('dup-party-001', 'Duplicate Corp', 'strategic', NULL, NULL)")
        except Exception:
            pass

        count_after = conn.execute(
            "SELECT COUNT(*) FROM parties WHERE party_id = 'dup-party-001'"
        ).fetchone()[0]
        assert count_after == 1, (
            "Duplicate party_id was inserted — PRIMARY KEY not enforced on parties table"
        )

    def test_sector_id_uniqueness(self, db):
        """Inserting a duplicate sector_id must be rejected (count must stay at 1)."""
        conn = get_connection()
        conn.execute("INSERT INTO sectors VALUES ('dup-sector-001', 'Technology', 'Software')")
        count_before = conn.execute(
            "SELECT COUNT(*) FROM sectors WHERE sector_id = 'dup-sector-001'"
        ).fetchone()[0]
        assert count_before == 1

        try:
            conn.execute("INSERT INTO sectors VALUES ('dup-sector-001', 'Healthcare', 'Pharma')")
        except Exception:
            pass

        count_after = conn.execute(
            "SELECT COUNT(*) FROM sectors WHERE sector_id = 'dup-sector-001'"
        ).fetchone()[0]
        assert count_after == 1, (
            "Duplicate sector_id was inserted — PRIMARY KEY not enforced on sectors table"
        )
