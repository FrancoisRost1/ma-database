"""Tests: field validation and duplicate detection."""
from ma.ingest.validator import validate_deal, validate_batch


def _base_row():
    return {
        "announcement_date": "2022-01-15",
        "target_name": "Test Co",
        "deal_type": "lbo",
        "deal_status": "closed",
        "data_origin": "real",
    }


def test_valid_row_passes(config):
    errors = validate_deal(_base_row(), config)
    assert errors == []


def test_missing_required_field(config):
    row = _base_row()
    del row["target_name"]
    errors = validate_deal(row, config)
    assert any("target_name" in e for e in errors)


def test_invalid_data_origin(config):
    row = {**_base_row(), "data_origin": "unknown"}
    errors = validate_deal(row, config)
    assert any("data_origin" in e for e in errors)


def test_invalid_deal_type(config):
    row = {**_base_row(), "deal_type": "hostile_takeover"}
    errors = validate_deal(row, config)
    assert any("deal_type" in e for e in errors)


def test_deal_value_out_of_range(config):
    row = {**_base_row(), "deal_value_usd": -100}
    errors = validate_deal(row, config)
    assert any("deal_value_usd" in e for e in errors)


def test_ev_ebitda_out_of_range(config):
    row = {**_base_row(), "ev_to_ebitda": 150.0}
    errors = validate_deal(row, config)
    assert any("ev_to_ebitda" in e for e in errors)


def test_closing_before_announcement(config):
    row = {**_base_row(), "closing_date": "2021-01-01"}
    errors = validate_deal(row, config)
    assert any("closing_date" in e for e in errors)


def test_batch_validation_returns_dataframe(config):
    rows = [_base_row(), {**_base_row(), "data_origin": "bad"}]
    df = validate_batch(rows, config)
    assert len(df) == 2
    assert df.iloc[0]["is_valid"] == True
    assert df.iloc[1]["is_valid"] == False
