"""Tests: CSV and Excel export."""
import os
import pandas as pd


def test_csv_export_creates_file(seeded_db, config, tmp_path):
    from ma.export.csv_export import export_deals_csv
    cfg = dict(config)
    cfg["export"] = {"csv": {"output_dir": str(tmp_path)}}
    path = export_deals_csv(config=cfg)
    assert os.path.exists(path)
    df = pd.read_csv(path)
    assert len(df) > 0


def test_csv_export_preserves_data_origin(seeded_db, config, tmp_path):
    from ma.export.csv_export import export_deals_csv
    cfg = dict(config)
    cfg["export"] = {"csv": {"output_dir": str(tmp_path)}}
    path = export_deals_csv(config=cfg)
    df = pd.read_csv(path)
    assert "data_origin" in df.columns
    assert set(df["data_origin"].unique()).issubset({"real", "synthetic"})


def test_excel_export_creates_file(seeded_db, config, tmp_path):
    from ma.export.excel_export import export_deals_excel
    cfg = dict(config)
    cfg["export"] = {"excel": {"output_dir": str(tmp_path), "include_summary_sheet": True, "include_valuation_sheet": True}}
    path = export_deals_excel(config=cfg)
    assert os.path.exists(path)
    assert path.endswith(".xlsx")


def test_excel_export_has_multiple_sheets(seeded_db, config, tmp_path):
    from ma.export.excel_export import export_deals_excel
    cfg = dict(config)
    cfg["export"] = {"excel": {"output_dir": str(tmp_path), "include_summary_sheet": True, "include_valuation_sheet": True}}
    path = export_deals_excel(config=cfg)
    xl = pd.ExcelFile(path)
    assert "Deals" in xl.sheet_names
