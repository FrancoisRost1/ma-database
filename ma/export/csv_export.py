"""
CSV export — filtered deal list from the Deal Explorer.
Exports the full v_deals_flat columns. data_origin column always preserved.
"""
import os
import pandas as pd
from datetime import datetime
from ma.db import queries


def export_deals_csv(filters: dict = None, config: dict = None, filename: str = None) -> str:
    """
    Export filtered deals to CSV.
    Returns the full output filepath.
    Assumption: all values in millions USD as stored; no currency conversion.
    """
    output_dir = (config or {}).get("export", {}).get("csv", {}).get("output_dir", "outputs")
    os.makedirs(output_dir, exist_ok=True)

    if not filename:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"ma_deals_{ts}.csv"

    df = queries.get_all_deals(filters)
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)
    return filepath
