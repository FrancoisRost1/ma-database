"""
Sector dataclass — GICS-style sector taxonomy (simplified to 11 sectors).
Sub-industries allow drill-down analytics within sectors.
"""
from dataclasses import dataclass, field
from typing import Optional
import uuid


@dataclass
class Sector:
    """
    GICS-style sector. sector_id is a stable slug (e.g. 'technology').
    sub_industry provides finer granularity for heatmap and valuation analytics.
    """

    sector_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sector_name: str = ""
    sub_industry: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dict for DuckDB insertion."""
        return {
            "sector_id": self.sector_id,
            "sector_name": self.sector_name,
            "sub_industry": self.sub_industry,
        }
