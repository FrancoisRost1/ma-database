"""
Deal dataclass — mirrors the 'deals' table schema exactly.
Used as the in-memory representation during ingestion and validation.
Simplifying assumption: single acquirer per deal (no joint ventures in Phase 1).
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
import uuid


@dataclass
class Deal:
    """
    Primary M&A transaction record.
    All monetary values are in millions USD.
    data_origin is ALWAYS required — 'real' or 'synthetic', never null.
    """

    # Primary key
    deal_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # Core transaction fields (required)
    announcement_date: Optional[date] = None
    target_name: str = ""
    deal_type: str = ""          # lbo / strategic_acquisition / merger / take_private / carve_out
    deal_status: str = ""        # announced / closed / terminated / pending
    data_origin: str = ""        # real / synthetic — NEVER null

    # Core transaction fields (optional)
    closing_date: Optional[date] = None
    deal_value_usd: Optional[float] = None   # millions USD
    deal_value_local: Optional[float] = None
    currency: str = "USD"
    enterprise_value: Optional[float] = None  # millions USD
    equity_value: Optional[float] = None      # millions USD

    # Parties (FK references resolved at ingestion)
    acquirer_party_id: Optional[str] = None
    target_party_id: Optional[str] = None

    # Classification
    target_status: Optional[str] = None      # public / private / subsidiary / carve_out
    sector_id: Optional[str] = None
    geography: str = "US"
    minority_or_control: Optional[str] = None   # control / minority / unknown
    hostile_or_friendly: Optional[str] = None   # hostile / friendly / unknown
    consortium_flag: bool = False

    # Qualitative
    financing_structure_text: Optional[str] = None
    notes: Optional[str] = None

    # Audit
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        """Convert to dict for DuckDB insertion."""
        return {
            "deal_id": self.deal_id,
            "announcement_date": self.announcement_date,
            "closing_date": self.closing_date,
            "deal_type": self.deal_type,
            "deal_status": self.deal_status,
            "deal_value_usd": self.deal_value_usd,
            "deal_value_local": self.deal_value_local,
            "currency": self.currency,
            "enterprise_value": self.enterprise_value,
            "equity_value": self.equity_value,
            "acquirer_party_id": self.acquirer_party_id,
            "target_name": self.target_name,
            "target_party_id": self.target_party_id,
            "target_status": self.target_status,
            "sector_id": self.sector_id,
            "geography": self.geography,
            "minority_or_control": self.minority_or_control,
            "hostile_or_friendly": self.hostile_or_friendly,
            "consortium_flag": self.consortium_flag,
            "financing_structure_text": self.financing_structure_text,
            "notes": self.notes,
            "data_origin": self.data_origin,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
