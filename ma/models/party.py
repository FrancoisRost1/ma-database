"""
Party dataclass — unified model for sponsors and strategic acquirers.
A single parties table handles both PE sponsors and corporate acquirers,
allowing sponsor vs strategic comparisons across the entire dataset.
"""
from dataclasses import dataclass, field
from typing import Optional
import uuid


@dataclass
class Party:
    """
    Unified entity: PE sponsor, strategic acquirer, or consortium.
    party_type distinguishes the acquirer category used in analytics.
    """

    party_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    party_name: str = ""
    party_type: str = ""      # sponsor / strategic / consortium / other
    headquarters: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dict for DuckDB insertion."""
        return {
            "party_id": self.party_id,
            "party_name": self.party_name,
            "party_type": self.party_type,
            "headquarters": self.headquarters,
            "description": self.description,
        }
