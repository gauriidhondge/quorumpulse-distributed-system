from typing import Dict, Optional
from pydantic import BaseModel

class SearchRequest(BaseModel):
    blood_group: str
    units: int
    urgency: str = 'normal'
    vector: Optional[Dict[str, int]] = None

class SearchResponse(BaseModel):
    found: bool
    donor_id: Optional[str] = None
    city: Optional[str] = None
    units_available: Optional[int] = None
    served_by: Optional[str] = None
    vector: Optional[Dict[str, int]] = None
