from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class LibrarySummary(BaseModel):
    id: str
    name: str
    address: str | None = None
    lat: float
    lon: float
    city: str
    district: str
    accessibility_score: float = Field(ge=0.0, le=100.0)


class LibraryDetail(LibrarySummary):
    metrics: dict[str, Any] = Field(default_factory=dict)
    explain: dict[str, Any] = Field(default_factory=dict)
    explain_text: str | None = None


class OutreachRecommendation(BaseModel):
    id: str
    name: str
    type: str | None = None
    address: str | None = None
    lat: float
    lon: float
    city: str
    district: str
    outreach_score: float
    recommendation_explain: str | None = None
    covered_desert_cells: int = 0
    site_access_score: float = 0.0


class DesertCell(BaseModel):
    cell_id: str
    city: str
    centroid_lat: float
    centroid_lon: float
    effective_score_0_100: float
    is_desert: bool
    gap_to_threshold: float
    best_library_id: str | None = None
    best_library_distance_m: float | None = None

