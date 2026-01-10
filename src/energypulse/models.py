"""Data models for the EnergyPulse pipeline."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class QualityStatus(str, Enum):
    """Quality check result status."""

    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"


class WeatherRecord(BaseModel):
    """Single weather observation from the API."""

    timestamp: datetime
    temperature_c: float = Field(ge=-60, le=60)
    humidity_pct: float = Field(ge=0, le=100)
    wind_speed_kmh: float = Field(ge=0)
    precipitation_mm: float = Field(ge=0)
    cloud_cover_pct: float = Field(ge=0, le=100)
    location: str


class EnergyRecord(BaseModel):
    """Energy demand record (simulated based on weather)."""

    timestamp: datetime
    demand_mwh: float = Field(ge=0)
    temperature_c: float
    is_weekend: bool
    hour_of_day: int = Field(ge=0, le=23)
    location: str


class QualityCheckResult(BaseModel):
    """Result of a data quality check."""

    check_name: str
    status: QualityStatus
    metric_value: float | None = None
    threshold: float | None = None
    message: str
    checked_at: datetime = Field(default_factory=datetime.now)


class MetricResult(BaseModel):
    """Computed metric value."""

    metric_name: str
    value: float
    unit: str
    dimensions: dict[str, str] = Field(default_factory=dict)
    computed_at: datetime = Field(default_factory=datetime.now)
