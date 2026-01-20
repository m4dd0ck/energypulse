"""Tests for data models."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from energypulse.models import EnergyRecord, QualityStatus, WeatherRecord


class TestWeatherRecord:
    def test_valid_record(self) -> None:
        record = WeatherRecord(
            timestamp=datetime.now(),
            temperature_c=20.5,
            humidity_pct=65.0,
            wind_speed_kmh=15.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        )
        assert record.temperature_c == 20.5
        assert record.location == "new_york"

    def test_temperature_range_validation(self) -> None:
        with pytest.raises(ValidationError):
            WeatherRecord(
                timestamp=datetime.now(),
                temperature_c=100.0,  # Too hot
                humidity_pct=50.0,
                wind_speed_kmh=10.0,
                precipitation_mm=0.0,
                cloud_cover_pct=0.0,
                location="test",
            )

    def test_humidity_range_validation(self) -> None:
        with pytest.raises(ValidationError):
            WeatherRecord(
                timestamp=datetime.now(),
                temperature_c=20.0,
                humidity_pct=150.0,  # Invalid
                wind_speed_kmh=10.0,
                precipitation_mm=0.0,
                cloud_cover_pct=0.0,
                location="test",
            )


class TestEnergyRecord:
    def test_valid_record(self) -> None:
        record = EnergyRecord(
            timestamp=datetime.now(),
            demand_mwh=5000.0,
            temperature_c=25.0,
            is_weekend=False,
            hour_of_day=14,
            location="new_york",
        )
        assert record.demand_mwh == 5000.0
        assert record.hour_of_day == 14

    def test_negative_demand_rejected(self) -> None:
        with pytest.raises(ValidationError):
            EnergyRecord(
                timestamp=datetime.now(),
                demand_mwh=-100.0,  # Invalid
                temperature_c=20.0,
                is_weekend=False,
                hour_of_day=12,
                location="test",
            )

    def test_invalid_hour_rejected(self) -> None:
        with pytest.raises(ValidationError):
            EnergyRecord(
                timestamp=datetime.now(),
                demand_mwh=5000.0,
                temperature_c=20.0,
                is_weekend=False,
                hour_of_day=25,  # Invalid
                location="test",
            )


class TestQualityStatus:
    def test_enum_values(self) -> None:
        assert QualityStatus.PASS.value == "pass"
        assert QualityStatus.FAIL.value == "fail"
        assert QualityStatus.WARN.value == "warn"
