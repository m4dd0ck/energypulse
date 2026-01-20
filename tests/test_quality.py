"""Tests for data quality checks."""

from datetime import datetime, timedelta

import pytest

from energypulse.models import EnergyRecord, QualityStatus, WeatherRecord
from energypulse.quality import QualityChecker


@pytest.fixture
def checker() -> QualityChecker:
    return QualityChecker()


@pytest.fixture
def sample_weather() -> list[WeatherRecord]:
    """Generate sample weather data for testing."""
    base_time = datetime.now() - timedelta(hours=48)
    return [
        WeatherRecord(
            timestamp=base_time + timedelta(hours=i),
            temperature_c=20.0 + i % 10,
            humidity_pct=50.0,
            wind_speed_kmh=10.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        )
        for i in range(48)
    ]


@pytest.fixture
def sample_energy() -> list[EnergyRecord]:
    """Generate sample energy data for testing."""
    base_time = datetime.now() - timedelta(hours=48)
    return [
        EnergyRecord(
            timestamp=base_time + timedelta(hours=i),
            demand_mwh=5000.0 + (i % 24) * 100,
            temperature_c=20.0,
            is_weekend=i % 7 >= 5,
            hour_of_day=i % 24,
            location="new_york",
        )
        for i in range(48)
    ]


class TestWeatherQualityChecks:
    def test_completeness_pass(
        self, checker: QualityChecker, sample_weather: list[WeatherRecord]
    ) -> None:
        results = checker.check_weather(sample_weather)
        completeness = next(r for r in results if r.check_name == "weather_completeness")
        assert completeness.status == QualityStatus.PASS

    def test_completeness_fail_with_few_records(self, checker: QualityChecker) -> None:
        few_records = [
            WeatherRecord(
                timestamp=datetime.now(),
                temperature_c=20.0,
                humidity_pct=50.0,
                wind_speed_kmh=10.0,
                precipitation_mm=0.0,
                cloud_cover_pct=30.0,
                location="test",
            )
            for _ in range(5)
        ]
        results = checker.check_weather(few_records)
        completeness = next(r for r in results if r.check_name == "weather_completeness")
        assert completeness.status == QualityStatus.FAIL

    def test_uniqueness_pass(
        self, checker: QualityChecker, sample_weather: list[WeatherRecord]
    ) -> None:
        results = checker.check_weather(sample_weather)
        uniqueness = next(r for r in results if r.check_name == "uniqueness")
        assert uniqueness.status == QualityStatus.PASS

    def test_uniqueness_detects_duplicates(self, checker: QualityChecker) -> None:
        ts = datetime.now()
        duplicates = [
            WeatherRecord(
                timestamp=ts,
                temperature_c=20.0,
                humidity_pct=50.0,
                wind_speed_kmh=10.0,
                precipitation_mm=0.0,
                cloud_cover_pct=30.0,
                location="test",
            )
            for _ in range(30)
        ]
        results = checker.check_weather(duplicates)
        uniqueness = next(r for r in results if r.check_name == "uniqueness")
        assert uniqueness.status == QualityStatus.FAIL
        assert uniqueness.metric_value == 29  # 29 duplicates


class TestEnergyQualityChecks:
    def test_completeness_pass(
        self, checker: QualityChecker, sample_energy: list[EnergyRecord]
    ) -> None:
        results = checker.check_energy(sample_energy)
        completeness = next(r for r in results if r.check_name == "energy_completeness")
        assert completeness.status == QualityStatus.PASS

    def test_demand_range_pass(
        self, checker: QualityChecker, sample_energy: list[EnergyRecord]
    ) -> None:
        results = checker.check_energy(sample_energy)
        demand_range = next(r for r in results if r.check_name == "demand_range")
        assert demand_range.status == QualityStatus.PASS

    def test_demand_consistency_pass(
        self, checker: QualityChecker, sample_energy: list[EnergyRecord]
    ) -> None:
        results = checker.check_energy(sample_energy)
        consistency = next(r for r in results if r.check_name == "demand_consistency")
        assert consistency.status == QualityStatus.PASS
