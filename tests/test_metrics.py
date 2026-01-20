"""Tests for metrics engine."""

from datetime import datetime, timedelta

import pytest

from energypulse.metrics import MetricsEngine
from energypulse.models import EnergyRecord


@pytest.fixture
def engine() -> MetricsEngine:
    return MetricsEngine()


@pytest.fixture
def sample_energy() -> list[EnergyRecord]:
    """Generate sample energy data with known values."""
    base_time = datetime(2024, 1, 15, 0, 0)  # Monday
    records = []

    for i in range(168):  # One week of hourly data
        ts = base_time + timedelta(hours=i)
        # Predictable demand pattern
        base_demand = 5000.0
        hour = i % 24
        day = i // 24

        # Peak hours (17-20) get higher demand
        if 17 <= hour <= 20:
            demand = base_demand * 1.3
        elif 0 <= hour <= 5:
            demand = base_demand * 0.7
        else:
            demand = base_demand

        # Weekend reduction
        is_weekend = day >= 5
        if is_weekend:
            demand *= 0.8

        records.append(
            EnergyRecord(
                timestamp=ts,
                demand_mwh=demand,
                temperature_c=20.0,
                is_weekend=is_weekend,
                hour_of_day=hour,
                location="test",
            )
        )

    return records


class TestMetricsEngine:
    def test_total_demand(
        self, engine: MetricsEngine, sample_energy: list[EnergyRecord]
    ) -> None:
        result = engine.total_demand(sample_energy, {})
        assert result.metric_name == "total_demand"
        assert result.unit == "MWh"
        assert result.value > 0

    def test_peak_demand(
        self, engine: MetricsEngine, sample_energy: list[EnergyRecord]
    ) -> None:
        result = engine.peak_demand(sample_energy, {})
        assert result.metric_name == "peak_demand"
        # Peak should be during peak hours: 5000 * 1.3 = 6500
        assert result.value == 6500.0

    def test_average_demand(
        self, engine: MetricsEngine, sample_energy: list[EnergyRecord]
    ) -> None:
        result = engine.average_demand(sample_energy, {})
        assert result.metric_name == "average_demand"
        assert result.value > 0

    def test_peak_hour_ratio(
        self, engine: MetricsEngine, sample_energy: list[EnergyRecord]
    ) -> None:
        result = engine.peak_hour_ratio(sample_energy, {})
        assert result.metric_name == "peak_hour_ratio"
        assert result.unit == "ratio"
        # Peak (6500) / avg should be > 1
        assert result.value > 1.0

    def test_weekend_vs_weekday_ratio(
        self, engine: MetricsEngine, sample_energy: list[EnergyRecord]
    ) -> None:
        result = engine.weekend_vs_weekday(sample_energy, {})
        assert result.metric_name == "weekend_weekday_ratio"
        # Weekend has 0.8 multiplier, so ratio should be around 0.8
        assert 0.7 < result.value < 0.9

    def test_empty_records(self, engine: MetricsEngine) -> None:
        result = engine.total_demand([], {})
        assert result.value == 0

    def test_compute_all(
        self, engine: MetricsEngine, sample_energy: list[EnergyRecord]
    ) -> None:
        results = engine.compute_all(sample_energy, dimensions={"location": "test"})
        metric_names = {r.metric_name for r in results}

        assert "total_demand" in metric_names
        assert "peak_demand" in metric_names
        assert "average_demand" in metric_names
        assert "peak_hour_ratio" in metric_names
        assert "weekend_weekday_ratio" in metric_names

        # Check dimensions are passed through
        for result in results:
            assert result.dimensions == {"location": "test"}
