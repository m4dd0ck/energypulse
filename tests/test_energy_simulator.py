"""Tests for energy demand simulation."""

from datetime import datetime

import pytest

from energypulse.ingestion import EnergySimulator
from energypulse.models import WeatherRecord


@pytest.fixture
def simulator() -> EnergySimulator:
    return EnergySimulator(seed=42)


@pytest.fixture
def weather_records() -> list[WeatherRecord]:
    """Sample weather records for simulation."""
    return [
        WeatherRecord(
            timestamp=datetime(2024, 1, 15, 12, 0),  # Monday noon
            temperature_c=20.0,
            humidity_pct=50.0,
            wind_speed_kmh=10.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        ),
        WeatherRecord(
            timestamp=datetime(2024, 1, 15, 18, 0),  # Monday 6 PM (peak)
            temperature_c=25.0,
            humidity_pct=50.0,
            wind_speed_kmh=10.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        ),
        WeatherRecord(
            timestamp=datetime(2024, 1, 20, 12, 0),  # Saturday noon
            temperature_c=20.0,
            humidity_pct=50.0,
            wind_speed_kmh=10.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        ),
    ]


class TestEnergySimulator:
    def test_simulation_produces_records(
        self, simulator: EnergySimulator, weather_records: list[WeatherRecord]
    ) -> None:
        energy = simulator.simulate_from_weather(weather_records)
        assert len(energy) == len(weather_records)

    def test_demand_is_positive(
        self, simulator: EnergySimulator, weather_records: list[WeatherRecord]
    ) -> None:
        energy = simulator.simulate_from_weather(weather_records)
        for record in energy:
            assert record.demand_mwh > 0

    def test_peak_hours_have_higher_demand(
        self, simulator: EnergySimulator, weather_records: list[WeatherRecord]
    ) -> None:
        energy = simulator.simulate_from_weather(weather_records)
        # Index 1 is 6 PM (peak hour), index 0 is noon
        noon_demand = energy[0].demand_mwh
        peak_demand = energy[1].demand_mwh
        assert peak_demand > noon_demand

    def test_weekend_has_lower_demand(
        self, simulator: EnergySimulator, weather_records: list[WeatherRecord]
    ) -> None:
        energy = simulator.simulate_from_weather(weather_records)
        # Index 0 is Monday, index 2 is Saturday (same hour, same temp)
        weekday_demand = energy[0].demand_mwh
        weekend_demand = energy[2].demand_mwh
        assert weekend_demand < weekday_demand

    def test_seed_produces_reproducible_results(
        self, weather_records: list[WeatherRecord]
    ) -> None:
        sim1 = EnergySimulator(seed=123)
        sim2 = EnergySimulator(seed=123)

        energy1 = sim1.simulate_from_weather(weather_records)
        energy2 = sim2.simulate_from_weather(weather_records)

        for e1, e2 in zip(energy1, energy2, strict=True):
            assert e1.demand_mwh == e2.demand_mwh

    def test_extreme_cold_increases_demand(self, simulator: EnergySimulator) -> None:
        cold = WeatherRecord(
            timestamp=datetime(2024, 1, 15, 12, 0),
            temperature_c=-10.0,  # Very cold
            humidity_pct=50.0,
            wind_speed_kmh=10.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        )
        mild = WeatherRecord(
            timestamp=datetime(2024, 1, 15, 12, 0),
            temperature_c=20.0,  # Comfortable
            humidity_pct=50.0,
            wind_speed_kmh=10.0,
            precipitation_mm=0.0,
            cloud_cover_pct=30.0,
            location="new_york",
        )

        cold_energy = simulator.simulate_from_weather([cold])[0]
        mild_energy = simulator.simulate_from_weather([mild])[0]

        assert cold_energy.demand_mwh > mild_energy.demand_mwh
