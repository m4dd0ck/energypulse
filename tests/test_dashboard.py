"""Tests for the dashboard's data loaders."""

from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from energypulse.dashboard.app import load_energy_data, load_weather_data
from energypulse.models import EnergyRecord, WeatherRecord
from energypulse.storage import Storage


@pytest.fixture
def storage(tmp_path: Path) -> Iterator[Storage]:
    store = Storage(tmp_path / "test.duckdb")
    base = datetime(2024, 1, 15, 0, 0)
    store.save_energy(
        [
            EnergyRecord(
                timestamp=base + timedelta(hours=i),
                demand_mwh=5000.0,
                temperature_c=20.0,
                is_weekend=False,
                hour_of_day=i,
                location=location,
            )
            for location in ("new_york", "chicago")
            for i in range(3)
        ]
    )
    store.save_weather(
        [
            WeatherRecord(
                timestamp=base + timedelta(hours=i),
                temperature_c=20.0,
                humidity_pct=50.0,
                wind_speed_kmh=10.0,
                precipitation_mm=0.0,
                cloud_cover_pct=30.0,
                location=location,
            )
            for location in ("new_york", "chicago")
            for i in range(3)
        ]
    )
    yield store
    store.close()


class TestLoaders:
    def test_energy_filters_by_location(self, storage: Storage) -> None:
        df = load_energy_data(storage, "chicago")
        assert len(df) == 3
        assert set(df["location"]) == {"chicago"}

    def test_weather_filters_by_location(self, storage: Storage) -> None:
        df = load_weather_data(storage, "new_york")
        assert len(df) == 3
        assert set(df["location"]) == {"new_york"}

    @pytest.mark.parametrize("hostile", ["new_york' OR '1'='1", "x'; DROP TABLE energy; --"])
    def test_location_is_bound_not_interpolated(self, storage: Storage, hostile: str) -> None:
        # A bound parameter is matched literally, so a hostile string is just
        # a location nobody has and returns nothing.
        assert load_energy_data(storage, hostile).empty
        assert load_weather_data(storage, hostile).empty
        assert len(load_energy_data(storage, "new_york")) == 3
