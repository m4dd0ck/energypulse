"""Tests for the Open-Meteo client."""

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

from energypulse.ingestion.weather import LOCATIONS, WeatherClient


def _fake_open_meteo(request: httpx.Request) -> httpx.Response:
    tz = request.url.params.get("timezone", "")
    body = {
        "timezone": tz,
        "hourly": {
            "time": ["2024-01-15T00:00", "2024-01-15T01:00"],
            "temperature_2m": [10.0, 11.0],
            "relative_humidity_2m": [50.0, 51.0],
            "wind_speed_10m": [5.0, 6.0],
            "precipitation": [0.0, 0.0],
            "cloud_cover": [20.0, 25.0],
        },
    }
    return httpx.Response(200, content=json.dumps(body).encode(), request=request)


class TestLocationTimezones:
    def test_every_location_has_a_valid_iana_timezone(self) -> None:
        for name, loc in LOCATIONS.items():
            ZoneInfo(loc.timezone)  # raises if the zone is unknown
            assert loc.timezone, name

    def test_only_new_york_uses_eastern_time(self) -> None:
        eastern = [name for name, loc in LOCATIONS.items() if loc.timezone == "America/New_York"]
        assert eastern == ["new_york"]


class TestWeatherClient:
    @pytest.mark.parametrize(
        ("location", "expected_tz"),
        [
            ("new_york", "America/New_York"),
            ("los_angeles", "America/Los_Angeles"),
            ("chicago", "America/Chicago"),
            ("houston", "America/Chicago"),
            ("phoenix", "America/Phoenix"),
        ],
    )
    def test_requests_use_the_city_timezone(self, location: str, expected_tz: str) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return _fake_open_meteo(request)

        with WeatherClient(transport=httpx.MockTransport(handler)) as client:
            records = client.fetch_historical(location, datetime(2024, 1, 15), datetime(2024, 1, 16))

        assert seen, "no request was made"
        assert all(r.url.params["timezone"] == expected_tz for r in seen)
        assert [r.location for r in records] == [location, location]

    def test_unknown_location_rejected(self) -> None:
        with WeatherClient(transport=httpx.MockTransport(_fake_open_meteo)) as client, pytest.raises(ValueError):
            client.fetch_historical("atlantis", datetime(2024, 1, 15), datetime(2024, 1, 16))
