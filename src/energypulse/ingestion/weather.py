"""Weather data ingestion from Open-Meteo API (free, no key required)."""

from datetime import datetime, timedelta

import httpx
import structlog
from pydantic import BaseModel

from energypulse.models import WeatherRecord

log = structlog.get_logger()

# Open-Meteo API - free, no API key needed
# Forecast endpoint: recent data (~7 days back) + forecast
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
# Archive endpoint: historical data going back years (free, no key needed)
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


class Location(BaseModel, frozen=True):
    """Coordinates and IANA timezone of a supported city."""

    lat: float
    lon: float
    timezone: str


# Major US cities for demo. Each city is queried in its own timezone so the
# returned hourly timestamps are local wall-clock time; otherwise hour-of-day
# patterns (morning ramp, evening peak) would be shifted for every city but
# New York.
LOCATIONS = {
    "new_york": Location(lat=40.7128, lon=-74.0060, timezone="America/New_York"),
    "los_angeles": Location(lat=34.0522, lon=-118.2437, timezone="America/Los_Angeles"),
    "chicago": Location(lat=41.8781, lon=-87.6298, timezone="America/Chicago"),
    "houston": Location(lat=29.7604, lon=-95.3698, timezone="America/Chicago"),
    "phoenix": Location(lat=33.4484, lon=-112.0740, timezone="America/Phoenix"),
}


class WeatherClient:
    """Client for fetching weather data from Open-Meteo API."""

    def __init__(self, timeout: float = 30.0, transport: httpx.BaseTransport | None = None) -> None:
        # 30s is generous but the API can be slow. `transport` lets tests swap in httpx.MockTransport.
        self._client = httpx.Client(timeout=timeout, transport=transport)

    def fetch_historical(
        self,
        location: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[WeatherRecord]:
        """Fetch historical weather data for a location.

        Uses archive API for dates older than 7 days, forecast API for recent data.
        For long date ranges, fetches in chunks to avoid API timeouts.

        Args:
            location: City name (must be in LOCATIONS)
            start_date: Start of date range
            end_date: End of date range

        Returns:
            List of hourly weather records
        """
        if location not in LOCATIONS:
            raise ValueError(f"Unknown location: {location}. Valid: {list(LOCATIONS.keys())}")

        loc = LOCATIONS[location]
        log.info(
            "fetching_weather",
            location=location,
            timezone=loc.timezone,
            start=start_date.date(),
            end=end_date.date(),
        )

        # Determine which endpoint to use based on how far back we're going
        days_back = (datetime.now() - start_date).days
        use_archive = days_back > 7

        if use_archive:
            # Archive API works best in chunks of ~30 days for large ranges
            records = self._fetch_in_chunks(loc, location, start_date, end_date)
        else:
            # Forecast endpoint for recent data
            records = self._fetch_single(OPEN_METEO_FORECAST_URL, loc, location, start_date, end_date)

        log.info("weather_fetched", location=location, record_count=len(records))
        return records

    def _fetch_single(
        self,
        url: str,
        loc: Location,
        location: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[WeatherRecord]:
        """Fetch weather data from a single API call."""
        params: dict[str, str | float] = {
            "latitude": loc.lat,
            "longitude": loc.lon,
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,cloud_cover",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "timezone": loc.timezone,
        }

        response = self._client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        return self._parse_response(data, location)

    def _fetch_in_chunks(
        self,
        loc: Location,
        location: str,
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = 30,
    ) -> list[WeatherRecord]:
        """Fetch historical data in chunks to handle long date ranges."""
        all_records: list[WeatherRecord] = []
        current_start = start_date

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            log.info(
                "fetching_chunk",
                location=location,
                chunk_start=current_start.date(),
                chunk_end=current_end.date(),
            )

            records = self._fetch_single(OPEN_METEO_ARCHIVE_URL, loc, location, current_start, current_end)
            all_records.extend(records)
            current_start = current_end + timedelta(days=1)

        return all_records

    def _parse_response(self, data: dict, location: str) -> list[WeatherRecord]:  # type: ignore[type-arg]
        """Parse Open-Meteo API response into WeatherRecord objects."""
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        records = []
        for i, time_str in enumerate(times):
            try:
                record = WeatherRecord(
                    timestamp=datetime.fromisoformat(time_str),
                    temperature_c=hourly["temperature_2m"][i],
                    humidity_pct=hourly["relative_humidity_2m"][i],
                    wind_speed_kmh=hourly["wind_speed_10m"][i],
                    precipitation_mm=hourly["precipitation"][i],
                    cloud_cover_pct=hourly["cloud_cover"][i],
                    location=location,
                )
                records.append(record)
            except (KeyError, IndexError, ValueError) as e:
                log.warning("parse_error", index=i, error=str(e))
                continue

        return records

    def fetch_current(self, location: str) -> WeatherRecord | None:
        """Fetch current weather for a location."""
        if location not in LOCATIONS:
            raise ValueError(f"Unknown location: {location}")

        loc = LOCATIONS[location]
        params: dict[str, str | float] = {
            "latitude": loc.lat,
            "longitude": loc.lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,cloud_cover",
            "timezone": loc.timezone,
        }

        response = self._client.get(OPEN_METEO_FORECAST_URL, params=params)
        response.raise_for_status()
        data = response.json()

        current = data.get("current", {})
        if not current:
            return None

        return WeatherRecord(
            timestamp=datetime.fromisoformat(current["time"]),
            temperature_c=current["temperature_2m"],
            humidity_pct=current["relative_humidity_2m"],
            wind_speed_kmh=current["wind_speed_10m"],
            precipitation_mm=current["precipitation"],
            cloud_cover_pct=current["cloud_cover"],
            location=location,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "WeatherClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
