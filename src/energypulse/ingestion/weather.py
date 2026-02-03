"""Weather data ingestion from Open-Meteo API (free, no key required)."""

from datetime import datetime, timedelta

import httpx
import structlog

from energypulse.models import WeatherRecord

log = structlog.get_logger()

# Open-Meteo API - free, no API key needed
# Forecast endpoint: recent data (~7 days back) + forecast
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
# Archive endpoint: historical data going back years (free, no key needed)
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Major US cities for demo
LOCATIONS = {
    "new_york": {"lat": 40.7128, "lon": -74.0060},
    "los_angeles": {"lat": 34.0522, "lon": -118.2437},
    "chicago": {"lat": 41.8781, "lon": -87.6298},
    "houston": {"lat": 29.7604, "lon": -95.3698},
    "phoenix": {"lat": 33.4484, "lon": -112.0740},
}


class WeatherClient:
    """Client for fetching weather data from Open-Meteo API."""

    def __init__(self, timeout: float = 30.0) -> None:
        self._client = httpx.Client(timeout=timeout)  # 30s is generous but the API can be slow

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

        coords = LOCATIONS[location]
        log.info("fetching_weather", location=location, start=start_date.date(), end=end_date.date())

        # Determine which endpoint to use based on how far back we're going
        days_back = (datetime.now() - start_date).days
        use_archive = days_back > 7

        if use_archive:
            # Archive API works best in chunks of ~30 days for large ranges
            records = self._fetch_in_chunks(coords, location, start_date, end_date)
        else:
            # Forecast endpoint for recent data
            records = self._fetch_single(
                OPEN_METEO_FORECAST_URL, coords, location, start_date, end_date
            )

        log.info("weather_fetched", location=location, record_count=len(records))
        return records

    def _fetch_single(
        self,
        url: str,
        coords: dict[str, float],
        location: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[WeatherRecord]:
        """Fetch weather data from a single API call."""
        params: dict[str, str | float] = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,cloud_cover",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "timezone": "America/New_York",
        }

        response = self._client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        return self._parse_response(data, location)

    def _fetch_in_chunks(
        self,
        coords: dict[str, float],
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

            records = self._fetch_single(
                OPEN_METEO_ARCHIVE_URL, coords, location, current_start, current_end
            )
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

        coords = LOCATIONS[location]
        params: dict[str, str | float] = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,cloud_cover",
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
