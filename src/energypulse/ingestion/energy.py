"""Energy demand simulation based on weather data."""

import random

import structlog

from energypulse.models import EnergyRecord, WeatherRecord

log = structlog.get_logger()

# Base load varies by city population/size (MWh)
BASE_LOAD = {
    "new_york": 5000,
    "los_angeles": 4500,
    "chicago": 3500,
    "houston": 4000,
    "phoenix": 3000,
}

# Comfortable temperature range (Celsius) - outside this range, HVAC kicks in
COMFORT_MIN = 18
COMFORT_MAX = 24


class EnergySimulator:
    """Simulates energy demand based on weather conditions."""

    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)

    def simulate_from_weather(self, weather_records: list[WeatherRecord]) -> list[EnergyRecord]:
        """Generate energy demand records from weather data.

        Args:
            weather_records: List of weather observations

        Returns:
            List of energy demand records (one per weather record)
        """
        log.info("simulating_energy", input_records=len(weather_records))

        energy_records = []
        for weather in weather_records:
            demand = self._calculate_demand(weather)
            record = EnergyRecord(
                timestamp=weather.timestamp,
                demand_mwh=round(demand, 2),
                temperature_c=weather.temperature_c,
                is_weekend=weather.timestamp.weekday() >= 5,
                hour_of_day=weather.timestamp.hour,
                location=weather.location,
            )
            energy_records.append(record)

        log.info("energy_simulated", output_records=len(energy_records))
        return energy_records

    def _calculate_demand(self, weather: WeatherRecord) -> float:
        """Calculate energy demand for a single hour based on weather.

        Model components:
        1. Base load (always-on infrastructure)
        2. Temperature-driven load (HVAC)
        3. Time-of-day multiplier (peak hours)
        4. Weekend discount (commercial buildings closed)
        5. Random noise (real-world variability)
        """
        base = BASE_LOAD.get(weather.location, 3000)
        hour = weather.timestamp.hour
        is_weekend = weather.timestamp.weekday() >= 5
        temp = weather.temperature_c

        # Temperature-driven HVAC load
        # Increases quadratically as we move away from comfort zone
        if temp < COMFORT_MIN:
            # Heating load
            temp_load = base * 0.3 * ((COMFORT_MIN - temp) / 20) ** 1.5
        elif temp > COMFORT_MAX:
            # Cooling load (AC is less efficient, so higher multiplier)
            temp_load = base * 0.4 * ((temp - COMFORT_MAX) / 20) ** 1.5
        else:
            temp_load = 0

        # Time-of-day multiplier
        # Peak hours: 7-9 AM (morning ramp), 5-8 PM (evening peak)
        if 7 <= hour <= 9:
            time_mult = 1.2
        elif 17 <= hour <= 20:
            time_mult = 1.35
        elif 0 <= hour <= 5:
            time_mult = 0.7  # Night valley
        else:
            time_mult = 1.0

        # Weekend reduction (commercial buildings)
        weekend_mult = 0.75 if is_weekend else 1.0

        # Combine factors
        demand = (base + temp_load) * time_mult * weekend_mult

        # Add realistic noise (±5%)
        noise = self._rng.gauss(1.0, 0.05)
        demand *= noise

        return float(max(0, demand))  # Demand can't be negative
