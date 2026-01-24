"""Data quality checks for weather and energy data."""

from collections.abc import Sequence
from datetime import datetime, timedelta

import structlog

from energypulse.models import (
    EnergyRecord,
    QualityCheckResult,
    QualityStatus,
    WeatherRecord,
)

log = structlog.get_logger()


class QualityChecker:
    """Runs data quality checks on ingested data."""

    def check_weather(self, records: Sequence[WeatherRecord]) -> list[QualityCheckResult]:
        """Run all quality checks on weather data."""
        results = []

        results.append(self._check_completeness(records, "weather"))
        results.append(self._check_freshness(records, max_age_hours=48))
        results.append(self._check_temperature_range(records))
        results.append(self._check_uniqueness(records))
        results.append(self._check_no_gaps(records))

        passed = sum(1 for r in results if r.status == QualityStatus.PASS)
        log.info("weather_quality_complete", passed=passed, total=len(results))

        return results

    def check_energy(self, records: Sequence[EnergyRecord]) -> list[QualityCheckResult]:
        """Run all quality checks on energy data."""
        results = []

        results.append(self._check_completeness(records, "energy"))
        results.append(self._check_demand_range(records))
        results.append(self._check_uniqueness(records))
        results.append(self._check_demand_consistency(records))

        passed = sum(1 for r in results if r.status == QualityStatus.PASS)
        log.info("energy_quality_complete", passed=passed, total=len(results))

        return results

    def _check_completeness(
        self, records: Sequence[WeatherRecord | EnergyRecord], data_type: str
    ) -> QualityCheckResult:
        count = len(records)
        threshold = 24  # At least 24 hours of data

        if count >= threshold:
            status = QualityStatus.PASS
            message = f"Found {count} {data_type} records (threshold: {threshold})"
        elif count >= threshold // 2:
            status = QualityStatus.WARN
            message = f"Low record count: {count} {data_type} records (threshold: {threshold})"
        else:
            status = QualityStatus.FAIL
            message = f"Insufficient data: {count} {data_type} records (threshold: {threshold})"

        return QualityCheckResult(
            check_name=f"{data_type}_completeness",
            status=status,
            metric_value=count,
            threshold=threshold,
            message=message,
        )

    def _check_freshness(
        self, records: Sequence[WeatherRecord], max_age_hours: int
    ) -> QualityCheckResult:
        if not records:
            return QualityCheckResult(
                check_name="weather_freshness",
                status=QualityStatus.FAIL,
                message="No records to check",
            )

        latest = max(r.timestamp for r in records)
        age = datetime.now() - latest
        age_hours = age.total_seconds() / 3600

        if age_hours <= max_age_hours:
            status = QualityStatus.PASS
            message = f"Latest data is {age_hours:.1f} hours old"
        elif age_hours <= max_age_hours * 2:
            status = QualityStatus.WARN
            message = f"Data is stale: {age_hours:.1f} hours old (threshold: {max_age_hours}h)"
        else:
            status = QualityStatus.FAIL
            message = f"Data is very stale: {age_hours:.1f} hours old (threshold: {max_age_hours}h)"

        return QualityCheckResult(
            check_name="weather_freshness",
            status=status,
            metric_value=age_hours,
            threshold=max_age_hours,
            message=message,
        )

    def _check_temperature_range(
        self, records: Sequence[WeatherRecord]
    ) -> QualityCheckResult:
        if not records:
            return QualityCheckResult(
                check_name="temperature_range",
                status=QualityStatus.FAIL,
                message="No records to check",
            )

        # Realistic range for US cities
        min_temp, max_temp = -40, 50

        temps = [r.temperature_c for r in records]
        out_of_range = [t for t in temps if not (min_temp <= t <= max_temp)]

        if not out_of_range:
            status = QualityStatus.PASS
            message = f"All {len(temps)} temperatures within range [{min_temp}, {max_temp}]°C"
        else:
            pct = len(out_of_range) / len(temps) * 100
            status = QualityStatus.FAIL if pct > 5 else QualityStatus.WARN
            message = f"{len(out_of_range)} temps ({pct:.1f}%) outside range [{min_temp}, {max_temp}]°C"

        return QualityCheckResult(
            check_name="temperature_range",
            status=status,
            metric_value=len(out_of_range),
            threshold=0,
            message=message,
        )

    def _check_uniqueness(
        self, records: Sequence[WeatherRecord | EnergyRecord]
    ) -> QualityCheckResult:
        if not records:
            return QualityCheckResult(
                check_name="uniqueness",
                status=QualityStatus.FAIL,
                message="No records to check",
            )

        seen = set()
        duplicates = 0
        for r in records:
            key = (r.timestamp, r.location)
            if key in seen:
                duplicates += 1
            seen.add(key)

        if duplicates == 0:
            status = QualityStatus.PASS
            message = f"All {len(records)} records are unique by timestamp+location"
        else:
            pct = duplicates / len(records) * 100
            status = QualityStatus.FAIL if pct > 1 else QualityStatus.WARN
            message = f"Found {duplicates} duplicate records ({pct:.1f}%)"

        return QualityCheckResult(
            check_name="uniqueness",
            status=status,
            metric_value=duplicates,
            threshold=0,
            message=message,
        )

    def _check_no_gaps(self, records: Sequence[WeatherRecord]) -> QualityCheckResult:
        if len(records) < 2:
            return QualityCheckResult(
                check_name="no_gaps",
                status=QualityStatus.WARN,
                message="Not enough records to check for gaps",
            )

        # Group by location
        by_location: dict[str, list[datetime]] = {}
        for r in records:
            by_location.setdefault(r.location, []).append(r.timestamp)

        total_gaps = 0
        for _location, timestamps in by_location.items():
            timestamps.sort()
            for i in range(1, len(timestamps)):
                gap = timestamps[i] - timestamps[i - 1]
                if gap > timedelta(hours=1, minutes=15):  # Allow 15min tolerance
                    total_gaps += 1

        if total_gaps == 0:
            status = QualityStatus.PASS
            message = "No gaps detected in hourly data"
        elif total_gaps <= 3:
            status = QualityStatus.WARN
            message = f"Found {total_gaps} gaps in hourly data"
        else:
            status = QualityStatus.FAIL
            message = f"Found {total_gaps} gaps in hourly data (data may be incomplete)"

        return QualityCheckResult(
            check_name="no_gaps",
            status=status,
            metric_value=total_gaps,
            threshold=0,
            message=message,
        )

    def _check_demand_range(self, records: Sequence[EnergyRecord]) -> QualityCheckResult:
        if not records:
            return QualityCheckResult(
                check_name="demand_range",
                status=QualityStatus.FAIL,
                message="No records to check",
            )

        # Reasonable range for city-level demand
        min_demand, max_demand = 500, 15000

        demands = [r.demand_mwh for r in records]
        out_of_range = [d for d in demands if not (min_demand <= d <= max_demand)]

        if not out_of_range:
            status = QualityStatus.PASS
            message = f"All {len(demands)} demand values within range [{min_demand}, {max_demand}] MWh"
        else:
            pct = len(out_of_range) / len(demands) * 100
            status = QualityStatus.FAIL if pct > 5 else QualityStatus.WARN
            message = f"{len(out_of_range)} demands ({pct:.1f}%) outside expected range"

        return QualityCheckResult(
            check_name="demand_range",
            status=status,
            metric_value=len(out_of_range),
            threshold=0,
            message=message,
        )

    def _check_demand_consistency(self, records: Sequence[EnergyRecord]) -> QualityCheckResult:
        if len(records) < 2:
            return QualityCheckResult(
                check_name="demand_consistency",
                status=QualityStatus.WARN,
                message="Not enough records to check consistency",
            )

        # Group by location and sort by time
        by_location: dict[str, list[EnergyRecord]] = {}
        for r in records:
            by_location.setdefault(r.location, []).append(r)

        spike_count = 0
        max_pct_change = 50  # Flag >50% hour-to-hour change

        for _location, loc_records in by_location.items():
            loc_records.sort(key=lambda x: x.timestamp)
            for i in range(1, len(loc_records)):
                prev, curr = loc_records[i - 1].demand_mwh, loc_records[i].demand_mwh
                if prev > 0:
                    pct_change = abs(curr - prev) / prev * 100
                    if pct_change > max_pct_change:
                        spike_count += 1

        if spike_count == 0:
            status = QualityStatus.PASS
            message = "Demand changes are consistent (no sudden spikes)"
        elif spike_count <= 5:
            status = QualityStatus.WARN
            message = f"Found {spike_count} unusual demand changes (>{max_pct_change}% hour-to-hour)"
        else:
            status = QualityStatus.FAIL
            message = f"Found {spike_count} unusual demand spikes - check data quality"

        return QualityCheckResult(
            check_name="demand_consistency",
            status=status,
            metric_value=spike_count,
            threshold=0,
            message=message,
        )
