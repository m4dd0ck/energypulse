"""Semantic metrics for energy analytics."""

from collections.abc import Sequence

import structlog

from energypulse.models import EnergyRecord, MetricResult, WeatherRecord

log = structlog.get_logger()


class MetricsEngine:
    """Computes semantic metrics from energy and weather data."""

    def compute_all(
        self,
        energy_records: Sequence[EnergyRecord],
        weather_records: Sequence[WeatherRecord] | None = None,
        dimensions: dict[str, str] | None = None,
    ) -> list[MetricResult]:
        """Compute all available metrics.

        Args:
            energy_records: Energy demand data
            weather_records: Optional weather data for correlation metrics
            dimensions: Optional dimension filters (location, date range, etc.)

        Returns:
            List of computed metric results
        """
        dims = dimensions or {}
        results = []

        # Core demand metrics
        results.append(self.total_demand(energy_records, dims))
        results.append(self.peak_demand(energy_records, dims))
        results.append(self.average_demand(energy_records, dims))
        results.append(self.peak_hour_ratio(energy_records, dims))

        # Time-based metrics
        results.append(self.weekend_vs_weekday(energy_records, dims))
        results.append(self.peak_hour_demand(energy_records, dims))
        results.append(self.overnight_minimum(energy_records, dims))

        # Weather correlation (if weather data provided)
        if weather_records:
            results.append(self.temperature_sensitivity(energy_records, weather_records, dims))

        log.info("metrics_computed", count=len(results), dimensions=dims)
        return results

    def total_demand(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        total = sum(r.demand_mwh for r in records)
        return MetricResult(
            metric_name="total_demand",
            value=round(total, 2),
            unit="MWh",
            dimensions=dims,
        )

    def peak_demand(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        if not records:
            return MetricResult(
                metric_name="peak_demand", value=0, unit="MWh", dimensions=dims
            )
        peak = max(r.demand_mwh for r in records)
        return MetricResult(
            metric_name="peak_demand",
            value=round(peak, 2),
            unit="MWh",
            dimensions=dims,
        )

    def average_demand(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        if not records:
            return MetricResult(
                metric_name="average_demand", value=0, unit="MWh", dimensions=dims
            )
        avg = sum(r.demand_mwh for r in records) / len(records)
        return MetricResult(
            metric_name="average_demand",
            value=round(avg, 2),
            unit="MWh",
            dimensions=dims,
        )

    def peak_hour_ratio(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        """Ratio of peak demand to average demand.

        Higher ratio indicates more variable demand (spikier load profile).
        Values > 1.5 suggest need for demand response programs.
        """
        if not records:
            return MetricResult(
                metric_name="peak_hour_ratio", value=0, unit="ratio", dimensions=dims
            )

        peak = max(r.demand_mwh for r in records)
        avg = sum(r.demand_mwh for r in records) / len(records)
        ratio = peak / avg if avg > 0 else 0

        return MetricResult(
            metric_name="peak_hour_ratio",
            value=round(ratio, 3),
            unit="ratio",
            dimensions=dims,
        )

    def weekend_vs_weekday(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        """Ratio of weekend to weekday average demand.

        Values < 1 indicate lower weekend demand (typical for commercial areas).
        Values > 1 indicate higher weekend demand (residential/entertainment areas).
        """
        weekend = [r for r in records if r.is_weekend]
        weekday = [r for r in records if not r.is_weekend]

        if not weekend or not weekday:
            return MetricResult(
                metric_name="weekend_weekday_ratio",
                value=0,
                unit="ratio",
                dimensions=dims,
            )

        weekend_avg = sum(r.demand_mwh for r in weekend) / len(weekend)
        weekday_avg = sum(r.demand_mwh for r in weekday) / len(weekday)
        ratio = weekend_avg / weekday_avg if weekday_avg > 0 else 0

        return MetricResult(
            metric_name="weekend_weekday_ratio",
            value=round(ratio, 3),
            unit="ratio",
            dimensions=dims,
        )

    def peak_hour_demand(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        """Average demand during peak hours (5-8 PM)."""
        peak_hours = [r for r in records if 17 <= r.hour_of_day <= 20]

        if not peak_hours:
            return MetricResult(
                metric_name="peak_hour_demand",
                value=0,
                unit="MWh",
                dimensions=dims,
            )

        avg = sum(r.demand_mwh for r in peak_hours) / len(peak_hours)
        return MetricResult(
            metric_name="peak_hour_demand",
            value=round(avg, 2),
            unit="MWh",
            dimensions=dims,
        )

    def overnight_minimum(
        self, records: Sequence[EnergyRecord], dims: dict[str, str]
    ) -> MetricResult:
        """Average demand during overnight hours (12-5 AM).

        Represents base load - the minimum demand that's always required.
        """
        overnight = [r for r in records if 0 <= r.hour_of_day <= 5]

        if not overnight:
            return MetricResult(
                metric_name="overnight_minimum",
                value=0,
                unit="MWh",
                dimensions=dims,
            )

        avg = sum(r.demand_mwh for r in overnight) / len(overnight)
        return MetricResult(
            metric_name="overnight_minimum",
            value=round(avg, 2),
            unit="MWh",
            dimensions=dims,
        )

    def temperature_sensitivity(
        self,
        energy_records: Sequence[EnergyRecord],
        weather_records: Sequence[WeatherRecord],
        dims: dict[str, str],
    ) -> MetricResult:
        """Correlation between temperature and energy demand.

        Positive values: demand increases with temperature (AC load dominant)
        Negative values: demand increases with cold (heating load dominant)
        Values near 0: minimal temperature sensitivity
        """
        # Match records by timestamp and location
        weather_lookup = {
            (w.timestamp, w.location): w.temperature_c for w in weather_records
        }

        temps = []
        demands = []
        for e in energy_records:
            key = (e.timestamp, e.location)
            if key in weather_lookup:
                temps.append(weather_lookup[key])
                demands.append(e.demand_mwh)

        if len(temps) < 10:
            return MetricResult(
                metric_name="temperature_sensitivity",
                value=0,
                unit="correlation",
                dimensions=dims,
            )

        # Simple Pearson correlation
        n = len(temps)
        sum_t = sum(temps)
        sum_d = sum(demands)
        sum_tt = sum(t * t for t in temps)
        sum_dd = sum(d * d for d in demands)
        sum_td = sum(t * d for t, d in zip(temps, demands, strict=True))

        numerator = n * sum_td - sum_t * sum_d
        denominator = ((n * sum_tt - sum_t**2) * (n * sum_dd - sum_d**2)) ** 0.5

        correlation = numerator / denominator if denominator > 0 else 0

        return MetricResult(
            metric_name="temperature_sensitivity",
            value=round(correlation, 3),
            unit="correlation",
            dimensions=dims,
        )
