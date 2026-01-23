"""DuckDB storage layer for persisting pipeline data."""

from collections.abc import Sequence
from pathlib import Path

import duckdb
import structlog

from energypulse.models import (
    EnergyRecord,
    MetricResult,
    QualityCheckResult,
    WeatherRecord,
)

log = structlog.get_logger()

DEFAULT_DB_PATH = Path("data/energypulse.duckdb")


class Storage:
    """DuckDB-based storage for all pipeline data."""

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(str(self._db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        """Create tables if they don't exist."""
        self._con.execute("""
            CREATE TABLE IF NOT EXISTS weather (
                timestamp TIMESTAMP,
                temperature_c DOUBLE,
                humidity_pct DOUBLE,
                wind_speed_kmh DOUBLE,
                precipitation_mm DOUBLE,
                cloud_cover_pct DOUBLE,
                location VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (timestamp, location)
            )
        """)

        self._con.execute("""
            CREATE TABLE IF NOT EXISTS energy (
                timestamp TIMESTAMP,
                demand_mwh DOUBLE,
                temperature_c DOUBLE,
                is_weekend BOOLEAN,
                hour_of_day INTEGER,
                location VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (timestamp, location)
            )
        """)

        self._con.execute("""
            CREATE TABLE IF NOT EXISTS quality_checks (
                check_id INTEGER PRIMARY KEY,
                check_name VARCHAR,
                status VARCHAR,
                metric_value DOUBLE,
                threshold DOUBLE,
                message VARCHAR,
                checked_at TIMESTAMP
            )
        """)

        self._con.execute("""
            CREATE SEQUENCE IF NOT EXISTS quality_check_seq START 1
        """)

        self._con.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                metric_id INTEGER PRIMARY KEY,
                metric_name VARCHAR,
                value DOUBLE,
                unit VARCHAR,
                dimensions VARCHAR,
                computed_at TIMESTAMP
            )
        """)

        self._con.execute("""
            CREATE SEQUENCE IF NOT EXISTS metric_seq START 1
        """)

        log.info("schema_initialized", db_path=str(self._db_path))

    def save_weather(self, records: Sequence[WeatherRecord]) -> int:
        """Save weather records (upsert on timestamp+location)."""
        if not records:
            return 0

        # Use INSERT OR REPLACE for upsert behavior
        self._con.executemany(
            """
            INSERT OR REPLACE INTO weather
            (timestamp, temperature_c, humidity_pct, wind_speed_kmh,
             precipitation_mm, cloud_cover_pct, location)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r.timestamp,
                    r.temperature_c,
                    r.humidity_pct,
                    r.wind_speed_kmh,
                    r.precipitation_mm,
                    r.cloud_cover_pct,
                    r.location,
                )
                for r in records
            ],
        )
        log.info("weather_saved", count=len(records))
        return len(records)

    def save_energy(self, records: Sequence[EnergyRecord]) -> int:
        """Save energy records (upsert on timestamp+location)."""
        if not records:
            return 0

        self._con.executemany(
            """
            INSERT OR REPLACE INTO energy
            (timestamp, demand_mwh, temperature_c, is_weekend, hour_of_day, location)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r.timestamp,
                    r.demand_mwh,
                    r.temperature_c,
                    r.is_weekend,
                    r.hour_of_day,
                    r.location,
                )
                for r in records
            ],
        )
        log.info("energy_saved", count=len(records))
        return len(records)

    def save_quality_results(self, results: Sequence[QualityCheckResult]) -> int:
        """Save quality check results."""
        if not results:
            return 0

        for r in results:
            self._con.execute(
                """
                INSERT INTO quality_checks
                (check_id, check_name, status, metric_value, threshold, message, checked_at)
                VALUES (nextval('quality_check_seq'), ?, ?, ?, ?, ?, ?)
                """,
                [r.check_name, r.status.value, r.metric_value, r.threshold, r.message, r.checked_at],
            )
        log.info("quality_results_saved", count=len(results))
        return len(results)

    def save_metrics(self, results: Sequence[MetricResult]) -> int:
        """Save computed metrics."""
        if not results:
            return 0

        for r in results:
            dims_str = str(r.dimensions) if r.dimensions else "{}"
            self._con.execute(
                """
                INSERT INTO metrics
                (metric_id, metric_name, value, unit, dimensions, computed_at)
                VALUES (nextval('metric_seq'), ?, ?, ?, ?, ?)
                """,
                [r.metric_name, r.value, r.unit, dims_str, r.computed_at],
            )
        log.info("metrics_saved", count=len(results))
        return len(results)

    def get_weather(
        self, location: str | None = None, limit: int = 1000
    ) -> list[WeatherRecord]:
        """Retrieve weather records."""
        query = "SELECT * FROM weather"
        params = []
        if location:
            query += " WHERE location = ?"
            params.append(location)
        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        result = self._con.execute(query, params).fetchall()
        return [
            WeatherRecord(
                timestamp=row[0],
                temperature_c=row[1],
                humidity_pct=row[2],
                wind_speed_kmh=row[3],
                precipitation_mm=row[4],
                cloud_cover_pct=row[5],
                location=row[6],
            )
            for row in result
        ]

    def get_energy(
        self, location: str | None = None, limit: int = 1000
    ) -> list[EnergyRecord]:
        """Retrieve energy records."""
        query = "SELECT * FROM energy"
        params = []
        if location:
            query += " WHERE location = ?"
            params.append(location)
        query += f" ORDER BY timestamp DESC LIMIT {limit}"

        result = self._con.execute(query, params).fetchall()
        return [
            EnergyRecord(
                timestamp=row[0],
                demand_mwh=row[1],
                temperature_c=row[2],
                is_weekend=row[3],
                hour_of_day=row[4],
                location=row[5],
            )
            for row in result
        ]

    def get_latest_metrics(self, limit: int = 50) -> list[dict[str, object]]:
        """Get most recent computed metrics."""
        result = self._con.execute(
            """
            SELECT metric_name, value, unit, dimensions, computed_at
            FROM metrics
            ORDER BY computed_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "metric_name": row[0],
                "value": row[1],
                "unit": row[2],
                "dimensions": row[3],
                "computed_at": row[4],
            }
            for row in result
        ]

    def get_quality_summary(self) -> dict[str, int]:
        """Get summary of recent quality check results."""
        result = self._con.execute("""
            WITH recent AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY check_name ORDER BY checked_at DESC) as rn
                FROM quality_checks
            )
            SELECT status, COUNT(*) as count
            FROM recent
            WHERE rn = 1
            GROUP BY status
        """).fetchall()

        return {row[0]: row[1] for row in result}

    def execute_query(self, query: str) -> list[tuple[object, ...]]:
        """Execute arbitrary SQL query (for dashboard)."""
        return self._con.execute(query).fetchall()

    def close(self) -> None:
        """Close database connection."""
        self._con.close()

    def __enter__(self) -> "Storage":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
