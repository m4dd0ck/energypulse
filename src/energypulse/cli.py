"""Command-line interface for EnergyPulse pipeline."""

from datetime import datetime, timedelta
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.table import Table

from energypulse.ingestion import EnergySimulator, WeatherClient
from energypulse.metrics import MetricsEngine
from energypulse.quality import QualityChecker
from energypulse.storage import Storage

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)

app = typer.Typer(
    name="energypulse",
    help="End-to-end weather and energy analytics pipeline",
    no_args_is_help=True,
)
console = Console()


@app.command()
def ingest(
    location: str = typer.Option("new_york", help="City to fetch data for"),
    days: int = typer.Option(7, help="Number of days of historical data"),
    db_path: Path = typer.Option(Path("data/energypulse.duckdb"), help="Database path"),
) -> None:
    """Ingest weather data and simulate energy demand."""
    console.print(f"[bold blue]Ingesting data for {location}...[/bold blue]")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    with WeatherClient() as client:
        weather_records = client.fetch_historical(location, start_date, end_date)

    console.print(f"  Fetched {len(weather_records)} weather records")

    simulator = EnergySimulator(seed=42)
    energy_records = simulator.simulate_from_weather(weather_records)
    console.print(f"  Simulated {len(energy_records)} energy records")

    with Storage(db_path) as storage:
        storage.save_weather(weather_records)
        storage.save_energy(energy_records)

    console.print("[bold green]Ingestion complete![/bold green]")


@app.command()
def quality(
    db_path: Path = typer.Option(Path("data/energypulse.duckdb"), help="Database path"),
) -> None:
    """Run data quality checks."""
    console.print("[bold blue]Running quality checks...[/bold blue]")

    checker = QualityChecker()

    with Storage(db_path) as storage:
        weather = storage.get_weather()
        energy = storage.get_energy()

        weather_results = checker.check_weather(weather)
        energy_results = checker.check_energy(energy)

        all_results = weather_results + energy_results
        storage.save_quality_results(all_results)

    # Display results table
    table = Table(title="Quality Check Results")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Message")

    for result in all_results:
        status_style = {
            "pass": "[green]PASS[/green]",
            "warn": "[yellow]WARN[/yellow]",
            "fail": "[red]FAIL[/red]",
        }
        table.add_row(
            result.check_name,
            status_style.get(result.status.value, result.status.value),
            result.message,
        )

    console.print(table)

    passed = sum(1 for r in all_results if r.status.value == "pass")
    console.print(f"\n[bold]{passed}/{len(all_results)} checks passed[/bold]")


@app.command()
def metrics(
    location: str = typer.Option(None, help="Filter by location"),
    db_path: Path = typer.Option(Path("data/energypulse.duckdb"), help="Database path"),
) -> None:
    """Compute and display energy metrics."""
    console.print("[bold blue]Computing metrics...[/bold blue]")

    engine = MetricsEngine()

    with Storage(db_path) as storage:
        weather = storage.get_weather(location=location)
        energy = storage.get_energy(location=location)

        dims = {"location": location} if location else {}
        results = engine.compute_all(energy, weather, dims)
        storage.save_metrics(results)

    # Display metrics table
    table = Table(title="Energy Metrics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right", style="bold")
    table.add_column("Unit")

    for metric in results:
        table.add_row(metric.metric_name, f"{metric.value:,.2f}", metric.unit)

    console.print(table)


@app.command()
def run(
    location: str = typer.Option("new_york", help="City to analyze"),
    days: int = typer.Option(7, help="Days of historical data"),
    db_path: Path = typer.Option(Path("data/energypulse.duckdb"), help="Database path"),
) -> None:
    """Run the full pipeline: ingest → quality → metrics."""
    console.print("[bold magenta]Running full EnergyPulse pipeline[/bold magenta]\n")

    # Step 1: Ingest
    console.rule("[bold]Step 1: Data Ingestion[/bold]")
    ingest(location=location, days=days, db_path=db_path)
    console.print()

    # Step 2: Quality
    console.rule("[bold]Step 2: Quality Checks[/bold]")
    quality(db_path=db_path)
    console.print()

    # Step 3: Metrics
    console.rule("[bold]Step 3: Compute Metrics[/bold]")
    metrics(location=location, db_path=db_path)
    console.print()

    console.print("[bold green]Pipeline complete![/bold green]")
    console.print("Dashboard: [cyan]uv run streamlit run src/energypulse/dashboard/app.py[/cyan]")


@app.command()
def status(
    db_path: Path = typer.Option(Path("data/energypulse.duckdb"), help="Database path"),
) -> None:
    """Show pipeline status and data summary."""
    if not db_path.exists():
        console.print("[yellow]No database found. Run 'energypulse ingest' first.[/yellow]")
        return

    with Storage(db_path) as storage:
        # Data counts
        weather_count = len(storage.get_weather(limit=10000))
        energy_count = len(storage.get_energy(limit=10000))

        # Quality summary
        quality_summary = storage.get_quality_summary()

        # Latest metrics
        latest_metrics = storage.get_latest_metrics(limit=10)

    console.print("[bold]Pipeline Status[/bold]\n")

    console.print(f"Weather records: {weather_count:,}")
    console.print(f"Energy records:  {energy_count:,}")
    console.print()

    console.print("[bold]Quality Check Summary:[/bold]")
    for status_name, count in quality_summary.items():
        style = {"pass": "green", "warn": "yellow", "fail": "red"}.get(status_name, "white")
        console.print(f"  [{style}]{status_name.upper()}[/{style}]: {count}")
    console.print()

    if latest_metrics:
        console.print("[bold]Latest Metrics:[/bold]")
        for m in latest_metrics[:5]:
            console.print(f"  {m['metric_name']}: {m['value']:,.2f} {m['unit']}")


if __name__ == "__main__":
    app()
