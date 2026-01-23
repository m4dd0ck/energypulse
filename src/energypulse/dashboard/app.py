"""Streamlit dashboard for EnergyPulse analytics.

Visualizes:
- Energy demand over time
- Temperature vs demand correlation
- Quality check status
- Key metrics summary
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from energypulse.storage import Storage

# Page config
st.set_page_config(
    page_title="EnergyPulse Dashboard",
    page_icon="⚡",
    layout="wide",
)

DB_PATH = Path("data/energypulse.duckdb")


@st.cache_resource
def get_storage() -> Storage:
    """Get cached database connection."""
    return Storage(DB_PATH)


def main() -> None:
    st.title("⚡ EnergyPulse Dashboard")
    st.markdown("Real-time weather and energy analytics")

    if not DB_PATH.exists():
        st.warning("No data found. Run `energypulse run` to populate the database.")
        st.code("uv run energypulse run --location new_york --days 7")
        return

    storage = get_storage()

    # Sidebar filters
    st.sidebar.header("Filters")
    locations = storage.execute_query("SELECT DISTINCT location FROM energy ORDER BY location")
    location_list = [row[0] for row in locations] if locations else ["new_york"]
    selected_location = str(st.sidebar.selectbox("Location", location_list))

    # Load data
    weather_df = load_weather_data(storage, selected_location)
    energy_df = load_energy_data(storage, selected_location)

    if energy_df.empty:
        st.warning(f"No data for {selected_location}")
        return

    # Key metrics row
    st.header("Key Metrics")
    display_key_metrics(energy_df)

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Energy Demand Over Time")
        fig_demand = create_demand_chart(energy_df)
        st.plotly_chart(fig_demand, use_container_width=True)

    with col2:
        st.subheader("Temperature vs Demand")
        fig_scatter = create_scatter_chart(energy_df)
        st.plotly_chart(fig_scatter, use_container_width=True)

    # Hourly patterns
    st.header("Demand Patterns")
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Average Demand by Hour")
        fig_hourly = create_hourly_chart(energy_df)
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col4:
        st.subheader("Weekday vs Weekend")
        fig_weekday = create_weekday_chart(energy_df)
        st.plotly_chart(fig_weekday, use_container_width=True)

    # Quality checks
    st.header("Data Quality")
    display_quality_checks(storage)

    # Raw data explorer
    with st.expander("Raw Data Explorer"):
        tab1, tab2 = st.tabs(["Energy Data", "Weather Data"])
        with tab1:
            st.dataframe(energy_df.head(100), use_container_width=True)
        with tab2:
            st.dataframe(weather_df.head(100), use_container_width=True)


def load_weather_data(storage: Storage, location: str) -> pd.DataFrame:
    """Load weather data into DataFrame."""
    query = f"""
        SELECT timestamp, temperature_c, humidity_pct, wind_speed_kmh,
               precipitation_mm, cloud_cover_pct, location
        FROM weather
        WHERE location = '{location}'
        ORDER BY timestamp
    """
    result = storage.execute_query(query)
    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(
        result,
        columns=["timestamp", "temperature_c", "humidity_pct", "wind_speed_kmh",
                 "precipitation_mm", "cloud_cover_pct", "location"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_energy_data(storage: Storage, location: str) -> pd.DataFrame:
    """Load energy data into DataFrame."""
    query = f"""
        SELECT timestamp, demand_mwh, temperature_c, is_weekend, hour_of_day, location
        FROM energy
        WHERE location = '{location}'
        ORDER BY timestamp
    """
    result = storage.execute_query(query)
    if not result:
        return pd.DataFrame()

    df = pd.DataFrame(
        result,
        columns=["timestamp", "demand_mwh", "temperature_c", "is_weekend", "hour_of_day", "location"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df["day_name"] = df["timestamp"].dt.day_name()
    return df


def display_key_metrics(df: pd.DataFrame) -> None:
    """Display key metrics in columns."""
    col1, col2, col3, col4 = st.columns(4)

    total_demand = df["demand_mwh"].sum()
    avg_demand = df["demand_mwh"].mean()
    peak_demand = df["demand_mwh"].max()
    peak_ratio = peak_demand / avg_demand if avg_demand > 0 else 0

    col1.metric("Total Demand", f"{total_demand:,.0f} MWh")
    col2.metric("Average Demand", f"{avg_demand:,.0f} MWh/hr")
    col3.metric("Peak Demand", f"{peak_demand:,.0f} MWh")
    col4.metric("Peak/Avg Ratio", f"{peak_ratio:.2f}x")


def create_demand_chart(df: pd.DataFrame) -> go.Figure:
    """Create time series chart of energy demand."""
    fig = px.line(
        df,
        x="timestamp",
        y="demand_mwh",
        color_discrete_sequence=["#1f77b4"],
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Demand (MWh)",
        showlegend=False,
        height=400,
    )
    return fig


def create_scatter_chart(df: pd.DataFrame) -> go.Figure:
    """Create scatter plot of temperature vs demand."""
    fig = px.scatter(
        df,
        x="temperature_c",
        y="demand_mwh",
        color="is_weekend",
        color_discrete_map={True: "#ff7f0e", False: "#1f77b4"},
        opacity=0.6,
        labels={"is_weekend": "Weekend"},
    )
    fig.update_layout(
        xaxis_title="Temperature (°C)",
        yaxis_title="Demand (MWh)",
        height=400,
    )
    return fig


def create_hourly_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart of average demand by hour."""
    hourly = df.groupby("hour_of_day")["demand_mwh"].mean().reset_index()

    fig = px.bar(
        hourly,
        x="hour_of_day",
        y="demand_mwh",
        color_discrete_sequence=["#2ca02c"],
    )
    fig.update_layout(
        xaxis_title="Hour of Day",
        yaxis_title="Average Demand (MWh)",
        showlegend=False,
        height=350,
    )
    return fig


def create_weekday_chart(df: pd.DataFrame) -> go.Figure:
    """Create box plot of demand by day of week."""
    # Order days properly
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    df["day_name"] = pd.Categorical(df["day_name"], categories=day_order, ordered=True)

    fig = px.box(
        df,
        x="day_name",
        y="demand_mwh",
        color="is_weekend",
        color_discrete_map={True: "#ff7f0e", False: "#1f77b4"},
    )
    fig.update_layout(
        xaxis_title="Day of Week",
        yaxis_title="Demand (MWh)",
        showlegend=False,
        height=350,
    )
    return fig


def display_quality_checks(storage: Storage) -> None:
    """Display quality check status."""
    query = """
        WITH recent AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY check_name ORDER BY checked_at DESC) as rn
            FROM quality_checks
        )
        SELECT check_name, status, message, checked_at
        FROM recent
        WHERE rn = 1
        ORDER BY check_name
    """
    results = storage.execute_query(query)

    if not results:
        st.info("No quality checks recorded yet. Run `energypulse quality` to check data quality.")
        return

    # Create columns for each status
    pass_count = sum(1 for r in results if r[1] == "pass")
    warn_count = sum(1 for r in results if r[1] == "warn")
    fail_count = sum(1 for r in results if r[1] == "fail")

    col1, col2, col3 = st.columns(3)
    col1.metric("✅ Passed", pass_count)
    col2.metric("⚠️ Warnings", warn_count)
    col3.metric("❌ Failed", fail_count)

    # Details table
    with st.expander("Check Details"):
        check_df = pd.DataFrame(results, columns=["Check", "Status", "Message", "Checked At"])

        def style_status(val: str) -> str:
            colors = {"pass": "background-color: #90EE90", "warn": "background-color: #FFD700",
                      "fail": "background-color: #FF6B6B"}
            return colors.get(val, "")

        styled = check_df.style.applymap(style_status, subset=["Status"])
        st.dataframe(styled, use_container_width=True)


if __name__ == "__main__":
    main()
