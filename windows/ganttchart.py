import pandas as pd
import sqlite3
from datetime import timedelta
from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import os
import webbrowser
import threading

import setup_start_files

# ---------------------------
# Database Path
# ---------------------------
DB_PATH = "windows/files/timetable.db"

# ---------------------------
# Read Data
# ---------------------------
conn = sqlite3.connect(setup_start_files.get_database_path())
df = pd.read_sql("SELECT * FROM timetable_datetime", conn)
conn.close()

# ---------------------------
# Data Preparation
# ---------------------------

# Convert datetime strings (e.g. '202510181900') to actual datetime
df["start_run_datetime"] = pd.to_datetime(df["start_run_datetime"], format="%Y%m%d%H%M")
df["end_run_datetime"] = pd.to_datetime(df["end_run_datetime"], format="%Y%m%d%H%M")

# Extract run_date (day of each series run)
df["run_date"] = df["start_run_datetime"].dt.date

# ---------------------------
# Build Series-Day summary (for zoom-out)
# ---------------------------
summary = (
    df.groupby(["series_id", "run_date"])
    .agg(
        start_run_datetime=("start_run_datetime", "min"),
        end_run_datetime=("end_run_datetime", "max"),
    )
    .reset_index()
)

# Create a unique series-day label
summary["series_day"] = summary["series_id"] + "_" + summary["run_date"].astype(str)

# ---------------------------
# Dash App
# ---------------------------

app = Dash(__name__)
app.title = "Timetable Gantt Viewer"

app.layout = html.Div(
    [
        html.H2("Weekly Gantt View (Zoom Out)"),
        html.P("Click on a bar below to zoom in to that series-day view."),

        dcc.Graph(id="weekly_gantt"),

        html.Hr(),
        html.H3("Zoom-In Job Timeline"),
        dcc.Graph(id="job_gantt"),

    ],
    style={"maxWidth": "90%", "margin": "auto"}
)


# ---------------------------
# Weekly (Zoom-Out) Gantt Chart
# ---------------------------
@app.callback(
    Output("weekly_gantt", "figure"),
    Input("weekly_gantt", "id")  # dummy trigger to render once
)
def render_weekly(_):
    fig = px.timeline(
        summary,
        x_start="start_run_datetime",
        x_end="end_run_datetime",
        y="series_id",
        color="series_day",
        hover_data=["series_day"],
        title="Series Overview by Day",
    )

    fig.update_yaxes(autorange="reversed")  # Gantt-style (top-down)
    fig.update_layout(
        height=500,
        xaxis_title="Date",
        yaxis_title="Series ID",
        legend_title="Run Date",
    )
    return fig


# ---------------------------
# Zoom-In Job View (based on bar click)
# ---------------------------
@app.callback(
    Output("job_gantt", "figure"),
    Input("weekly_gantt", "clickData"),
)
def zoom_in(clickData):
    if not clickData:
        # Default message
        return px.scatter(title="Click on a bar above to zoom in to job-level view.")

    # Extract the clicked point
    point = clickData["points"][0]
    series_id = point["y"]
    run_date = point["customdata"][0].split("_")[-1] if "customdata" in point else None

    # Fallback: parse date from hover or x start
    if not run_date:
        start_str = point.get("x")
        if start_str:
            run_date = str(pd.to_datetime(start_str).date())

    # Filter original dataframe
    df_filtered = df[
        (df["series_id"] == series_id) &
        (df["run_date"].astype(str) == run_date)
    ].sort_values("start_run_datetime")

    if df_filtered.empty:
        return px.scatter(title=f"No data for {series_id} on {run_date}")

    fig = px.timeline(
        df_filtered,
        x_start="start_run_datetime",
        x_end="end_run_datetime",
        y="job_id",
        color="job_id",
        title=f"Job Timeline: {series_id} on {run_date}",
    )

    fig.update_yaxes(autorange="reversed")
    fig.update_layout(
        height=500,
        xaxis_title="Time of Day",
        yaxis_title="Job ID",
        showlegend=False,
    )

    return fig

# ---------------------------
# Run Dash
# ---------------------------
if __name__ == "__main__":
    def open_browser():
        webbrowser.open_new("http://127.0.0.1:8050")
    threading.Timer(1, open_browser).start()
    app.run(debug=True, use_reloader=False)

# Optional callable
def show_gantt_chart():
    threading.Timer(1, lambda: webbrowser.open_new("http://127.0.0.1:8050")).start()
    app.run(debug=False, use_reloader=False)