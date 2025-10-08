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
# Database Path (relative)
# ---------------------------
db_path = os.path.join("windows", "files", "timetable.db")

# ---------------------------
# Read Data
# ---------------------------
conn = sqlite3.connect(setup_start_files.get_database_path())
df = pd.read_sql_query("SELECT * FROM OPERATING_SCHEDULE", conn)
conn.close()

# ---------------------------
# Preprocess
# ---------------------------

# --- Convert and clean time columns ---
df['start_run_date'] = pd.to_datetime(df['start_run_date'], errors='coerce')
df['end_run_date'] = pd.to_datetime(df['end_run_date'], errors='coerce')
df['minutes_dependent_job_id'] = pd.to_numeric(df['minutes_dependent_job_id'], errors='coerce').fillna(0)

# --- Helper function to compute times recursively ---
def resolve_job_time(job_id, visited=None):
    if visited is None:
        visited = set()

    # Prevent circular dependencies
    if job_id in visited:
        return
    visited.add(job_id)

    job = df.loc[df['job_id'] == job_id]
    if job.empty:
        return

    idx = job.index[0]
    start = job.at[idx, 'start_run_date']
    end = job.at[idx, 'end_run_date']

    # If this job already has start and end, nothing to do
    if pd.notna(start) and pd.notna(end):
        return

    dep_job_id = job.at[idx, 'dependent_job_id']
    offset_min = job.at[idx, 'minutes_dependent_job_id']

    # If job depends on another job
    if dep_job_id:
        resolve_job_time(dep_job_id, visited)  # resolve dependency first
        dep_job = df.loc[df['job_id'] == dep_job_id]
        if not dep_job.empty and pd.notna(dep_job.iloc[0]['end_run_date']):
            dep_end = dep_job.iloc[0]['end_run_date']
            start_time = dep_end + timedelta(minutes=offset_min)
            end_time = start_time + timedelta(minutes=1)  # assume 1 min duration if not defined
            df.at[idx, 'start_run_date'] = start_time
            df.at[idx, 'end_run_date'] = end_time

    # If still missing end but has start, estimate short run
    elif pd.notna(start) and pd.isna(end):
        df.at[idx, 'end_run_date'] = start + timedelta(minutes=1)

# --- Apply dependency resolution to all jobs ---
for job_id in df['job_id']:
    resolve_job_time(job_id)

series_df = df.groupby('series_id').agg(
    start_run_date=('start_run_date', 'min'),
    end_run_date=('end_run_date', 'max')
).reset_index()


# ---------------------------
# Dash App Layout
# ---------------------------
app = Dash(__name__)

app.layout = html.Div([
    html.H2("Operating Schedule Gantt Chart"),
    html.Label("Select View:"),
    dcc.RadioItems(
        id='view-toggle',
        options=[
            {'label': 'Zoom Out (Series View)', 'value': 'series'},
            {'label': 'Zoom In (Job View)', 'value': 'job'}
        ],
        value='series',
        inline=True
    ),
    html.Label("Select Series:"),
    dcc.Dropdown(
        id='series-dropdown',
        options=[{'label': s, 'value': s} for s in sorted(df['series_id'].dropna().unique())],
        value='LISSR001',
        clearable=False
    ),
    dcc.Graph(id='gantt-chart')
])

# ---------------------------
# Callbacks
# ---------------------------
@app.callback(
    Output('gantt-chart', 'figure'),
    Input('view-toggle', 'value'),
    Input('series-dropdown', 'value')
)
def update_chart(view, series_id):
    if view == 'series':
        # ✅ Show all series, sorted by start date
        filtered = series_df.sort_values("start_run_date")

        fig = px.timeline(
            filtered,
            x_start="start_run_date",
            x_end="end_run_date",
            y="series_id",
            color="series_id",
            title="Zoomed Out: All Series Overview",
            hover_data={"start_run_date": True, "end_run_date": True}
        )

        # ✅ Reverse y-axis so earliest series is on top
        fig.update_yaxes(autorange="reversed")

        # ✅ Focus x-axis range around data (avoid huge empty gaps)
        min_date = filtered["start_run_date"].min()
        max_date = filtered["end_run_date"].max()
        date_padding = (max_date - min_date) * 0.05  # 5% padding
        fig.update_xaxes(range=[min_date - date_padding, max_date + date_padding])

        # ✅ Make bars thicker and more visible
        fig.update_traces(marker_line_width=1.2)
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Series ID",
            height=700,
            bargap=0.4,
            template="plotly_white",
            title_x=0.5
        )

    else:
        # Zoom In → show individual jobs for selected series
        filtered = df[df['series_id'] == series_id]
        fig = px.timeline(
            filtered,
            x_start="start_run_date",
            x_end="end_run_date",
            y="job_id",
            color="job_id",
            title=f"Zoomed In: Jobs in {series_id}",
            hover_data=["job_desc", "dependent_job_id", "minutes_dependent_job_id"]
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Job ID",
            height=700,
            template="plotly_white",
            title_x=0.5
        )

    return fig

if __name__ == "__main__":
    import webbrowser
    import threading

    # Define a function to open browser after a small delay
    def open_browser():
        webbrowser.open_new("http://127.0.0.1:8050")

    # Run the browser open in a background thread (so it doesn’t block)
    threading.Timer(1.0, open_browser).start()

    # Start the Dash server (Dash 3+ syntax)
    app.run(debug=True)


# 🧩 Wrap server start in a callable function
def show_gantt_chart():
    """Launch the Dash Gantt Chart in a web browser."""
    def open_browser():
        webbrowser.open_new("http://127.0.0.1:8050")

    # Launch browser shortly after Dash starts
    threading.Timer(1, open_browser).start()

    # Run Dash (in blocking mode)
    app.run(debug=False, use_reloader=False)