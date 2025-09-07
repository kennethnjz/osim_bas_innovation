import sqlite3
import pandas as pd
import plotly.express as px
import os

# Get directory of this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# DB is in ../files relative to this script
db_path = os.path.join(BASE_DIR, "..", "files", "timetable.db")

# Connect and fetch data
conn = sqlite3.connect(db_path)
query = """
SELECT series_id, job_id, start_run_datetime, end_run_datetime, dependent_job_id
FROM TIMETABLE_DATETIME
ORDER BY series_id, start_run_datetime
"""
df = pd.read_sql_query(query, conn)
conn.close()

# Convert datetime strings (YYYYMMDDHHMM) to Python datetime
df["start_run_datetime"] = pd.to_datetime(df["start_run_datetime"], format="%Y%m%d%H%M")
df["end_run_datetime"] = pd.to_datetime(df["end_run_datetime"], format="%Y%m%d%H%M")

# Combine series_id + job_id as unique task label
df["task"] = df["series_id"] + "-" + df["job_id"]

# Build Gantt chart
fig = px.timeline(
    df,
    x_start="start_run_datetime",
    x_end="end_run_datetime",
    y="task",
    color="series_id",
    hover_data=["dependent_job_id"],
    title="Gantt Chart from TIMETABLE_DATETIME"
)

fig.update_yaxes(autorange="reversed")  # So earliest tasks are at top
fig.show()
