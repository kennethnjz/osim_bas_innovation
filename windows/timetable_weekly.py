import datetime
import decimal
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import filedialog
import mimetypes
from pathlib import Path

import numpy as np
from sqlalchemy.types import *
import os, sys
sys.path.insert(0, 'windows/')
import timetable_stud
import timetable_fac
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def parse_days_of_week(s):
    if pd.isna(s):
        return []
    return [int(x) for x in s.split(';')]

def expand_root_jobs(df, date_range):
    expanded_jobs = []

    for idx, row in df.iterrows():
        if pd.isna(row['dependent_job_id']):  # root job
            valid_days = row['days_of_week_list']
            run_dates = [d for d in date_range if (not valid_days or d.isoweekday() in valid_days)]

            for run_date in run_dates:
                chain = [row.copy()]
                chain[0]['run_date'] = run_date
                expanded_jobs.append(chain[0])

                # Now follow the dependency chain
                current_job_id = row['job_id']
                while True:
                    child_jobs = df[df['dependent_job_id'] == current_job_id]
                    if child_jobs.empty:
                        break
                    child = child_jobs.iloc[0].copy()
                    child['run_date'] = None  # will be filled later
                    chain.append(child)
                    current_job_id = child['job_id']
                    expanded_jobs.append(child)

    expanded_df = pd.DataFrame(expanded_jobs).reset_index(drop=True)
    return expanded_df

def fill_schedule_24h(df):
    # Fill start_time and end_time based on dependencies
    while df['start_time'].isna().any() or df['end_time'].isna().any():
        changes = False
        for idx, row in df.iterrows():
            if pd.isna(row['start_time']):
                dep_job_id = row['dependent_job_id']
                if pd.notna(dep_job_id):
                    dep_row = df[df['job_id'] == dep_job_id]
                    if dep_row.empty:
                        continue
                    dep_end = dep_row['end_time'].values[0]
                    if pd.isna(dep_end):
                        continue
                    # Calculate start_time = dependent end_time + minutes_dependent_job_id
                    start = dep_end + row['minutes_dependent_job_id']
                    df.at[idx, 'start_time'] = start
                    df.at[idx, 'end_time'] = start + row['est_run_time']
                    changes = True
            else:
                if pd.isna(row['end_time']):
                    df.at[idx, 'end_time'] = row['start_time'] + row['est_run_time']
                    changes = True
        if not changes:
            break

    # Assign run_date for root jobs (no dependency) based on allowed weekdays
    for idx, row in df.iterrows():
        if pd.isna(row['run_date']) and pd.isna(row['dependent_job_id']):
            valid_days = row['days_of_week_list']
            if not valid_days:  # empty list or NaN
                df.at[idx, 'run_date'] = date_range[0]
            else:
                for d in date_range:
                    if d.isoweekday() in valid_days:  # Monday=1 ... Sunday=7
                        df.at[idx, 'run_date'] = d
                        break

    # Assign run_date for dependent jobs considering 24h rollover
    while df['run_date'].isna().any():
        changes = False
        for idx, row in df.iterrows():
            if pd.isna(row['run_date']):
                dep_job_id = row['dependent_job_id']
                if pd.notna(dep_job_id):
                    dep_row = df[df['job_id'] == dep_job_id]
                    if dep_row.empty or pd.isna(dep_row.iloc[0]['run_date']):
                        continue
                    dep_run_date = dep_row.iloc[0]['run_date']
                    dep_end_time = dep_row.iloc[0]['end_time']

                    # Calculate day offset for dependent job end_time and current job start_time
                    dep_day_offset = dep_end_time // 2400
                    dep_time_of_day = dep_end_time % 2400

                    start_day_offset = row['start_time'] // 2400
                    start_time_of_day = row['start_time'] % 2400

                    # If job starts before dependent job ends on the same run_date day, add an extra day
                    if (start_day_offset < dep_day_offset) or (start_day_offset == dep_day_offset and start_time_of_day < dep_time_of_day):
                        start_day_offset = dep_day_offset
                        if start_time_of_day < dep_time_of_day:
                            start_day_offset += 1

                    # Calculate run_date index after adding day offset
                    dep_date_idx = date_range.index(dep_run_date)
                    new_date_idx = dep_date_idx + start_day_offset
                    if new_date_idx >= len(date_range):
                        new_date_idx = len(date_range) - 1
                    run_date = date_range[int(new_date_idx)]

                    df.at[idx, 'run_date'] = run_date
                    changes = True
        if not changes:
            break

    # Normalize start_time and end_time to 24-hour format (0000 to 2359)
    def normalize_time(t):
        t = t % 2400
        # Fix times like 2360 or 2399 which are invalid minutes representation
        hour = t // 100
        minute = t % 100
        if minute >= 60:
            hour += 1
            minute -= 60
        return hour * 100 + minute

    for idx, row in df.iterrows():
        df.at[idx, 'start_time'] = normalize_time(row['start_time'])
        df.at[idx, 'end_time'] = normalize_time(row['end_time'])

    return df

def generate_weekly_timetable():
    conn = sqlite3.connect(r'files/timetable.db')
    df = pd.read_sql_query("SELECT * FROM OPERATING_SCHEDULE WHERE schedule_type = '2'", conn)

    if df.empty:
        messagebox.showwarning('No Data', 'No data found in OPERATING_SCHEDULE!')
        conn.close()
        return

    # Define the columns
    columns = ['job_id', 'run_date', 'series_id', 'start_time', 'dependent_job_id', 'end_time']

    # Create an empty DataFrame with the specified columns
    df_timetable = pd.DataFrame(columns=columns)
    df_timetable['job_id'] = df['job_id']
    df_timetable['series_id'] = df['series_id']
    df_timetable['dependent_job_id'] = df['dependent_job_id']
    df_timetable['start_time'] = df['start_time'].astype(float)
    df_timetable['est_run_time'] = df['est_run_time'].astype(float)
    df_timetable['minutes_dependent_job_id'] = df['minutes_dependent_job_id'].astype(float)
    df_timetable['days_of_week'] = df['days_of_week']
    df_timetable['end_time'] = df['end_time'].astype(float)
    df_timetable['days_of_week_list'] = df_timetable['days_of_week'].apply(parse_days_of_week)

    today = datetime.today().date()
    date_range = [today + timedelta(days=i) for i in range(14)]

    df_timetable['days_of_week_list'] = df_timetable['days_of_week'].fillna('').apply(lambda x: [int(d) for d in x.split(';') if d.strip().isdigit()])
    df_expanded = expand_root_jobs(df_timetable, date_range)
    df_filled = fill_schedule_24h(df_expanded)
    df_filled = df_filled[columns]

    df_filled.to_sql('TIMETABLE', conn, if_exists='append', index=False)
    conn.close()