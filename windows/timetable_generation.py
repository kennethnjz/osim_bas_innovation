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
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import setup_start_files

import timetable_exclude_ph

def parse_days_of_week(s):
    if pd.isna(s):
        return []
    return [int(x) for x in s.split(';')]

def initiateTimetableDs(df, today_str, future_str):
    # Define the columns
    columns = [
        'job_id', 'start_run_datetime', 'series_id', 'start_time', 'dependent_job_id',
        'end_run_datetime', 'start_run_date', 'end_run_date', 'est_run_time',
        'minutes_dependent_job_id', 'days_of_week_list', 'root_job'
    ]

    # Create an empty DataFrame with the specified columns
    df_timetable = pd.DataFrame(columns=columns)

    #expand dependency job
    for idx, row in df.iterrows():
        depJobIds = []
        if not pd.isna(row['dependent_job_id']):
            depJobIds = [job.strip() for job in row['dependent_job_id'].split(',') if job.strip() != '']

        # root job
        if not depJobIds:
            new_row = {
                'job_id': row['job_id'],
                'series_id': row['series_id'],
                'start_run_datetime': None,
                'end_run_datetime': None,
                'start_time': row['start_time'],
                'dependent_job_id': row['job_id'],
                'start_run_date': row['start_run_date'],
                'end_run_date': row['end_run_date'] if pd.notna(row['end_run_date']) else '99999999',
                'est_run_time': row['est_run_time'],
                'minutes_dependent_job_id': row['minutes_dependent_job_id'],
                'days_of_week_list': parse_days_of_week(row['days_of_week']),
                'root_job': 1
            }
            df_timetable.loc[len(df_timetable)] = new_row
        else:
            for dep_id in depJobIds:
                new_row = {
                    'job_id': row['job_id'],
                    'series_id': row['series_id'],
                    'start_run_datetime': None,
                    'end_run_datetime': None,
                    'start_time': None,
                    'dependent_job_id': dep_id,
                    'start_run_date': row['start_run_date'],
                    'end_run_date': row['end_run_date'],
                    'est_run_time': row['est_run_time'],
                    'minutes_dependent_job_id': row['minutes_dependent_job_id'],
                    'days_of_week_list': None,
                    'root_job': 0
                }
                df_timetable.loc[len(df_timetable)] = new_row

    root_row = None

    # Keep only rows where dependent_job_id is either:
    # - NaN (no dependency), or
    # - Present in job_id column
    filtered_df = df_timetable[
        df_timetable['dependent_job_id'].isna() |  # Keep rows where dependent_job_id is NaN
        df_timetable['dependent_job_id'].isin(df_timetable['job_id'])  # OR is in job_id list
        ]

    df_root_jobs = filtered_df[filtered_df['root_job'] == 1]

    for idx, row in df_root_jobs.iterrows():
        if int(row['start_run_date']) <= int(future_str) and int(row['end_run_date']) >= int(today_str):
            if int(row['start_run_date']) <= int(today_str):
                df_root_jobs.at[idx,'start_run_datetime'] = today_str + row['start_time']
            elif int(row['start_run_date']) > int(today_str):
                df_root_jobs.at[idx,'start_run_datetime'] = row['start_run_date'] + row['start_time']

            # Parse start_run_datetime, assuming format YYYYMMDDHHMM
            start_dt = datetime.strptime(df_root_jobs.loc[idx,'start_run_datetime'], "%Y%m%d%H%M")

            delta = timedelta(minutes=int(row['est_run_time']))

            # Add est_run_time to start_dt
            end_dt = start_dt + delta

            # Format back if needed (same format as start)
            df_root_jobs.at[idx,'end_run_datetime'] = end_dt.strftime("%Y%m%d%H%M")

    df_non_root_jobs = filtered_df[filtered_df['root_job'] == 0]

    # Loop until all non-root jobs are processed
    while not df_non_root_jobs.empty:
        # Track if any job was processed in this pass
        processed_any = False
        indices_to_drop = []

        for idx, row in df_non_root_jobs.iterrows():
            # Check if dependency is in root jobs
            match = df_root_jobs[df_root_jobs['job_id'] == row['dependent_job_id']]

            if not match.empty:
                # We can now process this job
                root_row = match.iloc[0]

                df_non_root_jobs.at[idx, 'days_of_week_list'] = root_row['days_of_week_list']

                # Set start_run_datetime from dependent job's end_run_datetime
                df_non_root_jobs.at[idx, 'start_run_datetime'] = root_row['end_run_datetime']

                # Parse date
                start_dt = datetime.strptime(root_row['end_run_datetime'], "%Y%m%d%H%M")

                # Add durations
                delta1 = timedelta(minutes=int(row['est_run_time']))
                delta2 = timedelta(minutes=int(row['minutes_dependent_job_id']))
                end_dt = start_dt + delta1 + delta2

                # Update in df_non_root_jobs
                df_non_root_jobs.at[idx, 'end_run_datetime'] = end_dt.strftime("%Y%m%d%H%M")

                # Append to df_root_jobs
                df_root_jobs = pd.concat([df_root_jobs, df_non_root_jobs.loc[[idx]]], ignore_index=True)

                indices_to_drop.append(idx)

                processed_any = True
                break  # break to restart the loop with the updated root jobs

        if indices_to_drop:
            df_non_root_jobs = df_non_root_jobs.drop(indices_to_drop)

        if not processed_any:
            raise Exception("Unresolvable dependencies detected — some jobs refer to missing or circular dependencies.")

    # Replace matching dependent_job_id with empty string
    df_root_jobs['dependent_job_id'] = df_root_jobs.apply(
        lambda row: '' if row['job_id'] == row['dependent_job_id'] else row['dependent_job_id'],
        axis=1
    )

    agg_df = df_root_jobs.groupby('job_id').agg({
        'dependent_job_id': lambda x: ','.join(sorted(set(filter(None, x)))),
        'start_run_datetime': 'max',
        'end_run_datetime': 'max'
    }).reset_index()

    # Drop duplicates to get one row per job_id (you can choose how)
    base_df = df_root_jobs.drop_duplicates(subset=['job_id'])

    # Merge aggregated data back to base
    result = pd.merge(base_df, agg_df, on='job_id', suffixes=('', '_agg'))

    # Replace original columns with aggregated ones
    result['dependent_job_id'] = result['dependent_job_id_agg']
    result['start_run_datetime'] = result['start_run_datetime_agg']
    result['end_run_datetime'] = result['end_run_datetime_agg']
    result = result.drop(columns=['dependent_job_id_agg', 'start_run_datetime_agg', 'end_run_datetime_agg'])

    return result

def expand_schedule(df_timetable, today_str, future_str):

    df_expanded_timetable = pd.DataFrame()
    unique_series_ids = df_timetable['series_id'].unique().tolist()

    for id in unique_series_ids:
        filtered_df = df_timetable[df_timetable['series_id'] == id]
        filtered_df = filtered_df.sort_values(by='root_job', ascending=False).reset_index(drop=True)

        startDate = int(filtered_df.loc[0]['start_run_datetime'][:8])
        endDate = int(filtered_df.loc[0]['end_run_datetime'][:8])

        startdate_obj = datetime.strptime(filtered_df.loc[0]['start_run_datetime'][:8], "%Y%m%d").date()
        enddate_obj = datetime.strptime(filtered_df.loc[0]['end_run_datetime'][:8], "%Y%m%d").date()

        counter = 0
        expanded_rows = []

        while startDate < int(future_str):


            for idx, row in filtered_df.iterrows():

                row_startdate = datetime.strptime(row['start_run_datetime'][:8], "%Y%m%d").date()
                row_enddate = datetime.strptime(row['end_run_datetime'][:8], "%Y%m%d").date()
                row_startdate = row_startdate + timedelta(days=counter)
                row_enddate = row_enddate + timedelta(days=counter)

                if idx==0:
                    day_of_week = row_startdate.isoweekday()

                new_row = row.to_dict()

                new_row['start_run_datetime'] =  row_startdate.strftime("%Y%m%d") + row['start_run_datetime'][8:]
                new_row['end_run_datetime'] =  row_enddate.strftime("%Y%m%d") + row['end_run_datetime'][8:]

                days_str = row['days_of_week_list']

                if not row['days_of_week_list']:
                    expanded_rows.append(new_row)
                else:
                    if day_of_week in row['days_of_week_list']:
                        expanded_rows.append(new_row)

            counter += 1

            startdate_obj = startdate_obj + timedelta(days=1)
            enddate_obj = enddate_obj + timedelta(days=1)

            startDate = int(startdate_obj.strftime("%Y%m%d"))

        df_expanded_timetable = pd.concat([df_expanded_timetable, pd.DataFrame(expanded_rows)], ignore_index=True)
    return df_expanded_timetable

def generate_timetable():
    numberOfDays = 14
    today = datetime.today().date()
    today_str = today.strftime('%Y%m%d')
    future_date = today + timedelta(days=numberOfDays)
    future_str = future_date.strftime('%Y%m%d')

    conn = sqlite3.connect(setup_start_files.get_database_path()) # r'files/timetable.db'
    df = pd.read_sql_query("SELECT * FROM OPERATING_SCHEDULE", conn)

    if df.empty:
        messagebox.showwarning('No Data', 'No data found in OPERATING_SCHEDULE!')
        conn.close()
        return

    df_timetable = initiateTimetableDs(df, today_str, future_str)
    df_timetable = expand_schedule(df_timetable, today_str, future_str)

    columns_to_keep = [
        'series_id',
        'job_id',
        'start_run_datetime',
        'end_run_datetime',
        'dependent_job_id'
    ]
    df_timetable = df_timetable[columns_to_keep]
    df_timetable = df_timetable.sort_values(by=['series_id','start_run_datetime'], ascending=[True, True]).reset_index(drop=True)
    df_timetable.to_excel('output.xlsx', index=False)
    df_timetable.to_sql('TIMETABLE_DATETIME', conn, if_exists='append', index=False)

    # Apply public holiday exclusions with the same numberOfDays
    try:
        print("\n" + "="*60)
        print("APPLYING PUBLIC HOLIDAY EXCLUSIONS")
        print("="*60)
        timetable_exclude_ph.exclude_public_holidays(numberOfDays)
        print("Public holiday exclusions completed successfully")
    except Exception as e:
        print(f"Error during public holiday exclusion: {e}")
        # Don't re-raise here as the main timetable generation was successful
