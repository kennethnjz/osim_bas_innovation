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

def generate_daily_timetable():
    conn = sqlite3.connect(r'files/timetable.db')
    query_string = "SELECT * FROM OPERATING_SCHEDULE WHERE schedule_type = '1' AND CAST(strftime('%Y%m%d', 'now') AS int)  >= CAST(start_run_date AS int) AND CAST(strftime('%Y%m%d', 'now') AS int)  <= CAST(ifnull(nullif(end_run_date,''),'99999999') AS int)"
    df = pd.read_sql_query(query_string, conn)

    if df.empty:
        messagebox.showwarning('No Data', 'No daily schedule data found in OPERATING_SCHEDULE!')
        conn.close()
        return
    timetable_dict = df.to_dict('records')
    print(timetable_dict)

    for schedule_item in timetable_dict:
        print(f"\n---{schedule_item}---")
        print(f"\nseries_id: {schedule_item.get('series_id')} \njob_id: {schedule_item.get('job_id')} \nest_run_time: {schedule_item.get('est_run_time')} \nexclude_public_holidays: {schedule_item.get('exclude_public_holidays')} \nstart_time: {schedule_item.get('start_time')}")
        series_id = schedule_item.get('series_id')
        job_id = schedule_item.get('job_id')
        exclude_public_holidays = schedule_item.get('exclude_public_holidays')
        start_time = schedule_item.get('start_time')
        est_run_time = schedule_item.get('est_run_time')
        end_time =  ("0" + str(((int(start_time) // 100 + (int(est_run_time) // 60)) % 24) * 100 + int(est_run_time) % 60))[-4:]
        print(f"\nend_time: {end_time}")
        curr_date = datetime.date.today()
        timetable_df = pd.DataFrame({"series_id": [], "job_id": [], "run_date": [], "start_time": [], "end_time": [], "dependent_job_id": []})

        for day_no in range(14):
            inserted_date = curr_date + datetime.timedelta(day_no)
            new_df_record = {"series_id": series_id, "job_id": job_id, "run_date": inserted_date.strftime("%Y%m%d"), "start_time": start_time, "end_time": end_time, "dependent_job_id": ""}
            print(inserted_date.strftime("%Y%m%d"))
            #timetable_df = pd.concat([timetable_df, new_df_record], ignore_index=True)
            timetable_df.loc[len(timetable_df)] = new_df_record

        timetable_df.to_sql('TIMETABLE', conn, if_exists='append', index=False)

        print(timetable_df)
    conn.close()
