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

# def challenge():
#     conn = sqlite3.connect(r'files/timetable.db')
#
#     user = str(combo1.get())
#     if user == "Student":
#         cursor = conn.execute(f"SELECT PASSW, SECTION, NAME, ROLL FROM STUDENT WHERE SID='{id_entry.get()}'")
#         cursor = list(cursor)
#         if len(cursor) == 0:
#             messagebox.showwarning('Bad id', 'No such user found!')
#         elif passw_entry.get() != cursor[0][0]:
#             messagebox.showerror('Bad pass', 'Incorret Password!')
#         else:
#             nw = tk.Tk()
#             tk.Label(
#                 nw,
#                 text=f'{cursor[0][2]}\tSection: {cursor[0][1]}\tRoll No.: {cursor[0][3]}',
#                 font=('Consolas', 12, 'italic'),
#             ).pack()
#             m.destroy()
#             timetable_stud.student_tt_frame(nw, cursor[0][1])
#             nw.mainloop()
#
#     elif user == "Faculty":
#         cursor = conn.execute(f"SELECT PASSW, INI, NAME, EMAIL FROM FACULTY WHERE FID='{id_entry.get()}'")
#         cursor = list(cursor)
#         if len(cursor) == 0:
#             messagebox.showwarning('Bad id', 'No such user found!')
#         elif passw_entry.get() != cursor[0][0]:
#             messagebox.showerror('Bad pass', 'Incorret Password!')
#         else:
#             nw = tk.Tk()
#             tk.Label(
#                 nw,
#                 text=f'{cursor[0][2]} ({cursor[0][1]})\tEmail: {cursor[0][3]}',
#                 font=('Consolas', 12, 'italic'),
#             ).pack()
#             m.destroy()
#             timetable_fac.fac_tt_frame(nw, cursor[0][1])
#             nw.mainloop()
#
#     elif user == "Admin":
#         if id_entry.get() == 'admin' and passw_entry.get() == 'admin':
#             m.destroy()
#             os.system('py windows\\admin_screen.py')
#             # sys.exit()
#         else:
#             messagebox.showerror('Bad Input', 'Incorret Username/Password!')

def browse_files():
    filename = filedialog.askopenfilename(initialdir = "/",
                                          title = "Select the Import Template",
                                          filetypes = (("Excel Files",
                                                        "*.xlsx*"),
                                                       ("CSV Files",
                                                        "*.csv*")))

    filepath.config(text="File Opened: " + filename)

    file_type = Path(filename).suffix
    print("File Type: " + file_type)

    if not filename:
        return

    try:
        # Read Excel file into a pandas DataFrame
        col_names = [
            "srs_function",
            "series_id",
            "series_title",
            "job_id",
            "job_desc",
            "remarks",
            "start_run_date",
            "end_run_date",
            "run_mode",
            "est_trx_vol",
            "est_run_time",
            "priority_level",
            "server_name",
            "script",
            "os_option",
            "schedule_type",
            "month",
            "week_no",
            "day_no",
            "yearly_run_date",
            "days_of_week",
            "exclude_public_holidays",
            "start_time",
            "dependent_job_id",
            "minutes_dependent_job_id"
        ]

        df_schema = {
            "srs_function": str,
            "series_id": str,
            "series_title": str,
            "job_id": str,
            "job_desc": str,
            "remarks": str,
            "start_run_date": int,
            "end_run_date": str,
            "run_mode": str,
            "est_trx_vol": int,
            "est_run_time": int,
            "priority_level": int,
            "server_name": str,
            "script": str,
            "os_option": int,
            "schedule_type": str,
            "month": str,
            "week_no": str,
            "day_no": str,
            "yearly_run_date": str,
            "days_of_week": str,
            "exclude_public_holidays": bool,
            "start_time": str,
            "dependent_job_id": str,
            "minutes_dependent_job_id": str
        }

        if file_type == ".xlsx":
            df = pd.read_excel(filename,sheet_name="Template",names=col_names,dtype=df_schema)
        elif file_type == ".csv":
            df = pd.read_csv(filename,names=col_names,dtype=df_schema, encoding='cp1252',skiprows=1)
        print(df)

        # Connect to SQLite database
        conn = sqlite3.connect(r'files/timetable.db')

        # Load DataFrame into SQLite table (e.g., named 'excel_data')
        # 'if_exists='replace'' will overwrite the table if it already exists
        df.to_sql('OPERATING_SCHEDULE', conn, if_exists='replace', index=False)

        conn.commit()
        conn.close()
        print("Data successfully loaded from Excel to SQLite!")

    except Exception as e:
        print(f"An error occurred: {e}")

    #test

m = tk.Tk()

m.geometry('450x500')
m.title('Welcome')  

tk.Label(
    m,
    text='OS Intuitive Manager (OSIM)',
    font=('Consolas', 20, 'bold'),
    wrap=400
).pack(pady=20)

# tk.Label(
#     m,
#     text='Welcome!\nLogin to continue',
#     font=('Consolas', 12, 'italic')
# ).pack(pady=10)
#
# tk.Label(
#     m,
#     text='Username',
#     font=('Consolas', 15)
# ).pack()
#
# id_entry = tk.Entry(
#     m,
#     font=('Consolas', 12),
#     width=21
# )
# id_entry.pack()
#
# # Label5
# tk.Label(
#     m,
#     text='Password:',
#     font=('Consolas', 15)
# ).pack()

# toggles between show/hide password
# def show_passw():
#     if passw_entry['show'] == "●":
#         passw_entry['show'] = ""
#         B1_show['text'] = '●'
#         B1_show.update()
#     elif passw_entry['show'] == "":
#         passw_entry['show'] = "●"
#         B1_show['text'] = '○'
#         B1_show.update()
#     passw_entry.update()

# pass_entry_f = tk.Frame()
# pass_entry_f.pack()
# # Entry2
# passw_entry = tk.Entry(
#     pass_entry_f,
#     font=('Consolas', 12),
#     width=15,
#     show="●"
# )
# passw_entry.pack(side=tk.LEFT)
#
# B1_show = tk.Button(
#     pass_entry_f,
#     text='○',
#     font=('Consolas', 12, 'bold'),
#     command=show_passw,
#     padx=5
# )
# B1_show.pack(side=tk.LEFT, padx=15)

# combo1 = ttk.Combobox(
#     m,
#     values=['Student', 'Faculty', 'Admin']
# )
# combo1.pack(pady=15)
# combo1.current(0)
#
# tk.Button(
#     m,
#     text='Login',
#     font=('Consolas', 12, 'bold'),
#     padx=30,
#     command=challenge
# ).pack(pady=10)

filepath = tk.Label(
    m,
    text='Import the template:',
    font=('Consolas', 15)
)
filepath.pack(pady=10)

tk.Button(
    m,
    text='Import Template',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=browse_files
).pack(pady=10)

m.mainloop()