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

import timetable_daily
import timetable_dependency
import timetable_weekly

sys.path.insert(0, 'windows/')
import timetable_stud
import timetable_fac
import sqlite3
import pandas as pd

import schedule_template_validation

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

def import_function():
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
    #     # Read Excel file into a pandas DataFrame
    #     col_names = [
    #         "srs_function",
    #         "series_id",
    #         "series_title",
    #         "job_id",
    #         "job_desc",
    #         "remarks",
    #         "start_run_date",
    #         "end_run_date",
    #         "run_mode",
    #         "est_trx_vol",
    #         "est_run_time",
    #         "priority_level",
    #         "server_name",
    #         "script",
    #         "os_option",
    #         "schedule_type",
    #         "month",
    #         "week_no",
    #         "day_no",
    #         "yearly_run_date",
    #         "days_of_week",
    #         "exclude_public_holidays",
    #         "start_time",
    #         "dependent_job_id",
    #         "minutes_dependent_job_id"
    #     ]
    #
    #     df_schema = {
    #         "srs_function": str,
    #         "series_id": str,
    #         "series_title": str,
    #         "job_id": str,
    #         "job_desc": str,
    #         "remarks": str,
    #         "start_run_date": int,
    #         "end_run_date": str,
    #         "run_mode": str,
    #         "est_trx_vol": int,
    #         "est_run_time": int,
    #         "priority_level": int,
    #         "server_name": str,
    #         "script": str,
    #         "os_option": int,
    #         "schedule_type": str,
    #         "month": str,
    #         "week_no": str,
    #         "day_no": str,
    #         "yearly_run_date": str,
    #         "days_of_week": str,
    #         "exclude_public_holidays": bool,
    #         "start_time": str,
    #         "dependent_job_id": str,
    #         "minutes_dependent_job_id": str
    #     }

        # if file_type == ".xlsx":
        #     df = pd.read_excel(filename,sheet_name="Template",names=col_names,dtype=df_schema)
        # elif file_type == ".csv":
        #     df = pd.read_csv(filename,names=col_names,dtype=df_schema, encoding='cp1252',skiprows=1)
        df = schedule_template_validation.load_template_sheet(file_type, filename)
        print(df)
        df['validation_errors'] = df.apply(schedule_template_validation.validate_row, axis=1)
        has_error = (df['validation_errors'].astype(str).str.strip() != '').any()
        if has_error:
            messagebox.showerror("Validation Error", "Error in Import, Download Error Report for Details!")
            save = messagebox.askyesno("Save Report", "Do you want to save the error report?")

            if save:
                report_file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])

            if report_file_path:
                try:
                    df.to_excel(report_file_path, index=False)
                    messagebox.showinfo("Saved", f"Report saved to:\n{report_file_path}")
                except Exception as e:
                    messagebox.showerror("Save Failed", f"Could not save file:\n{str(e)}")
        else:
            df = df.drop('validation_errors', axis=1)

# Connect to SQLite database
        conn = sqlite3.connect(r'files/timetable.db')

        # Load DataFrame into SQLite table (e.g., named 'excel_data')
        # 'if_exists='replace'' will overwrite the table if it already exists
        df.to_sql('OPERATING_SCHEDULE', conn, if_exists='replace', index=False)

        conn.commit()
        conn.close()
        print("Data successfully loaded from Excel to SQLite!")

        messagebox.showinfo('Schedule Import Successful', 'Schedule has been successfully imported')

    except Exception as e:
        messagebox.showerror('Import Failed', f'An error occurred: {e}')
        print(f"An error occurred: {e}")

def generate_timetable():
    conn = sqlite3.connect(r'files/timetable.db')

    cursor = conn.cursor()

    try:
        cursor.execute(f"DELETE FROM TIMETABLE")

        conn.commit()  # Commit the transaction
        print(f"Table TIMETABLE truncated successfully.")

        timetable_daily.generate_daily_timetable()
        timetable_weekly.generate_weekly_timetable()
        timetable_dependency.generate_dependency_timetable()

        messagebox.showinfo('Timetable Generation Successful', 'Timetable has been generated')
    except sqlite3.Error as e:
        print(f"Error truncating table: {e}")
        conn.rollback() # Rollback if an error occurs
    except Exception as e:
        messagebox.showerror('Timetable Generation Failed', f'An error occurred: {e}')
    finally:
        cursor.close()
        conn.close()

# Export function
def export_schedule():
    try:
        conn = sqlite3.connect(r'files/timetable.db')
        df = pd.read_sql_query('SELECT * FROM OPERATING_SCHEDULE', conn)
        conn.close()
        if df.empty:
            messagebox.showwarning('No Data', 'No data found in OPERATING_SCHEDULE!')
            return
        filetypes = [('Excel Files', '*.xlsx'), ('CSV Files', '*.csv')]
        export_file = filedialog.asksaveasfilename(defaultextension='.xlsx', filetypes=filetypes, title='Export As')
        if not export_file:
            return
        if export_file.endswith('.csv'):
            df.to_csv(export_file, index=False)
        else:
            df.to_excel(export_file, index=False)
        messagebox.showinfo('Export Successful', f'Data exported to {export_file}')
    except Exception as e:
        messagebox.showerror('Export Failed', f'An error occurred: {e}')

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

# Import button
tk.Button(
    m,
    text='Import Template',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=import_function
).pack(pady=10)

# Export button
tk.Button(
    m,
    text='Export Schedule',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=export_schedule
).pack(pady=10)

# Generate Timetable button
tk.Button(
    m,
    text='Generate Timetable',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=generate_timetable
).pack(pady=10)

m.mainloop()