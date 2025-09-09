import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog
from pathlib import Path


import os, sys

import db_setup

import timetable_generation

sys.path.insert(0, 'windows/')

import sqlite3
import pandas as pd

import schedule_template_validation
import populate_db_from_schedule
import public_holiday
import ganttchart

import subprocess


localhost_url = "http://127.0.0.1:8056/"

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

        db_setup.create_database()

# Connect to SQLite database
        conn = sqlite3.connect(r'files/timetable.db')

        # Load DataFrame into SQLite table (e.g., named 'excel_data')
        # 'if_exists='replace'' will overwrite the table if it already exists
        df.to_sql('OPERATING_SCHEDULE', conn, if_exists='replace', index=False)

        conn.commit()
        conn.close()
        print("Data successfully loaded from Excel to SQLite!")

        # Populate other database tables from OPERATING_SCHEDULE
        populate_db_from_schedule.populate_data()

        messagebox.showinfo('Schedule Import Successful', 'Schedule has been successfully imported')

    except Exception as e:
        messagebox.showerror('Import Failed', f'An error occurred: {e}')
        print(f"An error occurred: {e}")

def generate_timetable():
    conn = sqlite3.connect(r'files/timetable.db')

    cursor = conn.cursor()

    try:
        cursor.execute(f"DELETE FROM TIMETABLE_DATETIME")

        conn.commit()  # Commit the transaction
        print(f"Table TIMETABLE truncated successfully.")

        #timetable_daily.generate_daily_timetable()
        timetable_generation.generate_timetable()
        #timetable_dependency.generate_dependency_timetable()

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
        query = f"SELECT {', '.join(col_names)} FROM OPERATING_SCHEDULE"
        df = pd.read_sql_query(query, conn)
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

def import_public_holiday_function():
    filename = filedialog.askopenfilename(initialdir = "/",
                                          title = "Select the Public Holiday File",
                                          filetypes = (("Excel Files",
                                                        "*.xlsx*"),
                                                       ("CSV Files",
                                                        "*.csv*")))

    if not filename:
        return

    file_type = Path(filename).suffix
    print("File Type: " + file_type)

    try:
        success, message = public_holiday.import_public_holiday(file_type, filename)

        if success:
            messagebox.showinfo('Public Holiday Import Successful', message)
        else:
            messagebox.showerror('Public Holiday Import Failed', message)

    except Exception as e:
        messagebox.showerror('Import Failed', f'An error occurred: {e}')
        print(f"An error occurred: {e}")

def open_calendar():
    subprocess.Popen(["python", "calendar_view.py"])
    #webbrowser.open(localhost_url)

 # Export OM
def export_om():
    try:
        conn = sqlite3.connect(r'files/timetable.db')
        # Define all columns you want to select and export
        db_cols = [
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
            "minutes_dependent_job_id",
            "job_frequency"
        ]
        # Only select columns that exist in the DB
        query_cols = [col for col in db_cols if col != "job_frequency"]
        query = f"SELECT {', '.join(query_cols)} FROM OPERATING_SCHEDULE"
        df = pd.read_sql_query(query, conn)
        conn.close()
        if df.empty:
            messagebox.showwarning('No Data', 'No data found in OPERATING_SCHEDULE!')
            return

        # Add job_frequency as blank if not present
        if "job_frequency" not in df.columns:
            df["job_frequency"] = ""

        # Add scheduling_instructions column
        def make_instruction(row):
            job_id = row.get('job_id', '')
            days_of_week_raw = row.get('days_of_week', '')
            start_time = row.get('start_time', '')
            dependentjob = row.get('dependent_job_id', '')
            try:
                mins = int(row.get('minutes_dependent_job_id', 0) or 0)
            except Exception:
                mins = 0
            # Handle exclude_public_holidays conversion
            excludepublicholidays = row.get('exclude_public_holidays', False)
            if isinstance(excludepublicholidays, str):
                excludepublicholidays = excludepublicholidays.lower() in ("true", "1", "yes")
            elif isinstance(excludepublicholidays, (int, float)):
                excludepublicholidays = bool(excludepublicholidays)

            # Convert days_of_week numbers to names and handle consecutive days
            day_map = {
                '1': 'Monday',
                '2': 'Tuesday',
                '3': 'Wednesday',
                '4': 'Thursday',
                '5': 'Friday',
                '6': 'Saturday',
                '7': 'Sunday'
            }
            day_order = ['1', '2', '3', '4', '5', '6', '7']
            # Support comma-separated or space-separated numbers
            if isinstance(days_of_week_raw, str):
                day_nums = [d.strip() for d in days_of_week_raw.replace(';', ',').replace(' ', ',').split(',') if d.strip()]
            elif isinstance(days_of_week_raw, (list, tuple)):
                day_nums = [str(d) for d in days_of_week_raw]
            else:
                day_nums = []
            # Sort and check for consecutive days
            day_indices = sorted([day_order.index(d) for d in day_nums if d in day_order])
            days_of_week = ''
            if day_indices:
                # Find consecutive ranges
                ranges = []
                start = prev = day_indices[0]
                for idx in day_indices[1:]:
                    if idx == prev + 1:
                        prev = idx
                    else:
                        ranges.append((start, prev))
                        start = prev = idx
                ranges.append((start, prev))
                # Format ranges
                day_names = []
                for s, e in ranges:
                    if s == e:
                        day_names.append(day_map[day_order[s]])
                    else:
                        day_names.append(f"{day_map[day_order[s]]} to {day_map[day_order[e]]}")
                days_of_week = ', '.join(day_names)

            # Construct instruction string
            if days_of_week and days_of_week.strip().lower() != 'None':
                instr = f"Job {job_id} runs {days_of_week}."
            else:
                instr = f"Job {job_id}."
            if start_time and str(start_time).strip().lower() != 'None':
                instr += f"\nRun at {start_time}."
            if dependentjob:
                if mins == 0:
                    instr += f"\nRun immediately after completion of {dependentjob}."
                else:
                    min_text = "min" if mins == 1 else "mins"
                    instr += f"\nRun {mins} {min_text} after completion of {dependentjob}."
            if excludepublicholidays:
                instr += "\nExclude public holidays."
            return instr
        df['scheduling_instructions'] = df.apply(make_instruction, axis=1)

        # Export columns
        export_cols = [
            "srs_function",
            "series_title",
            "job_frequency",
            "job_id",
            "run_mode",
            "est_run_time",
            "est_trx_vol",
            "scheduling_instructions",
            "priority_level",
            "server_name",
            "script",
            "start_run_date",
            "end_run_date",
            "remarks"
        ]

        # Only export columns that exist in the DataFrame
        export_cols = [col for col in export_cols if col in df.columns]

        filetypes = [('Excel Files', '*.xlsx'), ('CSV Files', '*.csv')]
        export_file = filedialog.asksaveasfilename(defaultextension='.xlsx', filetypes=filetypes, title='Export Operating Schedule As')
        if not export_file:
            return
        if export_file.endswith('.csv'):
            df.to_csv(export_file, index=False, columns=export_cols)
        else:
            df.to_excel(export_file, index=False, columns=export_cols)
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

# Export button (full)
tk.Button(
    m,
    text='Export Schedule',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=export_schedule
).pack(pady=10)

# Export button (OM)
tk.Button(
    m,
    text='Export OM',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=export_om
).pack(pady=10)

# Import Public Holiday button
tk.Button(
    m,
    text='Import Public Holiday',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=import_public_holiday_function
).pack(pady=10)

# Generate Timetable button
tk.Button(
    m,
    text='Generate Timetable',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=generate_timetable
).pack(pady=10)

# Generate Timetable button
tk.Button(
    m,
    text='Generate Gantt Chart',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=ganttchart.show_gantt_chart
).pack(pady=10)

# Open Calendar button
tk.Button(
    m,
    text='Open Calendar',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=open_calendar
).pack(pady=10)

try:
    m.mainloop()
except KeyboardInterrupt:
    print("KeyboardInterrupt detected. Exiting Tkinter mainloop.")
    m.quit()  # or root.destroy() depending on desired cleanup
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    m.destroy()