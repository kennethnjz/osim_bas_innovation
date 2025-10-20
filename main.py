import shutil
from datetime import datetime
import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog
from pathlib import Path
import threading

import requests

import os, sys

# Handle imports for both development and executable modes
if getattr(sys, 'frozen', False):
    # Executable mode - add bundled windows directory to path
    sys.path.insert(0, os.path.join(sys._MEIPASS, 'windows'))
else:
    # Development mode
    sys.path.insert(0, 'windows/')

# UPDATED IMPORT:
import setup_start_files
database_path = setup_start_files.get_database_path()

import db_setup
import timetable_generation

import sqlite3
import pandas as pd

import bas_template_import
import schedule_template_validation
import populate_db_from_schedule
import public_holiday
import ganttchart

import subprocess


localhost_url = "http://127.0.0.1:8056/"

def convert_bas_template_function():
    filename = filedialog.askopenfilename(initialdir = "/",
                                          title = "Select the Import BAS Template",
                                          filetypes=[("Excel files", "*.xlsx")])

    file_type = Path(filename).suffix
    print("File Type: " + file_type)

    if not filename:
        return
    
    bas_template_import.import_bas_template(file_type, filename)

def import_function():
    filename = filedialog.askopenfilename(initialdir = "/",
                                          title = "Select the Import Template",
                                          filetypes = (("Excel Files",
                                                        "*.xlsx*"),
                                                       ("CSV Files",
                                                        "*.csv*")))

    # filepath.config(text="File Opened: " + filename)

    file_type = Path(filename).suffix
    print("File Type: " + file_type)

    if not filename:
        return

    try:
        df = schedule_template_validation.load_template_sheet(file_type, filename)
        # print(df)
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
                    return
                except Exception as e:
                    messagebox.showerror("Save Failed", f"Could not save file:\n{str(e)}")
        else:
            df = df.drop('validation_errors', axis=1)

        db_setup.create_database()

        # Connect to SQLite database
        conn = sqlite3.connect(database_path) # r'files/timetable.db'

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
    conn = sqlite3.connect(database_path) # r'files/timetable.db'

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
        conn = sqlite3.connect(database_path) # r'files/timetable.db'
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

browser_process = None

# Open new browser for calendar view
def open_calendar():
    global browser_process
    try:
        # Open calendar subprocess - no need to pass data since calendar handles its own DB queries
        if getattr(sys, 'frozen', False):
            # Executable mode - use bundled calendar_view.py
            calendar_script = os.path.join(sys._MEIPASS, 'windows', 'calendar_view.py')
        else:
            # Development mode
            calendar_script = "windows\\calendar_view.py"

        browser_process = subprocess.Popen(["python", calendar_script, database_path])
    except Exception as e:
        messagebox.showerror('Calendar Generation Failed', f'An error occurred: {e}')

    # try:
    #     # Load DB
    #     conn = sqlite3.connect(database_path) # r'files/timetable.db'
    #     # Define all columns you want to select and export
    #     db_cols = [
    #         "series_id",
    #         "job_id",
    #         "start_run_datetime",
    #         "end_run_datetime",
    #         "dependent_job_id",
    #     ]
    #     # Only select columns that exist in the DB
    #     query_cols = [col for col in db_cols]
    #     query = f"SELECT {', '.join(query_cols)} FROM TIMETABLE_DATETIME"
    #     df_query = pd.read_sql_query(query, conn)
    #     conn.close()
    #     if df_query.empty:
    #         messagebox.showwarning('No Data', 'No data found in TIMETABLE_DATETIME!')
    #         return
    #
    #     columns = [
    #         'start_date' ,
    #         'start_time' ,
    #         'end_date' ,
    #         'end_time' ,
    #         'event_name' ,
    #         'event_color' ,
    #         'event_context'
    #     ]
    #
    #     # Create an empty DataFrame with the specified columns
    #     df_timetable = pd.DataFrame(columns=columns)
    #
    #     # Loop to insert new row of dataframe to be passed to subprocess
    #     for idx, row in df_query.iterrows():
    #         start_date = datetime.strptime(row['start_run_datetime'], "%Y%m%d%H%M").date().strftime("%Y-%m-%d")
    #         end_date = datetime.strptime(row['end_run_datetime'], "%Y%m%d%H%M").date().strftime("%Y-%m-%d")
    #         start_time = datetime.strptime(row['start_run_datetime'], "%Y%m%d%H%M").time().strftime("%H:%M:00")
    #         end_time = datetime.strptime(row['end_run_datetime'], "%Y%m%d%H%M").time().strftime("%H:%M:00")
    #
    #         # print(start_date)
    #         # print(end_date)
    #
    #         new_row = {
    #             'start_date' : start_date,
    #             'start_time' : start_time,
    #             'end_date' : end_date,
    #             'end_time' : end_time,
    #             'event_name' : row["job_id"],
    #             'event_color' : "bg-gradient-secondary",
    #             'event_context' : f'''| Series ID | Job ID | Start Date & Time | End Date & Time | Dependent Job ID |
    #             | :------: | :------: | :------: | :------: | :------: |
    #             | {row["series_id"]} | {row["job_id"]} | {start_date} {start_time} | {end_date} {end_time} | {row["dependent_job_id"]} |'''
    #             # 'event_context' : f'<table class="table table-bordered" style="font-size: 1rem;"><tbody><tr><td><h6>Series ID</h6></td><td><h6>Job ID</h6></td><td><h6>Start Date &amp; Time</h6></td><td><h6>End Date &amp; Time</h6></td><td><h6>Dependent Job ID</h6></td></tr><tr><td>{row["series_id"]}</td><td>{row["job_id"]}</td><td>value3</td><td>value4</td><td>{row["dependent_job_id"]}</td></tr></tbody></table><p><br></p>'
    #         }
    #         df_timetable.loc[len(df_timetable)] = new_row
    #
    #     # print(df_timetable.to_json(date_format="ISO"))
    #
    #     # Test Dataframe for testing
    #     # df_test = pd.DataFrame({
    #     #     'start_date' : ["2025-09-08"],
    #     #     'start_time' : ["19:56:00"],
    #     #     'end_date' : ["2025-09-02"],
    #     #     'end_time' : ["21:56:00"],
    #     #     'event_name' : ["LISD056"],
    #     #     'event_color' : ["bg-gradient-primary"],
    #     #     'event_context' : ["Job Description"]
    #     # })
    #
    #     # Open calendar subprocess
    #     if getattr(sys, 'frozen', False):
    #         # Executable mode - use bundled calendar_view.py
    #         calendar_script = os.path.join(sys._MEIPASS, 'windows', 'calendar_view.py')
    #     else:
    #         # Development mode
    #         calendar_script = "windows\calendar_view.py"
    #
    #     subprocess.Popen(["python", calendar_script, df_timetable.to_json(date_format="ISO")])
    # except sqlite3.Error as e:
    #     print(f"Error querying table: {e}")
    #     if 'conn' in locals():
    #         conn.rollback() # Rollback if an error occurs
    # except Exception as e:
    #     messagebox.showerror('Calendar Generation Failed', f'An error occurred: {e}')
    # finally:
    #     conn.close()



 # Export OM
def export_om():
    try:
        conn = sqlite3.connect(database_path) # r'files/timetable.db'
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

m.geometry('980x350')
m.title('Welcome to OS Insight Manager (OSIM)')

tk.Label(
    m,
    text='OS Insight Manager (OSIM)',
    font=('Consolas', 20, 'bold'),
    wrap=400
).pack(pady=20, side='top')

#Define a callback function
def download_blank_template(template_type):
    filetypes = [('Excel Files', '*.xlsx')]
    # Define the source and destination paths
    source_path = "C:/Users/YourUser/Documents/MyExcelFile.xlsx"  # Replace with your actual source path
    destination_path = filedialog.asksaveasfilename(defaultextension='.xlsx', filetypes=filetypes, title='Download Template As')
    if not destination_path:
        return
    if getattr(sys, 'frozen', False):
        try:

            # Get database from bundled resources
            if template_type == "OM":
                source_path = os.path.join(sys._MEIPASS, "template", "OM_Template.xlsx")
            elif template_type == "PH":
                source_path = os.path.join(sys._MEIPASS, "template", "Public_Holiday_Template.xlsx")
            # Copy the file
            shutil.copy2(source_path, destination_path)

            # shutil.copy(source_path, destination_path)
            messagebox.showinfo('Download Successful', f'Template downloaded to {destination_path}')
        except Exception as e:
            messagebox.showerror('Download Failed', f'An error occurred: {e}')
    else:
        try:

            # DEVELOPMENT MODE: Use OM_template.xlsx directly
            script_dir = Path(__file__).parent

            # Define the source and destination paths
            if template_type == "OM":
                source_path = script_dir / "template" / "OM_Template.xlsx"  # Replace with your actual source path
            elif template_type == "PH":
                source_path = script_dir / "template" / "Public_Holiday_Template.xlsx"

            # Copy the file
            shutil.copy(source_path, destination_path)
            messagebox.showinfo('Download Successful', f'Template downloaded to {destination_path}')
        except Exception as e:
            messagebox.showerror('Download Failed', f'An error occurred: {e}')


#Create a Label to display the link
link = tk.Label(m, text="Download Blank OM Template",font=('Helveticabold', 10), fg="blue", cursor="hand2")
link.place(relx=0.8, rely=0.06)
link.bind("<Button-1>", lambda e:
download_blank_template("OM"))

link_ph = tk.Label(m, text="Download Blank PH Template",font=('Helveticabold', 10), fg="blue", cursor="hand2")
link_ph.place(relx=0.8, rely=0.125)
link_ph.bind("<Button-1>", lambda e:
download_blank_template("PH"))

om_frame = tk.LabelFrame(m, text="Operating Manual", padx=10, pady=10)
om_frame.pack(padx=10, pady=10, side='left', fill='both', expand=True)

# filepath = tk.Label(
#     m,
#     text='Import the template:',
#     font=('Consolas', 15)
# )
# filepath.pack(pady=10)
# Convert BAS Template button
tk.Button(
    om_frame,
    text='Convert BAS Template',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=convert_bas_template_function,
    width=20
).pack(pady=10)

# Import button
tk.Button(
    om_frame,
    text='Import Template',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=import_function,
    width=20
).pack(pady=10)

# Export button (full)
tk.Button(
    om_frame,
    text='Export Template',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=export_schedule,
    width=20
).pack(pady=10)

# Export button (OS)
tk.Button(
    om_frame,
    text='Export Operating Manual',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=export_om,
    width=20
).pack(pady=10)

# Generate Timetable button
timetable_frame = tk.LabelFrame(m, text="Timetable", padx=10, pady=10)
timetable_frame.pack(padx=10, pady=10, side='left', fill='both', expand=True)

# Import Public Holiday button
tk.Button(
    timetable_frame,
    text='Import Public Holiday',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=import_public_holiday_function,
    width=20
).pack(pady=10)

tk.Button(
    timetable_frame,
    text='Generate Timetable',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=generate_timetable,
    width=20
).pack(pady=10)

# Generate Visualization
visualization_frame = tk.LabelFrame(m, text="Data Visualization", padx=10, pady=10)
visualization_frame.pack(padx=10, pady=10, side='left', fill='both', expand=True)

# Generate Gantt Chart button
tk.Button(
    visualization_frame,
    text='View Gantt Chart',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=ganttchart.show_gantt_chart,
    width=20
).pack(pady=10)

# Generate Calendar button
tk.Button(
    visualization_frame,
    text='View Calendar',
    font=('Consolas', 12, 'bold'),
    padx=30,
    command=open_calendar,
    width=20
).pack(pady=10)

def on_closing():
    global browser_process
    # Try to shutdown calendar server gracefully
    try:
        requests.post('http://127.0.0.1:8056/shutdown', timeout=2)
    except:
        pass  # Ignore if server is already down

    if browser_process and browser_process.poll() is None:  # Check if the subprocess is still running
        print(f"Terminating subprocess with PID: {browser_process.pid}")
        browser_process.terminate()  # Sends SIGTERM (graceful termination request)
        try:
            browser_process.wait(timeout=5) # Wait for the process to terminate, with a timeout
        except subprocess.TimeoutExpired:
            print(f"Subprocess {browser_process.pid} did not terminate gracefully, attempting kill.")
            browser_process.kill()
            browser_process.wait()  # Wait for kill to complete
    m.quit() # Close the Tkinter window
    sys.exit(0)  # Ensure process exits

m.protocol("WM_DELETE_WINDOW", on_closing)

try:
    m.mainloop()
except KeyboardInterrupt:
    print("KeyboardInterrupt detected. Exiting Tkinter mainloop.")
    if browser_process and browser_process.poll() is None:
        browser_process.terminate()
        browser_process.wait()
    m.destroy()
    sys.exit(0)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    if browser_process and browser_process.poll() is None:
        browser_process.terminate()
        browser_process.wait()
    m.destroy()
    sys.exit(1)