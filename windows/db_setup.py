import sqlite3
import setup_start_files

def create_database():
    # # Ensure the files directory exists
    # if not os.path.exists('files'):
    #     os.makedirs('files')

    # Connect to database (creates it if it doesn't exist)
    conn = sqlite3.connect(setup_start_files.get_database_path()) # r'files/timetable.db'
    cursor = conn.cursor()

    # Create tables
    try:
        # Operating Schedule table
        # columns missing :
        # srs_title TEXT,
        # no_error TEXT,
        # run_if_scheduled TEXT,
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS OPERATING_SCHEDULE (
            srs_function TEXT,
            series_id TEXT,
            series_title TEXT,
            job_id TEXT,
            job_desc TEXT,
            remarks TEXT,
            start_run_date TEXT,
            end_run_date TEXT,
            run_mode TEXT,
            est_trx_vol TEXT,
            est_run_time TEXT,
            priority_level TEXT,
            server_name TEXT,
            script TEXT,
            os_option TEXT,
            schedule_type TEXT,
            month TEXT,
            week_no TEXT,
            day_no TEXT,
            yearly_run_date TEXT,
            days_of_week TEXT,
            exclude_public_holidays TEXT,
            start_time TEXT,
            dependent_job_id TEXT,
            minutes_dependent_job_id TEXT
        )
        ''')

        # Timetable table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS TIMETABLE (
            series_id TEXT,
            job_id TEXT,
            run_date TEXT,
            start_time TEXT,
            end_time TEXT,
            dependent_job_id TEXT,
			PRIMARY KEY (series_id, job_id, run_date, start_time)
        )
        ''')

        # Timetable table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS TIMETABLE_DATETIME (
            series_id TEXT,
            job_id TEXT,
            start_run_datetime TEXT,
            end_run_datetime TEXT,
            dependent_job_id TEXT,
			PRIMARY KEY (series_id, job_id, start_run_datetime)
        )
        ''')

        # Job table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS JOB (
            system_code TEXT,
            job_id TEXT PRIMARY KEY,
            series_id TEXT,
            job_sequence TEXT,
            runchart_id TEXT,
            run_mode TEXT,
            est_run_time TEXT,
            est_volume TEXT,
            job_description TEXT,
            priority TEXT,
            server_id TEXT,
            script_id TEXT,
            args TEXT,
            first_run_date TEXT,
            suspended_date TEXT,
            last_run_date TEXT,
            exclude_ph TEXT,
            remarks TEXT
        )
        ''')

        # Job SRS Mapping table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS JOB_SRS_MAPPING (
            system_code TEXT,
            job_id TEXT,
            srs_id TEXT,
            srs_function_no TEXT,
            srs_version_number TEXT,
            PRIMARY KEY (job_id, srs_id, srs_function_no)
        )
        ''')

        # SRS table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS SRS (
            srs_id TEXT PRIMARY KEY,
            srs_title TEXT,
            srs_filename TEXT
        )
        ''')

        # SRS Function table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS SRS_FUNCTION (
            system_code TEXT,
            srs_function_no TEXT,
            srs_function_title TEXT,
            PRIMARY KEY (system_code, srs_function_no)
        )
        ''')

        # Run Series table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS RUN_SERIES (
            system_code TEXT,
            series_id TEXT,
            frequency TEXT,
            series_sequence_1 TEXT,
            series_sequence_2 TEXT,
            series_title TEXT,
            PRIMARY KEY (system_code, series_id)
        )
        ''')

        # Runchart table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS RUNCHART (
            runchart_id TEXT PRIMARY KEY,
            runchart_filename TEXT
        )
        ''')

        # Server Name table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS SERVER_NAME (
            server_id TEXT PRIMARY KEY,
            server_type TEXT,
            server_name TEXT
        )
        ''')

        # Script table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS SCRIPT (
            script_id TEXT PRIMARY KEY,
            script TEXT
        )
        ''')

        # Job Dependency table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS JOB_DEPENDENCY (
            job_id TEXT,
            job_id_parent TEXT,
            timetable_id TEXT,
            run_if_scheduled TEXT,
            PRIMARY KEY (job_id, job_id_parent, timetable_id)
        )
        ''')

        # Normal table
        # ePT / no error list
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS NORMAL (
            job_id TEXT,
            stepname TEXT,
            rc TEXT,
            PRIMARY KEY (job_id, stepname, rc)
        )
        ''')

        # Additional Scheduling Instruction table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS SCHEDULING_INSTRUCTION_ADD (
            job_id TEXT,
            add_instruction_id TEXT,
            add_instruction TEXT,
            PRIMARY KEY (job_id, add_instruction_id)
        )
        ''')

        # Daily Timetable table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS TIMETABLE_DAILY (
            job_id TEXT,
            series_id TEXT,
            timetable_id TEXT,
            run_time TEXT,
			run_option TEXT,
            days_of_week TEXT,
            PRIMARY KEY (job_id, timetable_id)
        )
        ''')

        # Weekly Timetable table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS TIMETABLE_WEEKLY (
            job_id TEXT,
            series_id TEXT,
            timetable_id TEXT,
            run_time TEXT,
			run_option TEXT,
            days_of_week TEXT,
            PRIMARY KEY (job_id, timetable_id)
        )
        ''')

        # Monthly Timetable table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS TIMETABLE_MONTHLY (
            job_id TEXT,
            series_id TEXT,
            timetable_id TEXT,
            run_time TEXT,
			run_option TEXT,
            day_of_month TEXT,
            month TEXT,
            PRIMARY KEY (job_id, timetable_id)
        )
        ''')

        # Public Holiday table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS PUBLIC_HOLIDAY (
            year TEXT,
            ph_date TEXT,
            PRIMARY KEY (year, ph_date)
        )
        ''')

        # Amendment Log table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS AMENDMENT_LOG (
            log_id TEXT PRIMARY KEY,
            system_code TEXT,
            om_version TEXT,
            changed_date TEXT,
            project_id TEXT,
            itpm TEXT,
            change_history TEXT,
            status TEXT
        )
        ''')

        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_system_code ON JOB(system_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_series_id ON JOB(series_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_job_sequence ON JOB(job_sequence)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_runchart_id ON JOB(runchart_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_server_id ON JOB(server_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_script_id ON JOB(script_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_exclude_ph ON JOB(exclude_ph)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_JOB_SRS_MAPPING_system_code ON JOB_SRS_MAPPING(system_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_RUN_SERIES_system_code_frequency ON RUN_SERIES(system_code, frequency)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_TIMETABLE_DAILY_series_id ON TIMETABLE_DAILY(series_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_TIMETABLE_WEEKLY_series_id ON TIMETABLE_WEEKLY(series_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_TIMETABLE_MONTHLY_series_id ON TIMETABLE_MONTHLY(series_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_AMENDMENT_LOG_system_om_ver ON AMENDMENT_LOG(system_code, om_version)')

        conn.commit()
        print("Database tables created successfully!")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        conn.close()

if __name__ == "__main__":
    create_database()