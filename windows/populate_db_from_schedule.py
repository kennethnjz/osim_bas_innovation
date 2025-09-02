import sqlite3
import pandas as pd
import re
import db_setup

# Define delimiter as a variable for flexibility
DELIMITER = ";"

def populate_data():
    """
    Populate database tables from OPERATING_SCHEDULE table
    """
    # First setup the database tables
    db_setup.create_database()

    # Connect to database
    conn = sqlite3.connect(r'files/timetable.db')
    cursor = conn.cursor()

    try:
        # Read OPERATING_SCHEDULE data
        df = pd.read_sql_query("SELECT * FROM OPERATING_SCHEDULE", conn)

        if df.empty:
            print("No data found in OPERATING_SCHEDULE table")
            return

        # Clear existing data from tables we'll populate
        tables_to_clear = [
            'SRS_FUNCTION', 'RUN_SERIES', 'RUNCHART', 'SERVER_NAME',
            'SCRIPT', 'JOB', 'JOB_SRS_MAPPING', 'JOB_DEPENDENCY',
            'TIMETABLE_DAILY', 'TIMETABLE_WEEKLY', 'TIMETABLE_MONTHLY'
        ]

        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")

        # Track running numbers for various tables (per system_code)
        frequency_counts = {}
        runchart_counter = 1
        server_counter = 1
        script_counter = 1
        srs_function_frequency_counts = {}

        # Collections to store unique records
        srs_functions = {}
        run_series = {}
        runcharts = {}
        servers = {}
        scripts = {}
        jobs = {}  # Changed to dict to store unique jobs by job_id
        job_srs_mappings = []
        job_dependencies = []
        timetable_daily = []
        timetable_weekly = []
        timetable_monthly = []

        # Group records by job_id for processing
        job_groups = df.groupby('job_id')

        # Process each job_id group
        for job_id, job_records in job_groups:
            # Extract system_code from current job_id (first 3 characters)
            system_code = job_id[:3] if len(job_id) >= 3 else job_id

            # Process the first record for JOB table (since job_id is unique in JOB table)
            first_record = job_records.iloc[0]

            # Initialize system_code-specific counters if needed
            if system_code not in srs_function_frequency_counts:
                srs_function_frequency_counts[system_code] = {}

            # Extract srs_function components
            srs_function = str(first_record['srs_function']).strip() if pd.notna(first_record['srs_function']) else ""
            srs_function_no, srs_function_title, runchart_filename = parse_srs_function(
                srs_function, srs_function_frequency_counts[system_code]
            )

            # Extract frequency from job_id (4th character)
            frequency = job_id[3] if len(job_id) > 3 else ""

            # Calculate series_sequence_1 (per system_code)
            system_frequency_key = (system_code, frequency)
            if system_frequency_key not in frequency_counts:
                frequency_counts[system_frequency_key] = 0

            series_id_raw = str(first_record['series_id']).strip() if pd.notna(first_record['series_id']) else ""

            # Generate series_id if empty
            if not series_id_raw:
                frequency_counts[system_frequency_key] += 1
                series_id = f"{frequency}{frequency_counts[system_frequency_key]:03d}"
            else:
                series_id = series_id_raw
                series_key = (system_code, series_id)
                if series_key not in run_series:
                    frequency_counts[system_frequency_key] += 1

            series_sequence_1 = frequency_counts[system_frequency_key]

            # Store SRS_FUNCTION
            if srs_function_no and (system_code, srs_function_no, srs_function_title) not in srs_functions:
                srs_functions[(system_code, srs_function_no, srs_function_title)] = {
                    'srs_function_no': srs_function_no,
                    'system_code': system_code,
                    'srs_function_title': srs_function_title
                }

            # Store RUN_SERIES
            series_key = (system_code, series_id)
            if series_key not in run_series:
                run_series[series_key] = {
                    'series_id': series_id,
                    'system_code': system_code,
                    'frequency': frequency,
                    'series_sequence_1': str(series_sequence_1),
                    'series_sequence_2': "",
                    'series_title': str(first_record['series_title']).strip() if pd.notna(first_record['series_title']) else ""
                }

            # Store RUNCHART (Updated - removed system_code)
            runchart_key = runchart_filename  # Changed to just filename
            if runchart_filename and runchart_key not in runcharts:
                runcharts[runchart_key] = {
                    'runchart_id': str(runchart_counter),
                    'runchart_filename': runchart_filename
                }
                runchart_counter += 1

            # Extract server information
            server_name = str(first_record['server_name']).strip() if pd.notna(first_record['server_name']) else ""
            server_type = extract_server_type(str(first_record['script']) if pd.notna(first_record['script']) else "")

            server_key = (server_name, server_type)
            if server_name and server_key not in servers:
                servers[server_key] = {
                    'server_id': str(server_counter),
                    'server_type': server_type,
                    'server_name': server_name
                }
                server_counter += 1

            # Extract script and args
            script_text, args = parse_script_field(str(first_record['script']) if pd.notna(first_record['script']) else "")

            if script_text and script_text not in scripts:
                scripts[script_text] = {
                    'script_id': str(script_counter),
                    'script': script_text
                }
                script_counter += 1

            # Calculate job_sequence (order within series for this system_code)
            jobs_in_series = [j for j in jobs.values() if j['series_id'] == series_id and j['system_code'] == system_code]
            job_sequence = len(jobs_in_series) + 1

            # Get IDs for foreign keys (Updated runchart_id lookup)
            runchart_id = runcharts.get(runchart_filename, {}).get('runchart_id', "")  # Changed key lookup
            server_id = servers.get(server_key, {}).get('server_id', "")
            script_id = scripts.get(script_text, {}).get('script_id', "")

            # Store JOB (only once per job_id)
            if job_id not in jobs:
                job_record = {
                    'system_code': system_code,
                    'job_id': job_id,
                    'series_id': series_id,
                    'job_sequence': str(job_sequence),
                    'runchart_id': runchart_id,
                    'run_mode': str(first_record['run_mode']).strip() if pd.notna(first_record['run_mode']) else "",
                    'est_run_time': str(first_record['est_run_time']).strip() if pd.notna(first_record['est_run_time']) else "",
                    'est_volume': str(first_record['est_trx_vol']).strip() if pd.notna(first_record['est_trx_vol']) else "",
                    'job_description': str(first_record['job_desc']).strip() if pd.notna(first_record['job_desc']) else "",
                    'priority': str(first_record['priority_level']) if pd.notna(first_record['priority_level']) else "",
                    'server_id': server_id,
                    'script_id': script_id,
                    'args': args,
                    'first_run_date': str(first_record['start_run_date']).strip() if pd.notna(first_record['start_run_date']) else "",
                    'suspended_date': "",
                    'last_run_date': str(first_record['end_run_date']).strip() if pd.notna(first_record['end_run_date']) else "",
                    'exclude_ph': str(first_record['exclude_public_holidays']).strip() if pd.notna(first_record['exclude_public_holidays']) else "",
                    'remarks': str(first_record['remarks']).strip() if pd.notna(first_record['remarks']) else ""
                }
                jobs[job_id] = job_record

                # Store JOB_SRS_MAPPING (only once per job_id)
                if srs_function_no:
                    job_srs_mappings.append({
                        'system_code': system_code,
                        'job_id': job_id,
                        'srs_id': "",
                        'srs_function_no': srs_function_no,
                        'srs_version_number': ""
                    })

            # Process timetable records and dependencies for each record of this job_id
            timetable_records = process_job_timetables(job_id, job_records, series_id)

            # Add to appropriate timetable collections
            for record in timetable_records:
                if frequency == 'D':
                    timetable_daily.append(record)
                elif frequency == 'W':
                    timetable_weekly.append(record)
                elif frequency == 'M':
                    timetable_monthly.append(record)

            # Process job dependencies
            dependencies = process_job_dependencies(job_id, job_records)
            job_dependencies.extend(dependencies)

        # Insert data into tables
        insert_srs_functions(cursor, list(srs_functions.values()))
        insert_run_series(cursor, list(run_series.values()))
        insert_runcharts(cursor, list(runcharts.values()))
        insert_servers(cursor, list(servers.values()))
        insert_scripts(cursor, list(scripts.values()))
        insert_jobs(cursor, list(jobs.values()))
        insert_job_srs_mappings(cursor, job_srs_mappings)
        insert_job_dependencies(cursor, job_dependencies)
        insert_timetable_daily(cursor, timetable_daily)
        insert_timetable_weekly(cursor, timetable_weekly)
        insert_timetable_monthly(cursor, timetable_monthly)

        conn.commit()
        print("Database populated successfully from OPERATING_SCHEDULE!")

    except Exception as e:
        print(f"Error populating database: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def process_job_timetables(job_id, job_records, series_id):
    """
    Process timetable records for a job_id, handling multiple schedules
    """
    frequency = job_id[3] if len(job_id) > 3 else ""
    timetable_records = []

    if frequency in ['D', 'W']:
        # Process daily/weekly jobs
        records_list = []

        for _, row in job_records.iterrows():
            days_of_week = str(row['days_of_week']).strip() if pd.notna(row['days_of_week']) else ""
            start_time = str(row['start_time']).strip() if pd.notna(row['start_time']) else ""
            minutes_dependent = str(row['minutes_dependent_job_id']).strip() if pd.notna(row['minutes_dependent_job_id']) else ""

            records_list.append({
                'days_of_week': days_of_week,
                'start_time': start_time,
                'minutes_dependent': minutes_dependent,
                'row_data': row
            })

        # Sort records according to requirements
        sorted_records = sort_daily_weekly_records(records_list)

        # Generate timetable_id and process records
        all_days = set(['1', '2', '3', '4', '5', '6', '7'])
        used_days = set()

        for i, record in enumerate(sorted_records):
            timetable_id = f"{frequency}{i + 1}"

            if record['days_of_week'] == "":
                # Empty days_of_week gets leftover days
                leftover_days = all_days - used_days
                days_of_week = DELIMITER.join(sorted(leftover_days))
            else:
                days_of_week = record['days_of_week']
                # Track used days
                for day in record['days_of_week'].split(DELIMITER):
                    if day.strip():
                        used_days.add(day.strip())

            timetable_record = {
                'job_id': job_id,
                'series_id': series_id,
                'timetable_id': timetable_id,
                'days_of_week': days_of_week,
                'run_time': record['start_time'],
                'run_option': record['minutes_dependent']
            }
            timetable_records.append(timetable_record)

    elif frequency == 'M':
        # Process monthly jobs
        records_list = []

        for _, row in job_records.iterrows():
            month = str(row['month']).strip() if pd.notna(row['month']) else ""
            day_of_month = str(row['day_no']).strip() if pd.notna(row['day_no']) else ""
            start_time = str(row['start_time']).strip() if pd.notna(row['start_time']) else ""
            minutes_dependent = str(row['minutes_dependent_job_id']).strip() if pd.notna(row['minutes_dependent_job_id']) else ""

            records_list.append({
                'month': month,
                'day_of_month': day_of_month,
                'start_time': start_time,
                'minutes_dependent': minutes_dependent,
                'row_data': row
            })

        # Sort records according to requirements
        sorted_records = sort_monthly_records(records_list)

        # Generate timetable_id and process records
        all_months = set(['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'])
        used_months = set()

        for i, record in enumerate(sorted_records):
            timetable_id = f"M{i + 1}"

            if record['month'] == "":
                # Empty month gets leftover months
                leftover_months = all_months - used_months
                month = DELIMITER.join(sorted(leftover_months, key=int))
            else:
                month = record['month']
                # Track used months
                for m in record['month'].split(DELIMITER):
                    if m.strip():
                        used_months.add(m.strip())

            timetable_record = {
                'job_id': job_id,
                'series_id': series_id,
                'timetable_id': timetable_id,
                'day_of_month': record['day_of_month'],
                'month': month,
                'run_time': record['start_time'],
                'run_option': record['minutes_dependent']
            }
            timetable_records.append(timetable_record)

    return timetable_records

def sort_daily_weekly_records(records_list):
    """
    Sort daily/weekly records according to requirements:
    1. Empty days_of_week first
    2. Then by descending number of days
    3. If same number of days, by earliest day
    """
    def sort_key(record):
        days_of_week = record['days_of_week']

        if days_of_week == "":
            return (0, 0)  # Empty string comes first

        days = [d.strip() for d in days_of_week.split(DELIMITER) if d.strip()]
        num_days = len(days)
        first_day = min([int(d) for d in days]) if days else 8

        return (-num_days, first_day)  # Negative for descending order

    return sorted(records_list, key=sort_key)

def sort_monthly_records(records_list):
    """
    Sort monthly records according to requirements:
    1. Empty month first
    2. Then by descending number of months
    3. If same number of months, by earliest month
    """
    def sort_key(record):
        month = record['month']

        if month == "":
            return (0, 0)  # Empty string comes first

        months = [m.strip() for m in month.split(DELIMITER) if m.strip()]
        num_months = len(months)
        first_month = min([int(m) for m in months]) if months else 13

        return (-num_months, first_month)  # Negative for descending order

    return sorted(records_list, key=sort_key)

def process_job_dependencies(job_id, job_records):
    """
    Process job dependencies for all timetable records of a job_id
    """
    frequency = job_id[3] if len(job_id) > 3 else ""
    dependencies = []

    # Get sorted records to match timetable_id generation
    if frequency in ['D', 'W']:
        records_list = []
        for _, row in job_records.iterrows():
            days_of_week = str(row['days_of_week']).strip() if pd.notna(row['days_of_week']) else ""
            records_list.append({
                'days_of_week': days_of_week,
                'row_data': row
            })
        sorted_records = sort_daily_weekly_records(records_list)

        for i, record in enumerate(sorted_records):
            timetable_id = f"{frequency}{i + 1}"
            row = record['row_data']

            dependent_job_id = str(row['dependent_job_id']).strip() if pd.notna(row['dependent_job_id']) else ""
            if dependent_job_id:
                parent_job_ids = [parent_id.strip() for parent_id in dependent_job_id.split(DELIMITER) if parent_id.strip()]
                for parent_job_id in parent_job_ids:
                    dependencies.append({
                        'job_id': job_id,
                        'job_id_parent': parent_job_id,
                        'timetable_id': timetable_id
                    })

    elif frequency == 'M':
        records_list = []
        for _, row in job_records.iterrows():
            month = str(row['month']).strip() if pd.notna(row['month']) else ""
            records_list.append({
                'month': month,
                'row_data': row
            })
        sorted_records = sort_monthly_records(records_list)

        for i, record in enumerate(sorted_records):
            timetable_id = f"M{i + 1}"
            row = record['row_data']

            dependent_job_id = str(row['dependent_job_id']).strip() if pd.notna(row['dependent_job_id']) else ""
            if dependent_job_id:
                parent_job_ids = [parent_id.strip() for parent_id in dependent_job_id.split(DELIMITER) if parent_id.strip()]
                for parent_job_id in parent_job_ids:
                    dependencies.append({
                        'job_id': job_id,
                        'job_id_parent': parent_job_id,
                        'timetable_id': timetable_id
                    })

    return dependencies

def parse_srs_function(srs_function, frequency_counts):
    """Parse srs_function field to extract components"""
    if not srs_function:
        return "", "", ""

    # Extract runchart_filename (content within parentheses)
    runchart_match = re.search(r'\(([^)]+)\)', srs_function)
    runchart_filename = runchart_match.group(1).strip() if runchart_match else ""

    # Remove parentheses content for further processing
    text_without_parens = re.sub(r'\([^)]*\)', '', srs_function).strip()

    if '-' in text_without_parens:
        # Split by dash
        parts = text_without_parens.split('-', 1)
        srs_function_no = parts[0].strip()
        srs_function_title = parts[1].strip()
    else:
        # No dash found - need to generate srs_function_no
        srs_function_title = text_without_parens.strip()

        # Extract frequency from title (first character if it matches frequency pattern)
        frequency_map = {'D': 'A', 'W': 'B', 'S': 'C', 'M': 'D', 'Q': 'E', 'A': 'F', 'R': 'G'}

        # Try to determine frequency from context or use default
        frequency_char = 'D'  # Default frequency
        for freq in frequency_map:
            if freq in srs_function_title.upper():
                frequency_char = freq
                break

        # Get next running number for this frequency
        if frequency_char not in frequency_counts:
            frequency_counts[frequency_char] = 0
        frequency_counts[frequency_char] += 1

        srs_function_no = f"{frequency_map[frequency_char]}{frequency_counts[frequency_char]:03d}"

    return srs_function_no, srs_function_title, runchart_filename

def extract_server_type(script_text):
    """Extract server type from script field"""
    script_upper = script_text.upper()
    if 'BRF' in script_upper:
        return 'BRF'
    elif 'BDP' in script_upper:
        return 'BDP'
    return ""

def parse_script_field(script_text):
    """Parse script field to extract script and args"""
    if not script_text.strip():
        return "", ""

    # Find ARGS section
    args_match = re.search(r'ARGS\s*:?\s*(.*)', script_text, re.DOTALL | re.IGNORECASE)

    if args_match:
        # Split at ARGS
        script_part = script_text[:args_match.start()].strip()
        args_part = args_match.group(1).strip()
        return script_part, args_part
    else:
        return script_text.strip(), ""

def insert_srs_functions(cursor, srs_functions):
    """Insert SRS_FUNCTION records"""
    for record in srs_functions:
        cursor.execute("""
            INSERT INTO SRS_FUNCTION (srs_function_no, system_code, srs_function_title)
            VALUES (?, ?, ?)
        """, (record['srs_function_no'], record['system_code'], record['srs_function_title']))

def insert_run_series(cursor, run_series):
    """Insert RUN_SERIES records"""
    for record in run_series:
        cursor.execute("""
            INSERT INTO RUN_SERIES (series_id, system_code, frequency, series_sequence_1, series_sequence_2, series_title)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (record['series_id'], record['system_code'], record['frequency'],
              record['series_sequence_1'], record['series_sequence_2'], record['series_title']))

def insert_runcharts(cursor, runcharts):
    """Insert RUNCHART records"""
    for record in runcharts:
        cursor.execute("""
            INSERT INTO RUNCHART (runchart_id, runchart_filename)
            VALUES (?, ?)
        """, (record['runchart_id'], record['runchart_filename']))

def insert_servers(cursor, servers):
    """Insert SERVER_NAME records"""
    for record in servers:
        cursor.execute("""
            INSERT INTO SERVER_NAME (server_id, server_type, server_name)
            VALUES (?, ?, ?)
        """, (record['server_id'], record['server_type'], record['server_name']))

def insert_scripts(cursor, scripts):
    """Insert SCRIPT records"""
    for record in scripts:
        cursor.execute("""
            INSERT INTO SCRIPT (script_id, script)
            VALUES (?, ?)
        """, (record['script_id'], record['script']))

def insert_jobs(cursor, jobs):
    """Insert JOB records"""
    for record in jobs:
        cursor.execute("""
            INSERT INTO JOB (system_code, job_id, series_id, job_sequence, runchart_id, run_mode,
                           est_run_time, est_volume, job_description, priority, server_id, script_id,
                           args, first_run_date, suspended_date, last_run_date, exclude_ph, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (record['system_code'], record['job_id'], record['series_id'], record['job_sequence'],
              record['runchart_id'], record['run_mode'], record['est_run_time'], record['est_volume'],
              record['job_description'], record['priority'], record['server_id'], record['script_id'],
              record['args'], record['first_run_date'], record['suspended_date'], record['last_run_date'],
              record['exclude_ph'], record['remarks']))

def insert_job_srs_mappings(cursor, mappings):
    """Insert JOB_SRS_MAPPING records"""
    for record in mappings:
        cursor.execute("""
            INSERT INTO JOB_SRS_MAPPING (system_code, job_id, srs_id, srs_function_no, srs_version_number)
            VALUES (?, ?, ?, ?, ?)
        """, (record['system_code'], record['job_id'], record['srs_id'],
              record['srs_function_no'], record['srs_version_number']))

def insert_job_dependencies(cursor, dependencies):
    """Insert JOB_DEPENDENCY records"""
    # Remove duplicates by converting to set of tuples and back
    unique_dependencies = []
    seen = set()
    for record in dependencies:
        key = (record['job_id'], record['job_id_parent'], record['timetable_id'])
        if key not in seen:
            seen.add(key)
            unique_dependencies.append(record)

    for record in unique_dependencies:
        cursor.execute("""
            INSERT INTO JOB_DEPENDENCY (job_id, job_id_parent, timetable_id)
            VALUES (?, ?, ?)
        """, (record['job_id'], record['job_id_parent'], record['timetable_id']))

def insert_timetable_daily(cursor, timetable_records):
    """Insert TIMETABLE_DAILY records"""
    for record in timetable_records:
        cursor.execute("""
            INSERT INTO TIMETABLE_DAILY (job_id, series_id, timetable_id, run_time, run_option, days_of_week)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['series_id'], record['timetable_id'],
              record['run_time'], record['run_option'], record['days_of_week']))

def insert_timetable_weekly(cursor, timetable_records):
    """Insert TIMETABLE_WEEKLY records"""
    for record in timetable_records:
        cursor.execute("""
            INSERT INTO TIMETABLE_WEEKLY (job_id, series_id, timetable_id, run_time, run_option, days_of_week)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['series_id'], record['timetable_id'],
              record['run_time'], record['run_option'], record['days_of_week']))

def insert_timetable_monthly(cursor, timetable_records):
    """Insert TIMETABLE_MONTHLY records"""
    for record in timetable_records:
        cursor.execute("""
            INSERT INTO TIMETABLE_MONTHLY (job_id, series_id, timetable_id, day_of_month, month, run_time, run_option)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['series_id'], record['timetable_id'],
              record['day_of_month'], record['month'],
              record['run_time'], record['run_option']))

if __name__ == "__main__":
    populate_data()