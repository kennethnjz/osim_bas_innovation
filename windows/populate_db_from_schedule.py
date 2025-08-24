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

        # Extract system_code from first job_id
        system_code = df['job_id'].iloc[0][:3] if not df.empty else ""

        # Clear existing data from tables we'll populate
        tables_to_clear = [
            'SRS_FUNCTION', 'RUN_SERIES', 'RUNCHART', 'SERVER_NAME',
            'SCRIPT', 'JOB', 'JOB_SRS_MAPPING', 'JOB_DEPENDENCY',
            'TIMETABLE_DAILY', 'TIMETABLE_WEEKLY', 'TIMETABLE_MONTHLY'
        ]

        for table in tables_to_clear:
            cursor.execute(f"DELETE FROM {table}")

        # Track running numbers for various tables
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
        jobs = []
        job_srs_mappings = []
        job_dependencies = []
        timetable_daily = []
        timetable_weekly = []
        timetable_monthly = []

        # Process each row in OPERATING_SCHEDULE
        for idx, row in df.iterrows():
            # Extract srs_function components
            srs_function = str(row['srs_function']).strip() if pd.notna(row['srs_function']) else ""
            srs_function_no, srs_function_title, runchart_filename = parse_srs_function(
                srs_function, srs_function_frequency_counts
            )

            # Extract frequency from job_id (4th character)
            job_id = str(row['job_id']).strip() if pd.notna(row['job_id']) else ""
            frequency = job_id[3] if len(job_id) > 3 else ""

            # Calculate series_sequence_1
            if frequency not in frequency_counts:
                frequency_counts[frequency] = 0

            series_id_raw = str(row['series_id']).strip() if pd.notna(row['series_id']) else ""

            # Generate series_id if empty
            if not series_id_raw:
                frequency_counts[frequency] += 1
                series_id = f"{frequency}{frequency_counts[frequency]:03d}"
            else:
                series_id = series_id_raw
                if series_id not in [rs['series_id'] for rs in run_series.values()]:
                    frequency_counts[frequency] += 1

            series_sequence_1 = frequency_counts[frequency]

            # Store SRS_FUNCTION
            if srs_function_no and (srs_function_no, srs_function_title) not in srs_functions:
                srs_functions[(srs_function_no, srs_function_title)] = {
                    'srs_function_no': srs_function_no,
                    'system_code': system_code,
                    'srs_function_title': srs_function_title
                }

            # Store RUN_SERIES
            if series_id not in run_series:
                run_series[series_id] = {
                    'series_id': series_id,
                    'system_code': system_code,
                    'frequency': frequency,
                    'series_sequence_1': str(series_sequence_1),
                    'series_sequence_2': "",
                    'series_title': str(row['series_title']).strip() if pd.notna(row['series_title']) else ""
                }

            # Store RUNCHART
            if runchart_filename and runchart_filename not in runcharts:
                runcharts[runchart_filename] = {
                    'runchart_id': str(runchart_counter),
                    'system_code': system_code,
                    'runchart_filename': runchart_filename
                }
                runchart_counter += 1

            # Extract server information
            server_name = str(row['server_name']).strip() if pd.notna(row['server_name']) else ""
            server_type = extract_server_type(str(row['script']) if pd.notna(row['script']) else "")

            server_key = (server_name, server_type)
            if server_name and server_key not in servers:
                servers[server_key] = {
                    'server_id': str(server_counter),
                    'server_type': server_type,
                    'server_name': server_name
                }
                server_counter += 1

            # Extract script and args
            script_text, args = parse_script_field(str(row['script']) if pd.notna(row['script']) else "")

            if script_text and script_text not in scripts:
                scripts[script_text] = {
                    'script_id': str(script_counter),
                    'script': script_text
                }
                script_counter += 1

            # Calculate job_sequence (order within series)
            jobs_in_series = [j for j in jobs if j['series_id'] == series_id]
            job_sequence = len(jobs_in_series) + 1

            # Get IDs for foreign keys
            runchart_id = runcharts.get(runchart_filename, {}).get('runchart_id', "")
            server_id = servers.get(server_key, {}).get('server_id', "")
            script_id = scripts.get(script_text, {}).get('script_id', "")

            # Store JOB
            job_record = {
                'system_code': system_code,
                'job_id': job_id,
                'series_id': series_id,
                'job_sequence': str(job_sequence),
                'runchart_id': runchart_id,
                'run_mode': str(row['run_mode']).strip() if pd.notna(row['run_mode']) else "",
                'est_run_time': str(row['est_run_time']).strip() if pd.notna(row['est_run_time']) else "",
                'est_volume': str(row['est_trx_vol']).strip() if pd.notna(row['est_trx_vol']) else "",
                'job_description': str(row['job_desc']).strip() if pd.notna(row['job_desc']) else "",
                'priority': str(row['priority_level']) if pd.notna(row['priority_level']) else "",
                'server_id': server_id,
                'script_id': script_id,
                'args': args,
                'first_run_date': str(row['start_run_date']).strip() if pd.notna(row['start_run_date']) else "",
                'suspended_date': "",
                'last_run_date': str(row['end_run_date']).strip() if pd.notna(row['end_run_date']) else "",
                'remarks': str(row['remarks']).strip() if pd.notna(row['remarks']) else ""
            }
            jobs.append(job_record)

            # Store JOB_SRS_MAPPING
            if srs_function_no:
                job_srs_mappings.append({
                    'system_code': system_code,
                    'job_id': job_id,
                    'srs_id': "",
                    'srs_function_no': srs_function_no,
                    'srs_version_number': ""
                })

            # Process JOB_DEPENDENCY
            dependent_job_id = str(row['dependent_job_id']).strip() if pd.notna(row['dependent_job_id']) else ""
            if dependent_job_id:
                parent_job_ids = [parent_id.strip() for parent_id in dependent_job_id.split(DELIMITER) if parent_id.strip()]
                for parent_job_id in parent_job_ids:
                    job_dependencies.append({
                        'job_id': job_id,
                        'job_id_parent': parent_job_id,
                        'occurrence_id': "",
                        'run_option': str(row['minutes_dependent_job_id']).strip() if pd.notna(row['minutes_dependent_job_id']) else "",
                        'run_if_scheduled': "",
                        'occurrence': ""
                    })

            # Process timetable records based on frequency
            exclude_ph = str(row['exclude_public_holidays']).strip() if pd.notna(row['exclude_public_holidays']) else ""
            run_time = str(row['start_time']).strip() if pd.notna(row['start_time']) else ""
            timetable_id = "1"  # Each job_id only has 1 run_time currently

            # Process days_of_week
            days_of_week_raw = str(row['days_of_week']).strip() if pd.notna(row['days_of_week']) else ""
            if not days_of_week_raw:
                days_of_week = "1;2;3;4;5;6;7"
            else:
                days_of_week = days_of_week_raw

            if frequency == 'D':
                timetable_daily.append({
                    'job_id': job_id,
                    'series_id': series_id,
                    'timetable_id': timetable_id,
                    'exclude_ph': exclude_ph,
                    'days_of_week': days_of_week,
                    'run_time': run_time
                })
            elif frequency == 'W':
                timetable_weekly.append({
                    'job_id': job_id,
                    'series_id': series_id,
                    'timetable_id': timetable_id,
                    'exclude_ph': exclude_ph,
                    'days_of_week': days_of_week,
                    'run_time': run_time
                })
            elif frequency == 'M':
                day_of_month = str(row['day_no']).strip() if pd.notna(row['day_no']) else ""
                month = str(row['month']).strip() if pd.notna(row['month']) else ""
                no_run = calculate_no_run_months(month)

                timetable_monthly.append({
                    'job_id': job_id,
                    'series_id': series_id,
                    'timetable_id': timetable_id,
                    'exclude_ph': exclude_ph,
                    'day_of_month': day_of_month,
                    'month': month,
                    'run_time': run_time,
                    'no_run': no_run
                })

        # Insert data into tables
        insert_srs_functions(cursor, list(srs_functions.values()))
        insert_run_series(cursor, list(run_series.values()))
        insert_runcharts(cursor, list(runcharts.values()))
        insert_servers(cursor, list(servers.values()))
        insert_scripts(cursor, list(scripts.values()))
        insert_jobs(cursor, jobs)
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

def calculate_no_run_months(month_field):
    """Calculate no_run months based on month field"""
    if not month_field:
        return ""

    all_months = set(range(1, 13))  # Months 1-12
    run_months = set()

    # Parse the month field
    month_parts = [part.strip() for part in month_field.split(DELIMITER) if part.strip()]
    for month_str in month_parts:
        try:
            month_num = int(month_str)
            if 1 <= month_num <= 12:
                run_months.add(month_num)
        except ValueError:
            continue

    # Calculate no_run months
    no_run_months = all_months - run_months

    if not no_run_months:
        return ""

    return DELIMITER.join(str(month) for month in sorted(no_run_months))

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
            INSERT INTO RUNCHART (runchart_id, system_code, runchart_filename)
            VALUES (?, ?, ?)
        """, (record['runchart_id'], record['system_code'], record['runchart_filename']))

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
                           args, first_run_date, suspended_date, last_run_date, remarks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (record['system_code'], record['job_id'], record['series_id'], record['job_sequence'],
              record['runchart_id'], record['run_mode'], record['est_run_time'], record['est_volume'],
              record['job_description'], record['priority'], record['server_id'], record['script_id'],
              record['args'], record['first_run_date'], record['suspended_date'], record['last_run_date'],
              record['remarks']))

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
        key = (record['job_id'], record['job_id_parent'], record['occurrence_id'])
        if key not in seen:
            seen.add(key)
            unique_dependencies.append(record)

    for record in unique_dependencies:
        cursor.execute("""
            INSERT INTO JOB_DEPENDENCY (job_id, job_id_parent, occurrence_id, run_option, run_if_scheduled, occurrence)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['job_id_parent'], record['occurrence_id'],
              record['run_option'], record['run_if_scheduled'], record['occurrence']))

def insert_timetable_daily(cursor, timetable_records):
    """Insert TIMETABLE_DAILY records"""
    for record in timetable_records:
        cursor.execute("""
            INSERT INTO TIMETABLE_DAILY (job_id, series_id, timetable_id, exclude_ph, days_of_week, run_time)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['series_id'], record['timetable_id'],
              record['exclude_ph'], record['days_of_week'], record['run_time']))

def insert_timetable_weekly(cursor, timetable_records):
    """Insert TIMETABLE_WEEKLY records"""
    for record in timetable_records:
        cursor.execute("""
            INSERT INTO TIMETABLE_WEEKLY (job_id, series_id, timetable_id, exclude_ph, days_of_week, run_time)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['series_id'], record['timetable_id'],
              record['exclude_ph'], record['days_of_week'], record['run_time']))

def insert_timetable_monthly(cursor, timetable_records):
    """Insert TIMETABLE_MONTHLY records"""
    for record in timetable_records:
        cursor.execute("""
            INSERT INTO TIMETABLE_MONTHLY (job_id, series_id, timetable_id, exclude_ph, day_of_month, month, run_time, no_run)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (record['job_id'], record['series_id'], record['timetable_id'],
              record['exclude_ph'], record['day_of_month'], record['month'],
              record['run_time'], record['no_run']))

if __name__ == "__main__":
    populate_data()