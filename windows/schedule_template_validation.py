from datetime import datetime
import pandas as pd

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
    "start_run_date": str,
    "end_run_date": str,
    "run_mode": str,
    "est_trx_vol": str,
    "est_run_time": str,
    "priority_level": str,
    "server_name": str,
    "script": str,
    "os_option": str,
    "schedule_type": str,
    "month": str,
    "week_no": str,
    "day_no": str,
    "yearly_run_date": str,
    "days_of_week": str,
    "exclude_public_holidays": str,
    "start_time": str,
    "dependent_job_id": str,
    "minutes_dependent_job_id": str
}

mandatory_fields = ["srs_function",
                    "series_id",
                    "series_title",
                    "job_id",
                    "job_desc",
                    "start_run_date",
                    "run_mode",
                    "est_run_time",
                    "priority_level",
                    "os_option",
                    "exclude_public_holidays"]


def is_empty(val):
    return pd.isna(val) or (isinstance(val, str) and val.strip() == '')


def load_template_sheet(file_type, filename):
    """
    Loads the 'Template' sheet from the given Excel file into a pandas DataFrame.

    Parameters:
    - file_path (str): Path to the Excel file.

    Returns:
    - pd.DataFrame: Data from the 'Template' sheet.
    """
    try:
        if file_type == ".xlsx":
            df = pd.read_excel(filename, sheet_name="Template", names=col_names, dtype=df_schema)
            return df
        elif file_type == ".csv":
            df = pd.read_csv(filename, names=col_names, dtype=df_schema, encoding='cp1252', skiprows=1)
            return df
    except FileNotFoundError:
        print(f"Error: File not found at '{filename}'")
    except ValueError as ve:
        print(f"Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def validate_row(row):
    errors = []
    missing_cols = []
    for col in mandatory_fields:
        val = row[col]
        if is_empty(val):
            missing_cols.append(col)

    if missing_cols:
        errors.append(f"Missing or empty: {', '.join(missing_cols)}")

    errors = validate_permissible_value(row, errors)
    errors = validate_date_fields(row, errors)
    errors = validate_time_fields(row, errors)

    if errors:
        return "; ".join(errors)
    else:
        val = row['os_option']
        if isinstance(val, str) and val.isdigit():  # convert string digits like "1" to int
            val = int(val)

        if val == 1: # by schedule
            val = row['schedule_type']
            if isinstance(val, str) and val.isdigit():
                val = int(val)

            if val == 2: # weekly schedule type
                errors = validate_weekly_data(row, errors)

        if errors:
            return "; ".join(errors)
    return ""

def validate_permissible_value(row, errors):
    # Define permissible values
    permissible_values = {
        'priority_level': [1, 2, 3, 4, 5],
        'os_option': [1, 2],
        'schedule_type': [1, 2],
        'exclude_public_holidays': [0, 1],
    }

    # Validate simple (non-list) fields
    for column, allowed in permissible_values.items():
        val = row[column]
        if not is_empty(val):
            if isinstance(val, str) and val.isdigit():
                val = int(val)
            if val not in allowed:
                errors.append(f"Invalid value '{val}' in column '{column}'")

    # Validate 'days_of_week' separately (semicolon-separated list)
    val = row.get('days_of_week')
    days_of_week_values = [1, 2, 3, 4, 5, 6, 7]
    if not is_empty(val) and isinstance(val, str):
        vals = val.split(';')
        for v in vals:
            v = v.strip()
            if v.isdigit():
                v_int = int(v)
                if v_int not in days_of_week_values:
                    errors.append(f"Invalid value '{v}' in column 'days_of_week'")
                    break
            else:
                errors.append(f"Non-numeric value '{v}' in column 'days_of_week'")
                break

    return errors


def validate_date_fields(row, errors):
    date_fields = ['start_run_date', 'end_run_date', 'yearly_run_date']

    for field in date_fields:
        val = row.get(field)
        if not is_empty(val):
            if isinstance(val, str):
                val = val.strip()
                try:
                    datetime.strptime(val, "%Y%m%d")
                except ValueError:
                    errors.append(f"Invalid date format '{val}' in column '{field}' (expected yyyyMMdd)")
            elif isinstance(val, (int, float)):
                try:
                    val_str = str(int(val))
                    datetime.strptime(val_str, "%Y%m%d")
                except ValueError:
                    errors.append(f"Invalid date format '{val}' in column '{field}' (expected yyyyMMdd)")
            else:
                errors.append(f"Unsupported type '{type(val).__name__}' in column '{field}'")

    return errors


def validate_time_fields(row, errors):
    # Validate 'Start Time' - must be HHMM between 0000 and 2359
    start_time = row.get('start_time')
    if not is_empty(start_time):
        if isinstance(start_time, str):
            start_time = start_time.strip()
            if len(start_time) != 4 or not start_time.isdigit():
                errors.append(f"Invalid format '{start_time}' in 'Start Time' (expected HHMM)")
            else:
                hh = int(start_time[:2])
                mm = int(start_time[2:])
                if not (0 <= hh <= 23) or not (0 <= mm <= 59):
                    errors.append(f"Invalid time '{start_time}' in 'Start Time' (HH must be 00-23 and MM 00-59)")
        elif isinstance(start_time, (int, float)):
            start_time_str = f"{int(start_time):04d}"
            hh = int(start_time_str[:2])
            mm = int(start_time_str[2:])
            if not (0 <= hh <= 23) or not (0 <= mm <= 59):
                errors.append(f"Invalid time '{start_time_str}' in 'Start Time' (HH must be 00-23 and MM 00-59)")
        else:
            errors.append(f"Unsupported type '{type(start_time).__name__}' in 'Start Time'")

    # Validate 'Minutes after Successful Dependent Job ID' - integer 0 to 30
    minutes = row.get('minutes_dependent_job_id')
    if not pd.isna(minutes):
        try:
            minutes_int = int(minutes)
            if not (0 <= minutes_int <= 30):
                errors.append(f"Invalid value '{minutes}' in 'minutes_dependent_job_id' (expected 0-30)")
        except (ValueError, TypeError):
            errors.append(f"Invalid type '{minutes}' in 'minutes_dependent_job_id' (expected integer)")

    return errors


def validate_weekly_data(row, errors):
    val = row.get('days_of_week')
    if is_empty(val):
        errors.append(f"Required 'days_of_week' for weekly schedule type")
    return errors

