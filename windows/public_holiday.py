import pandas as pd
import sqlite3
from datetime import datetime
from pathlib import Path

def import_public_holiday(file_type, filename):
    """
    Import public holiday data from Excel/CSV file into PUBLIC_HOLIDAY table

    Parameters:
    - file_type (str): File extension (.xlsx or .csv)
    - filename (str): Path to the file

    Returns:
    - tuple: (success: bool, message: str)
    """
    try:
        # Load the file
        if file_type == ".xlsx":
            df = pd.read_excel(filename)
        elif file_type == ".csv":
            df = pd.read_csv(filename, encoding='cp1252')
        else:
            return False, f"Unsupported file type: {file_type}"

        # Check if required columns exist
        if 'year' not in df.columns or 'ph_date' not in df.columns:
            return False, "File must contain 'year' and 'ph_date' columns"

        # Validate and process data
        valid_records = []
        years_to_delete = set()
        invalid_entries = []

        for idx, row in df.iterrows():
            year_val = row['year']
            ph_date_val = row['ph_date']

            # Validate year (yyyy format)
            if not is_valid_year(year_val):
                invalid_entries.append(f"Row {idx + 1}: Invalid year '{year_val}'")
                continue

            year_str = str(int(year_val))

            # Check if ph_date is empty (deletion case)
            if is_empty_ph_date(ph_date_val):
                years_to_delete.add(year_str)
                continue

            # Validate ph_date (yyyyMMdd format)
            if not is_valid_ph_date(ph_date_val):
                invalid_entries.append(f"Row {idx + 1}: Invalid ph_date '{ph_date_val}' for year {year_str}")
                continue

            ph_date_str = format_ph_date(ph_date_val)
            valid_records.append({'year': year_str, 'ph_date': ph_date_str})

        # Log invalid entries
        if invalid_entries:
            print("Invalid entries found:")
            for entry in invalid_entries:
                print(f"  {entry}")

        # Connect to database
        conn = sqlite3.connect(r'files/timetable.db')
        cursor = conn.cursor()

        try:
            # Process deletions for years with empty ph_date
            for year in years_to_delete:
                cursor.execute("DELETE FROM PUBLIC_HOLIDAY WHERE year = ?", (year,))
                print(f"Deleted all public holidays for year {year}")

            # Group valid records by year for replacement
            years_with_data = {}
            for record in valid_records:
                year = record['year']
                if year not in years_with_data:
                    years_with_data[year] = []
                years_with_data[year].append(record)

            # Process each year with data
            for year, records in years_with_data.items():
                # Delete existing records for this year
                cursor.execute("DELETE FROM PUBLIC_HOLIDAY WHERE year = ?", (year,))

                # Insert new records
                for record in records:
                    cursor.execute("""
                        INSERT INTO PUBLIC_HOLIDAY (year, ph_date)
                        VALUES (?, ?)
                    """, (record['year'], record['ph_date']))

                print(f"Replaced public holidays for year {year} with {len(records)} dates")

            conn.commit()

            total_processed = len(years_to_delete) + len(years_with_data)
            if total_processed == 0 and not invalid_entries:
                return False, "No valid records found to process"

            # Build success message
            message_parts = []
            if total_processed > 0:
                message_parts.append(f"Successfully processed {total_processed} year(s)")
                if years_to_delete:
                    message_parts.append(f"Deleted: {len(years_to_delete)} year(s)")
                if years_with_data:
                    message_parts.append(f"Updated: {len(years_with_data)} year(s)")

            if invalid_entries:
                message_parts.append(f"Found {len(invalid_entries)} invalid entries (see console log)")

            message = ". ".join(message_parts) + "."

            return True, message

        except Exception as e:
            conn.rollback()
            return False, f"Database error: {str(e)}"
        finally:
            conn.close()

    except Exception as e:
        return False, f"File processing error: {str(e)}"

def is_valid_year(year_val):
    """Check if year value is valid (yyyy format)"""
    if pd.isna(year_val):
        return False

    try:
        year_int = int(year_val)
        # Check if it's a reasonable year (4 digits)
        if 1000 <= year_int <= 9999:
            return True
    except (ValueError, TypeError):
        pass

    return False

def is_empty_ph_date(ph_date_val):
    """Check if ph_date is empty string"""
    if pd.isna(ph_date_val):
        return True

    if isinstance(ph_date_val, str) and ph_date_val.strip() == '':
        return True

    return False

def is_valid_ph_date(ph_date_val):
    """Check if ph_date is valid yyyyMMdd format"""
    if pd.isna(ph_date_val):
        return False

    try:
        # Convert to string and remove any decimal points
        ph_date_str = str(ph_date_val).split('.')[0]

        # Check if it's 8 digits
        if len(ph_date_str) != 8 or not ph_date_str.isdigit():
            return False

        # Try to parse as date
        datetime.strptime(ph_date_str, "%Y%m%d")
        return True

    except (ValueError, TypeError):
        return False

def format_ph_date(ph_date_val):
    """Format ph_date to yyyyMMdd string"""
    # Convert to string and remove any decimal points
    ph_date_str = str(ph_date_val).split('.')[0]
    return ph_date_str.zfill(8)  # Ensure 8 digits with leading zeros if needed