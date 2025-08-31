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
        # Load the file without assuming headers
        if file_type == ".xlsx":
            df = pd.read_excel(filename, header=None)
        elif file_type == ".csv":
            df = pd.read_csv(filename, encoding='cp1252', header=None)
        else:
            return False, f"Unsupported file type: {file_type}"

        # Get the first column regardless of header name
        if df.empty or df.shape[1] == 0:
            return False, "File is empty or has no columns"

        # Use the first column for ph_date values
        first_column = df.iloc[:, 0]

        # Check if first row is a header (contains "ph_date" or similar text)
        start_idx = 0
        if len(first_column) > 0:
            first_value = first_column.iloc[0]
            if is_header_row(first_value):
                start_idx = 1  # Skip the header row

        # Validate and process data
        valid_records = []
        years_to_delete = set()
        invalid_entries = []

        for idx in range(start_idx, len(first_column)):
            ph_date_val = first_column.iloc[idx]
            row_num = idx + 1  # For user-friendly row numbering

            # Skip empty values
            if is_empty_ph_date(ph_date_val):
                continue

            # Check if it's a year-only value (4 digits) for deletion
            if is_year_only(ph_date_val):
                year_str = str(int(ph_date_val))
                if is_valid_year_range(year_str):
                    years_to_delete.add(year_str)
                else:
                    invalid_entries.append(f"Row {row_num}: Invalid year '{year_str}'")
                continue

            # Validate ph_date (yyyyMMdd format and valid date)
            if not is_valid_ph_date(ph_date_val):
                # Format the display value to remove decimal point for console message
                display_ph_date = str(ph_date_val).split('.')[0] if not pd.isna(ph_date_val) else ph_date_val
                invalid_entries.append(f"Row {row_num}: Invalid ph_date '{display_ph_date}'")
                continue

            ph_date_str = format_ph_date(ph_date_val)

            # Extract year from ph_date
            year_str = ph_date_str[:4]

            # This entry is valid, add to valid records
            valid_records.append({'year': year_str, 'ph_date': ph_date_str})

        # Log invalid entries summary
        if invalid_entries:
            print("Invalid entries found:")
            for entry in invalid_entries:
                print(f"  {entry}")

        # Connect to database
        conn = sqlite3.connect(r'files/timetable.db')
        cursor = conn.cursor()

        try:
            # Get existing years in database before making changes
            cursor.execute("SELECT DISTINCT year FROM PUBLIC_HOLIDAY")
            existing_years = set(row[0] for row in cursor.fetchall())

            # Process deletions for years specified for deletion
            actually_deleted_years = []
            for year in years_to_delete:
                cursor.execute("DELETE FROM PUBLIC_HOLIDAY WHERE year = ?", (year,))
                rows_deleted = cursor.rowcount
                if rows_deleted > 0:
                    actually_deleted_years.append(year)
                    print(f"Deleted {rows_deleted} public holiday records for year {year}")
                else:
                    print(f"No records found to delete for year {year}")

            # Group valid records by year for replacement
            years_with_data = {}
            for record in valid_records:
                year = record['year']
                if year not in years_with_data:
                    years_with_data[year] = []
                years_with_data[year].append(record)

            # Separate added vs updated years
            added_years = []
            updated_years = []
            duplicate_entries = []

            # Process each year with data
            for year, records in years_with_data.items():
                # Check if this year existed before
                if year in existing_years:
                    updated_years.append(year)
                else:
                    added_years.append(year)

                # Delete existing records for this year (replacement)
                cursor.execute("DELETE FROM PUBLIC_HOLIDAY WHERE year = ?", (year,))

                # Insert new valid records
                successful_inserts = 0
                for record in records:
                    try:
                        cursor.execute("""
                            INSERT INTO PUBLIC_HOLIDAY (year, ph_date)
                            VALUES (?, ?)
                        """, (record['year'], record['ph_date']))
                        successful_inserts += 1
                    except sqlite3.IntegrityError as e:
                        if "UNIQUE constraint failed" in str(e):
                            duplicate_entries.append(f"Duplicate entry: {record['ph_date']} (year {record['year']})")
                            print(f"Duplicate entry skipped: {record['ph_date']} (year {record['year']})")
                        else:
                            # Re-raise if it's a different integrity error
                            raise e

                if successful_inserts > 0:
                    print(f"{'Updated' if year in existing_years else 'Added'} public holidays for year {year} with {successful_inserts} valid dates")

            conn.commit()

            # Check if any valid operations were performed
            total_processed = len(actually_deleted_years) + len(years_with_data)

            # If there are invalid entries and no successful operations, return error
            if total_processed == 0 and invalid_entries:
                return False, "No valid records found to process. Check console for invalid entries."

            # Special case: if we only had deletion requests but no records were actually deleted
            if total_processed == 0 and len(years_to_delete) > 0:
                return True, "No records found to delete"

            # General case: no valid operations at all
            if total_processed == 0:
                return False, "No valid records found to process"

            # Build success message with specific years
            message_parts = ["Public Holiday has been successfully imported"]

            if added_years:
                added_years_sorted = sorted(added_years)
                message_parts.append(f"Added: {', '.join(added_years_sorted)}")

            if updated_years:
                updated_years_sorted = sorted(updated_years)
                message_parts.append(f"Updated: {', '.join(updated_years_sorted)}")

            if actually_deleted_years:
                deleted_years_sorted = sorted(actually_deleted_years)
                message_parts.append(f"Deleted: {', '.join(deleted_years_sorted)}")

            if invalid_entries:
                message_parts.append(f"Found {len(invalid_entries)} invalid entries (see console log)")

            if duplicate_entries:
                message_parts.append(f"Found {len(duplicate_entries)} duplicate entries (see console log)")

            message = "\n".join(message_parts)

            return True, message

        except Exception as e:
            conn.rollback()
            return False, f"Database error: {str(e)}"
        finally:
            conn.close()

    except Exception as e:
        return False, f"File processing error: {str(e)}"

def is_header_row(value):
    """Check if the value appears to be a header (like 'ph_date')"""
    if pd.isna(value):
        return False

    # Convert to string and check if it's a text header
    str_value = str(value).strip().lower()

    # Common header patterns
    header_patterns = ['ph_date', 'date', 'holiday', 'public_holiday', 'phdate']

    return str_value in header_patterns

def is_empty_ph_date(ph_date_val):
    """Check if ph_date is empty string"""
    if pd.isna(ph_date_val):
        return True

    if isinstance(ph_date_val, str) and ph_date_val.strip() == '':
        return True

    return False

def is_year_only(ph_date_val):
    """Check if value is exactly a 4-digit year (for deletion)"""
    if pd.isna(ph_date_val):
        return False

    try:
        # Convert to string and remove any decimal points
        val_str = str(ph_date_val).split('.')[0]

        # Check if it's exactly 4 digits (not 6 or 8)
        if len(val_str) == 4 and val_str.isdigit():
            return True
    except (ValueError, TypeError):
        pass

    return False

def is_valid_year_range(year_str):
    """Check if year is in valid range"""
    try:
        year_int = int(year_str)
        return 1000 <= year_int <= 9999
    except (ValueError, TypeError):
        return False

def is_valid_ph_date(ph_date_val):
    """Check if ph_date is valid yyyyMMdd format and represents a valid date"""
    if pd.isna(ph_date_val):
        return False

    try:
        # Convert to string and remove any decimal points
        ph_date_str = str(ph_date_val).split('.')[0]

        # Check if it's exactly 8 digits (not 4 or 6)
        if len(ph_date_str) != 8 or not ph_date_str.isdigit():
            return False

        # Try to parse as date to ensure it's a valid date
        datetime.strptime(ph_date_str, "%Y%m%d")
        return True

    except (ValueError, TypeError):
        return False

def format_ph_date(ph_date_val):
    """Format ph_date to yyyyMMdd string"""
    # Convert to string and remove any decimal points
    ph_date_str = str(ph_date_val).split('.')[0]
    return ph_date_str.zfill(8)  # Ensure 8 digits with leading zeros if needed