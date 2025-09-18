import sqlite3
from datetime import datetime, timedelta
import setup_start_files

def exclude_public_holidays(numberOfDays=14):
    """
    Exclude public holidays from TIMETABLE_DATETIME for jobs that have exclude_ph set to true

    Parameters:
    - numberOfDays (int): Number of days from today to check for public holidays (default: 14)
    """
    conn = sqlite3.connect(setup_start_files.get_database_path()) # r'files/timetable.db'
    cursor = conn.cursor()

    try:
        # Calculate date range (today to numberOfDays later)
        today = datetime.today().date()
        future_date = today + timedelta(days=numberOfDays)
        today_str = today.strftime('%Y%m%d')
        future_str = future_date.strftime('%Y%m%d')

        print(f"Processing public holiday exclusions from {today_str} to {future_str} ({numberOfDays} days)")

        # Extract job_ids where exclude_ph is true (1)
        cursor.execute("SELECT job_id FROM JOB WHERE exclude_ph = '1'")
        exclude_ph_jobs = cursor.fetchall()

        if not exclude_ph_jobs:
            print("No jobs found with exclude_ph set to true")
            return

        exclude_ph_job_ids = [job[0] for job in exclude_ph_jobs]
        print(f"Found {len(exclude_ph_job_ids)} jobs with exclude_ph enabled: {exclude_ph_job_ids}")

        # Extract public holidays within the date range
        cursor.execute("""
            SELECT ph_date FROM PUBLIC_HOLIDAY 
            WHERE ph_date >= ? AND ph_date <= ?
        """, (today_str, future_str))

        public_holidays = cursor.fetchall()

        if not public_holidays:
            print("No public holidays found in the specified date range")
            return

        ph_dates = [ph[0] for ph in public_holidays]
        print(f"Found {len(ph_dates)} public holidays in date range: {ph_dates}")

        # First, get the records that will be deleted (for detailed logging)
        placeholders_jobs = ','.join(['?' for _ in exclude_ph_job_ids])
        placeholders_dates = ','.join(['?' for _ in ph_dates])

        select_query = f"""
            SELECT series_id, job_id, start_run_datetime, end_run_datetime, dependent_job_id
            FROM TIMETABLE_DATETIME 
            WHERE job_id IN ({placeholders_jobs})
            AND SUBSTR(start_run_datetime, 1, 8) IN ({placeholders_dates})
            ORDER BY series_id, job_id, start_run_datetime
        """

        cursor.execute(select_query, exclude_ph_job_ids + ph_dates)
        records_to_delete = cursor.fetchall()

        if not records_to_delete:
            print("No timetable records found matching the criteria for deletion")
            return

        print(f"\nFound {len(records_to_delete)} timetable records to delete:")
        print("-" * 120)
        print(f"{'Series ID':<12} {'Job ID':<15} {'Start DateTime':<15} {'End DateTime':<15} {'Dependent Job':<15} {'PH Date':<10}")
        print("-" * 120)

        # Display details of records to be deleted
        for record in records_to_delete:
            series_id, job_id, start_run_datetime, end_run_datetime, dependent_job_id = record
            ph_date = start_run_datetime[:8]  # Extract date from start_run_datetime

            # Format dependent_job_id for display
            dep_job_display = dependent_job_id if dependent_job_id else "(none)"

            print(f"{series_id:<12} {job_id:<15} {start_run_datetime:<15} {end_run_datetime:<15} {dep_job_display:<15} {ph_date:<10}")

        # Delete records from TIMETABLE_DATETIME where conditions are met
        delete_query = f"""
            DELETE FROM TIMETABLE_DATETIME 
            WHERE job_id IN ({placeholders_jobs})
            AND SUBSTR(start_run_datetime, 1, 8) IN ({placeholders_dates})
        """

        cursor.execute(delete_query, exclude_ph_job_ids + ph_dates)
        deleted_count = cursor.rowcount

        conn.commit()

        print("-" * 120)
        print(f"Successfully deleted {deleted_count} timetable records for public holiday exclusions")

        # Summary by public holiday
        if deleted_count > 0:
            print(f"\nSummary by Public Holiday:")
            print("-" * 50)
            for ph_date in ph_dates:
                matching_records = [r for r in records_to_delete if r[2][:8] == ph_date]
                if matching_records:
                    try:
                        formatted_date = datetime.strptime(ph_date, '%Y%m%d').strftime('%d %b %Y (%A)')
                        print(f"  {ph_date} ({formatted_date}): {len(matching_records)} records deleted")
                    except ValueError:
                        print(f"  {ph_date}: {len(matching_records)} records deleted")

                    # Show job IDs for this public holiday
                    job_ids_for_ph = list(set([r[1] for r in matching_records]))
                    print(f"    Jobs affected: {', '.join(sorted(job_ids_for_ph))}")

    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def get_excluded_jobs_summary():
    """
    Get a summary of jobs that have exclude_ph enabled
    """
    conn = sqlite3.connect(r'files/timetable.db')
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT job_id, job_description, series_id 
            FROM JOB 
            WHERE exclude_ph = '1'
            ORDER BY job_id
        """)

        excluded_jobs = cursor.fetchall()

        if excluded_jobs:
            print(f"\nJobs with Public Holiday Exclusion Enabled ({len(excluded_jobs)} total):")
            print("-" * 80)
            for job_id, job_desc, series_id in excluded_jobs:
                print(f"Job ID: {job_id:<15} Series: {series_id:<10} Description: {job_desc}")
        else:
            print("No jobs found with exclude_ph enabled")

    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")
    finally:
        conn.close()

def get_public_holidays_summary(numberOfDays=14):
    """
    Get a summary of upcoming public holidays

    Parameters:
    - numberOfDays (int): Number of days from today to check for public holidays (default: 14)
    """
    conn = sqlite3.connect(r'files/timetable.db')
    cursor = conn.cursor()

    try:
        # Calculate date range (today to numberOfDays later)
        today = datetime.today().date()
        future_date = today + timedelta(days=numberOfDays)
        today_str = today.strftime('%Y%m%d')
        future_str = future_date.strftime('%Y%m%d')

        cursor.execute("""
            SELECT ph_date FROM PUBLIC_HOLIDAY 
            WHERE ph_date >= ? AND ph_date <= ?
            ORDER BY ph_date
        """, (today_str, future_str))

        public_holidays = cursor.fetchall()

        if public_holidays:
            print(f"\nUpcoming Public Holidays ({today_str} to {future_str}) - {numberOfDays} days:")
            print("-" * 50)
            for ph_date_tuple in public_holidays:
                ph_date = ph_date_tuple[0]
                # Format date for display
                try:
                    formatted_date = datetime.strptime(ph_date, '%Y%m%d').strftime('%d %b %Y (%A)')
                    print(f"  {ph_date} - {formatted_date}")
                except ValueError:
                    print(f"  {ph_date}")
        else:
            print(f"No public holidays found between {today_str} and {future_str}")

    except sqlite3.Error as e:
        print(f"Database error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Default numberOfDays when run standalone
    numberOfDays = 14

    print("=" * 80)
    print("TIMETABLE PUBLIC HOLIDAY EXCLUSION")
    print("=" * 80)

    # Show summaries first
    get_excluded_jobs_summary()
    get_public_holidays_summary(numberOfDays)

    print("\n" + "=" * 80)
    print("PROCESSING EXCLUSIONS")
    print("=" * 80)

    # Process the exclusions
    exclude_public_holidays(numberOfDays)

    print("\nPublic holiday exclusion processing completed!")