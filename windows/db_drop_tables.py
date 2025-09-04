import sqlite3
import os

def drop_all_tables():
    """
    Drop all tables created from db_setup.py
    This allows db_setup.py to recreate the tables with updated schemas.
    Run directly, not via main.py
    """

    # Database file path
    db_path = r'../files/timetable.db'

    # Check if database file exists
    if not os.path.exists(db_path):
        print(f"Database file {db_path} does not exist.")
        return

    # List of tables to drop
    tables_to_drop = [
        'OPERATING_SCHEDULE',
        'TIMETABLE',
        'TIMETABLE_DATETIME',
        'JOB',
        'JOB_SRS_MAPPING',
        'SRS',
        'SRS_FUNCTION',
        'RUN_SERIES',
        'RUNCHART',
        'SERVER_NAME',
        'SCRIPT',
        'JOB_DEPENDENCY',
        'NORMAL',
        'SCHEDULING_INSTRUCTION_ADD',
        'TIMETABLE_DAILY',
        'TIMETABLE_WEEKLY',
        'TIMETABLE_MONTHLY',
        'PUBLIC_HOLIDAY',
        'AMENDMENT_LOG'
    ]

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get list of existing tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = [row[0] for row in cursor.fetchall()]

        dropped_tables = []

        # Drop each table if it exists
        for table in tables_to_drop:
            if table in existing_tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                dropped_tables.append(table)

        conn.commit()

        # Simple log of dropped tables
        if dropped_tables:
            print(f"Dropped {len(dropped_tables)} tables:")
            for i in range(0, len(dropped_tables), 5):
                chunk = dropped_tables[i:i+5]
                print(', '.join(chunk))
        else:
            print("No tables found to drop.")

    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    drop_all_tables()