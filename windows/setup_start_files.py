import sys
import os
import shutil
from pathlib import Path

def setup_files():
    """Create osim folder and setup all necessary files next to executable"""

    if getattr(sys, 'frozen', False):
        # EXECUTABLE MODE: Create osim folder next to executable
        exe_dir = Path(sys.executable).parent
        osim_folder = exe_dir / "osim"
        osim_folder.mkdir(exist_ok=True)

        # Database path in osim folder
        db_path = osim_folder / "timetable.db"

        # Only copy database if it doesn't exist
        if not db_path.exists():
            try:
                # Get database from bundled resources
                bundled_db_path = os.path.join(sys._MEIPASS, "files", "timetable.db")
                shutil.copy2(bundled_db_path, db_path)
                print(f"✅ Database created at: {db_path}")
            except Exception as e:
                print(f"❌ Could not create database: {e}")
        else:
            print(f"✅ Using existing database at: {db_path}")  # Added this line for clarity

        return {
            'database_path': str(db_path),
            'osim_folder': str(osim_folder)
        }

    else:
        # DEVELOPMENT MODE: Use files/timetable.db directly
        script_dir = Path(__file__).parent
        db_path = script_dir / "files" / "timetable.db"

        # Make sure files folder exists
        files_folder = script_dir / "files"
        files_folder.mkdir(exist_ok=True)

        print(f"✅ Using development database at: {db_path}")  # Added this line for clarity

        return {
            'database_path': str(db_path),
            'osim_folder': str(files_folder)
        }

def get_database_path():
    """Get the path to the database"""
    file_paths = setup_files()
    return file_paths['database_path']

def get_osim_folder():
    """Get the path to the osim folder (or files folder in dev mode)"""
    file_paths = setup_files()
    return file_paths['osim_folder']

def get_all_file_paths():
    """Get all setup file paths"""
    return setup_files()

def is_development_mode():
    """Check if running in development mode"""
    return not getattr(sys, 'frozen', False)

def is_executable_mode():
    """Check if running as executable"""
    return getattr(sys, 'frozen', False)