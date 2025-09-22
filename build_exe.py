import subprocess
import sys
import os
import tkinter as tk
from tkinter import filedialog, messagebox

def build_executable():
    """Build executable that creates osim folder with database"""

    # Check if PyInstaller is installed
    try:
        import PyInstaller
    except ImportError:
        print("Installing PyInstaller...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    # Create a root window (hidden)
    root = tk.Tk()
    root.withdraw()

    try:
        # Ask if it's a GUI with console or GUI only app
        is_gui_with_console = messagebox.askyesno(
            "Application Type",
            "Is this a GUI with Console application?\n\nYes = with Console application\nNo = GUI only (no console window)"
        )

        # Show folder selection dialog
        messagebox.showinfo("Build Executable", "Please select where to save the executable")

        output_dir = filedialog.askdirectory(
            title="Select folder to save executable",
            initialdir=os.getcwd()
        )

        if not output_dir:
            print("❌ Build cancelled by user.")
            return

        print(f"Building executable with database...")
        print(f"Output directory: {output_dir}")

        # Build command with database file included
        build_command = [
            "pyinstaller",
            "--onefile",
            "--name", "OSIM",
            "--add-data", "files;files" if sys.platform == "win32" else "files:files",
            "--add-data", "template;template" if sys.platform == "win32" else "template:template",
            "--add-data", "windows;windows" if sys.platform == "win32" else "windows:windows",
            "--add-data", "inst;inst" if sys.platform == "win32" else "inst:inst",
            "--add-data", "full_calendar_component;full_calendar_component" if sys.platform == "win32" else "full_calendar_component:full_calendar_component",
            "--hidden-import", "tkinter.ttk",
            "--hidden-import", "dash",
            "--hidden-import", "plotly",
            "--hidden-import", "pandas",
            "--hidden-import", "sqlite3",
            "--hidden-import", "full_calendar_component",
            "--hidden-import", "db_setup",
            "--hidden-import", "db_utils",
            "--hidden-import", "timetable_generation",
            "--hidden-import", "schedule_template_validation",
            "--hidden-import", "populate_db_from_schedule",
            "--hidden-import", "public_holiday",
            "--hidden-import", "ganttchart",
            "--hidden-import", "timetable_exclude_ph",
            "--hidden-import", "dash_mantine_components",
            "--hidden-import", "dash_mantine_components._imports_",
            "--hidden-import", "dash_mantine_components.utils",
            "--distpath", output_dir,
            "main.py"
        ]

        # Add windowed option for GUI apps
        if not is_gui_with_console:
            build_command.insert(-1, "--windowed")

        # Build the executable
        result = subprocess.run(build_command, check=True)

        if result.returncode == 0:
            print(f"✅ Build successful with database included!")

            # Show success dialog
            show_result = messagebox.askyesnocancel(
                "Build Complete",
                f"Executable built successfully with database!\nLocation: {output_dir}\n\nWould you like to open the output folder?"
            )

            if show_result:  # Yes - open folder
                if sys.platform == "win32":
                    os.startfile(output_dir)
                elif sys.platform == "darwin":
                    subprocess.run(["open", output_dir])
                else:
                    subprocess.run(["xdg-open", output_dir])

    except subprocess.CalledProcessError as e:
        print(f"❌ Build failed with error code: {e.returncode}")
        messagebox.showerror("Build Failed", "Build failed. Check console for details.")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        messagebox.showerror("Error", f"An unexpected error occurred: {e}")

    finally:
        # Ensure tkinter is properly closed
        root.quit()
        root.destroy()

if __name__ == "__main__":
    build_executable()
    print("Build script completed.")
    sys.exit(0)